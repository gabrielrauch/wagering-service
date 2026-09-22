//go:build multi

// Two publishers competing over one outbox, each killed in a different half of
// the window between claiming an event and recording that it was sent.
package multi

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/faults"
)

// The two publishers' names.
//
// Distinct, because the name lands in outbox.claimed_by and scopes a reschedule
// to the publisher holding the row — two publishers sharing one would put back
// each other's claims, and this scenario would be measuring that instead.
const (
	publisherOne = "publisher-one"
	publisherTwo = "publisher-two"
)

// TestTwoPublishersKilledAtEitherEndOfTheSendPublishEveryEventExactlyOnce runs
// two publishers over one outbox and kills each of them in a different place.
//
// The two deaths are the two halves of the same window and they fail in
// opposite directions:
//
//   - after_claim_before_publish leaves an event CLAIMED AND UNSENT. The risk
//     is losing it: the process that was going to send it is gone, and nothing
//     but the claim expiring by wall clock will ever give it back.
//   - after_publish_before_mark leaves an event SENT AND UNRECORDED. The risk
//     is sending it twice: the row still says unpublished, so whoever claims it
//     next sends it again — and the only thing standing between that and a
//     second event on the wire is the deduplication id being the event id.
//
// So the assertion is two-sided and neither side is optional. Every event ends
// up on the destination queue — nothing was lost — and every event is there
// exactly once by eventId — nothing was duplicated. Both counts are taken over
// the queue itself rather than over the outbox, because the outbox is the thing
// that might be wrong.
//
// That a republication genuinely happened is asserted separately, on
// `attempts`, which the claim increments. The event the killed publisher had
// already sent comes back with an attempt count above one, which is a second
// claim and therefore a second send of an event that was already on the wire.
// Without it this scenario would pass against a system that never republished
// anything, because "exactly once" is also what never sending it twice looks
// like.
func TestTwoPublishersKilledAtEitherEndOfTheSendPublishEveryEventExactlyOnce(t *testing.T) {
	t.Parallel()
	w := newWorld(t, "publishers")

	// Three wallets and two operations each, so that there is more than enough
	// work for two publishers to compete over and for each of them to reach its
	// fault point on its first turn.
	var wallets []string
	for i := range 3 {
		player := scoped(fmt.Sprintf("player-publishers-%d", i))
		wallet := openWallet(t, w.base, player, "100.00")
		wallets = append(wallets, wallet.WalletID)
		for j := range 2 {
			external := scoped(fmt.Sprintf("publishers-bet-%d-%d", i, j))
			got := operationOf(t, submit(t, w.base, providerA,
				bet(providerA, external, wallet, "5.00"), external+"-key"))
			if got.Status != processed {
				t.Fatalf("the bet %s is %s (%s), wanted %s",
					external, got.Status, got.FailureCode, processed)
			}
		}
	}
	waiting := eventIDs(t, w, "published_at IS NULL")
	if len(waiting) < 2 {
		t.Fatalf("the outbox holds %d unpublished events, and two publishers need at least "+
			"two to each claim one", len(waiting))
	}

	// One event per claim, so that each publisher reaches its fault point
	// holding exactly one event and the other has something left to claim.
	const oneAtATime = "1"
	first := w.startWorker(t, publisherOne, workerSettings{
		publishes:  true,
		publisher:  publisherOne,
		faultPoint: faults.AfterPublishBeforeMark,
		extra:      map[string]string{"PUBLISHER_BATCH": oneAtATime},
	})
	second := w.startWorker(t, publisherTwo, workerSettings{
		publishes:  true,
		publisher:  publisherTwo,
		faultPoint: faults.AfterClaimBeforePublish,
		extra:      map[string]string{"PUBLISHER_BATCH": oneAtATime},
	})
	diedAtFaultPoint(t, first, faults.AfterPublishBeforeMark, faultBudget)
	diedAtFaultPoint(t, second, faults.AfterClaimBeforePublish, faultBudget)

	// What each of them was holding when it died. The first had sent its event
	// and had not recorded it; the second had claimed its event and had sent
	// nothing.
	sentUnrecorded := heldBy(t, w, publisherOne)
	claimedUnsent := heldBy(t, w, publisherTwo)
	if len(sentUnrecorded) != 1 || len(claimedUnsent) != 1 {
		t.Fatalf("the two publishers died holding %d and %d unpublished events, wanted one "+
			"each", len(sentUnrecorded), len(claimedUnsent))
	}

	// Both come back under their own names, which is what a scheduler restarting
	// a replica does. Neither releases what it was holding on the way in — a
	// process that was killed ran no shutdown — so the claims have to expire on
	// their own, and that they do is half of what this scenario asserts.
	restartedOne := w.startWorker(t, publisherOne+"-restarted", workerSettings{
		publishes: true,
		publisher: publisherOne,
		extra:     map[string]string{"PUBLISHER_BATCH": oneAtATime},
	})
	restartedTwo := w.startWorker(t, publisherTwo+"-restarted", workerSettings{
		publishes: true,
		publisher: publisherTwo,
		extra:     map[string]string{"PUBLISHER_BATCH": oneAtATime},
	})
	eventually(t, settleBudget, "the outbox to drain", func() error {
		if left := eventIDs(t, w, "published_at IS NULL"); len(left) != 0 {
			return fmt.Errorf("%d events are still unpublished", len(left))
		}
		return nil
	})
	restartedOne.stop(t)
	restartedTwo.stop(t)

	published := eventIDs(t, w, "published_at IS NOT NULL")
	onTheWire := map[string]int{}
	for _, body := range w.drain(t, w.outboundURL) {
		var carried struct {
			EventID string `json:"eventId"`
		}
		if err := json.Unmarshal([]byte(body), &carried); err != nil {
			t.Fatalf("read an event off %s: %v\n%s", w.outbound, err, body)
		}
		if carried.EventID == "" {
			t.Fatalf("an event on %s carries no eventId:\n%s", w.outbound, body)
		}
		onTheWire[carried.EventID]++
	}

	for _, id := range published {
		switch onTheWire[id] {
		case 1:
		case 0:
			t.Errorf("event %s is marked published and is not on %s: it was lost",
				id, w.outbound)
		default:
			t.Errorf("event %s is on %s %d times, wanted once",
				id, w.outbound, onTheWire[id])
		}
		delete(onTheWire, id)
	}
	for id, count := range onTheWire {
		t.Errorf("event %s is on %s %d times and the outbox has no published row for it",
			id, w.outbound, count)
	}

	// The event the first publisher had already sent when it died was claimed
	// again and therefore sent again — and is on the queue once all the same.
	if got := attemptsOn(t, w, sentUnrecorded[0]); got < 2 {
		t.Errorf("the event %s was sent before its publisher died and reports %d claims: "+
			"it was never republished, so nothing here proved a duplicate send is "+
			"answered with the message the queue already has", sentUnrecorded[0], got)
	}
	// And the one the second had claimed and not sent was taken up by somebody
	// else once the claim expired, rather than being held for ever by a process
	// that no longer exists.
	if got := attemptsOn(t, w, claimedUnsent[0]); got < 2 {
		t.Errorf("the event %s was claimed by a publisher that died and reports %d claims: "+
			"the abandoned claim was never taken up", claimedUnsent[0], got)
	}
	reconciled(t, w.base, wallets...)
}

// eventIDs reads the outbox event identifiers matching a condition, in the
// order the rows were written.
//
// The condition is a literal in this file and never anything a caller composed
// from data, which is what makes writing it into the statement acceptable: a
// column name cannot be a placeholder.
func eventIDs(t *testing.T, w *world, condition string) []string {
	t.Helper()
	rows, err := w.owner.Query(context.Background(),
		`SELECT event_id::text FROM wagering.outbox WHERE `+condition+` ORDER BY sequence`)
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan an outbox row: %v", err)
		}
		found = append(found, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	return found
}

// heldBy is the unpublished events a publisher's claim still stands on.
func heldBy(t *testing.T, w *world, publisher string) []string {
	t.Helper()
	rows, err := w.owner.Query(context.Background(),
		`SELECT event_id::text FROM wagering.outbox `+
			`WHERE claimed_by = $1 AND published_at IS NULL ORDER BY sequence`, publisher)
	if err != nil {
		t.Fatalf("read what %s holds: %v", publisher, err)
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan an outbox row: %v", err)
		}
		found = append(found, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read what %s holds: %v", publisher, err)
	}
	return found
}

// attemptsOn is how many times an event has been claimed, which is how many
// times it has been sent: the claim increments it and every non-empty claim is
// followed by a send.
func attemptsOn(t *testing.T, w *world, event string) int {
	t.Helper()
	return countRows(t, w.owner,
		`SELECT attempts FROM wagering.outbox WHERE event_id = $1`, event)
}
