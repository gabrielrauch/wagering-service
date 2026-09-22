package workers

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// TestAnOpeningSubmittedOverTheQueueIsRefusedAndNeverApplied puts an envelope
// whose kind is OPENING through the consumer and the REAL write path.
//
// An opening is raised by this system when a wallet is created and by nothing
// else, and the consumer's envelope deliberately does not check the kind — the
// business fields are parsed once, by the application layer, so that HTTP and
// SQS cannot disagree about them. What this proves is that the refusal still
// happens where it should: before any I/O. The transaction manager here counts
// how often it is asked for a transaction and the count stays at zero, so the
// message was refused without the wallet lock, a row or an event ever being in
// play; and the consumer, told the submission is Invalid, leaves the message
// for the redrive policy rather than deleting it or asking for it again.
//
// The control below is the same envelope with a legitimate kind. It reaches
// the transaction manager, which is what shows the zero above is the kind's
// doing and not the fixture's.
func TestAnOpeningSubmittedOverTheQueueIsRefusedAndNeverApplied(t *testing.T) {
	t.Run("an opening", func(t *testing.T) {
		tx := &countingTx{}
		queue := newFakeQueue([]sqs.Message{message("handle-1",
			validBody(t, map[string]any{"data": validData(map[string]any{"kind": "OPENING"})}), 1)})
		logs := &recorder{}

		consumerOver(t, t.Context(), ConsumerConfig{
			Queue: queue, Wagering: realWagering(t, tx), Logger: logs.logger(),
		})
		queue.handled(t)

		if got := tx.asked.Load(); got != 0 {
			t.Errorf("the write path opened %d transactions for an opening, want none: the kind "+
				"was refused after money could have moved", got)
		}
		if got := queue.deletedHandles(); len(got) != 0 {
			t.Errorf("deleted = %v, want the message left for the redrive policy", got)
		}
		if got := queue.changes(); len(got) != 0 {
			t.Errorf("visibility changes = %v, want the message left alone: refusing an "+
				"opening is not a transient failure", got)
		}
		if got := queue.releasedHandles(); len(got) != 0 {
			t.Errorf("released = %v, want nothing released while running", got)
		}

		record := logs.await(t, "the message cannot be handled and was left for the redrive policy")
		if class, _ := attr(record, "class"); class != string(app.Invalid) {
			t.Errorf("the message was refused as %q, want %s", class, app.Invalid)
		}
		if id, _ := attr(record, "messageId"); id != "message-1" {
			t.Errorf("the line names message %q, want message-1", id)
		}
		// The refusal is the domain's rule about internal kinds, and not some
		// other member of the envelope the fixture got wrong.
		if reason, _ := attr(record, "error"); !strings.Contains(reason, "OPENING") {
			t.Errorf("the line blames %q, want the refusal to be about the OPENING kind", reason)
		}
		if logs.find("the operation was applied") != nil {
			t.Error("the consumer reported an opening as applied")
		}
	})

	t.Run("the same envelope as a bet reaches the transaction manager", func(t *testing.T) {
		tx := &countingTx{}
		queue := newFakeQueue([]sqs.Message{message("handle-1", validBody(t, nil), 1)})

		consumerOver(t, t.Context(), ConsumerConfig{Queue: queue, Wagering: realWagering(t, tx)})
		queue.handled(t)

		if got := tx.asked.Load(); got != 1 {
			t.Fatalf("the write path opened %d transactions for a bet, want 1", got)
		}
		// The manager answered Retryable, so the bet is hidden for its backoff
		// rather than left: the two kinds part ways at the classification and
		// nowhere earlier.
		if got := queue.changes(); len(got) != 1 {
			t.Errorf("visibility changes = %v, want the bet hidden for its backoff", got)
		}
	})
}

// realWagering is the application's own write path, over a transaction manager
// that never opens one.
func realWagering(t *testing.T, tx app.TxManager) *app.Wagering {
	t.Helper()
	policy, err := wagering.NewReferencePolicy(3, time.Hour)
	if err != nil {
		t.Fatalf("reference policy: %v", err)
	}
	processor, err := wagering.NewProcessor(policy)
	if err != nil {
		t.Fatalf("processor: %v", err)
	}
	wagers, err := app.NewWagering(app.WageringDeps{
		Tx:        tx,
		Processor: processor,
		Clock:     fixedClock{at: testTime()},
		IDs:       mintedIDs{},
		Backoff:   app.BackoffPolicy{Initial: time.Minute, Factor: 2, Max: time.Hour},
		Defects:   discardDefects{},
	})
	if err != nil {
		t.Fatalf("wire the write path: %v", err)
	}
	return wagers
}

// countingTx is a transaction manager that counts how often it was asked for a
// transaction and never provides one.
//
// It answers Retryable, which is what a database that is not there answers,
// so that a submission which DOES reach it is visible in what the consumer
// then does with the message.
type countingTx struct{ asked atomic.Int32 }

var errNoTransaction = app.AsRetryable(errors.New("this test opens no transaction"))

func (c *countingTx) WithinMovement(context.Context, func(context.Context, *app.Repos) error) error {
	c.asked.Add(1)
	return errNoTransaction
}

func (c *countingTx) WithinSnapshot(context.Context, func(context.Context, *app.ReadRepos) error) error {
	c.asked.Add(1)
	return errNoTransaction
}

// mintedIDs mints identifiers the way the composition root does.
type mintedIDs struct{}

func (mintedIDs) WalletID() wagering.WalletID           { return wagering.NewWalletID() }
func (mintedIDs) TransactionID() wagering.TransactionID { return wagering.NewTransactionID() }
func (mintedIDs) LedgerEntryID() wagering.LedgerEntryID { return wagering.NewLedgerEntryID() }
func (mintedIDs) EventID() app.EventID                  { return app.NewEventID() }

// discardDefects is a defect observer for a test on a path that reports none.
type discardDefects struct{}

func (discardDefects) CannotCarryForward(context.Context, wagering.TransactionID, error) {}
