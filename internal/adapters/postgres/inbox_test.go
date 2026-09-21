//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// TestTheInboxAnswersWhatItHasHandled pins the read a redelivery turns on.
//
// The three answers below are three different decisions the use case makes from
// this one call: nothing found means handle the message, the same body means
// replay whatever it settled to, and a different body under one message id
// means nothing can decide which is real. Getting the first two confused is how
// a message is applied twice.
func TestTheInboxAnswersWhatItHasHandled(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	message := app.InboxMessage{Consumer: "wagering", MessageID: "m-find", BodyHash: bodyHash}

	t.Run("a message nobody has handled", func(t *testing.T) {
		// (nil, nil), never an error. Absence is an answer the caller has a
		// branch for, and putting it in the error channel would make it
		// something the caller had to tell apart from a database that is down.
		if got := w.find(t, message.Key()); got != nil {
			t.Fatalf("an unhandled message came back as %v, wanted nothing", got)
		}
	})

	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		return r.Inbox.Record(ctx, message, at(1))
	})
	if err != nil {
		t.Fatalf("record a message: %v", err)
	}

	t.Run("a message this consumer has handled", func(t *testing.T) {
		got := w.find(t, message.Key())
		if got == nil {
			t.Fatal("a recorded message cannot be found")
		}
		if got.Consumer != message.Consumer || got.MessageID != message.MessageID {
			t.Fatalf("found %s/%s, wanted %s/%s",
				got.Consumer, got.MessageID, message.Consumer, message.MessageID)
		}
		if got.BodyHash != message.BodyHash {
			t.Fatalf("the stored fingerprint is %q, wanted %q", got.BodyHash, message.BodyHash)
		}
		// One write, not two: there is no second statement marking the message
		// complete, because the row and the domain changes it describes commit
		// together. A row that exists is a message that was handled.
		if !got.ReceivedAt.Equal(at(1)) {
			t.Fatalf("received at %s, wanted %s", got.ReceivedAt, at(1))
		}
		if !got.CompletedAt.Equal(got.ReceivedAt) {
			t.Fatalf("completed at %s and received at %s, wanted one instant",
				got.CompletedAt, got.ReceivedAt)
		}
	})

	t.Run("the same message seen by another consumer", func(t *testing.T) {
		// The consumer is half of the identity, because two consumers may
		// legitimately see one message. A lookup that ignored it would report
		// somebody else's work as this consumer's and drop the message.
		other := app.InboxKey{Consumer: "reporting", MessageID: message.MessageID}
		if got := w.find(t, other); got != nil {
			t.Fatalf("another consumer's lookup found %v, wanted nothing", got)
		}
	})
}

// find reads an inbox record through the adapter.
func (w *world) find(t *testing.T, key app.InboxKey) *app.InboxRecord {
	t.Helper()
	var record *app.InboxRecord
	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		var err error
		record, err = r.Inbox.Find(ctx, key)
		return err
	})
	if err != nil {
		t.Fatalf("read the inbox for %s/%s: %v", key.Consumer, key.MessageID, err)
	}
	return record
}
