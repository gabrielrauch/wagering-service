package postgres_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

const insertInboxMessage = `
	INSERT INTO wagering.inbox (consumer_name, message_id, payload_hash, received_at, completed_at)
	VALUES ($1, $2, $3, $4, $5)`

func TestInboxRecordsAMessageOncePerConsumer(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	const consumer = "wagering-worker"
	message := "sqs-" + wagering.NewTransactionID().String()

	accepts(t, db, insertInboxMessage, consumer, message, hashOf(message), base, nil)

	t.Run("the same message again", func(t *testing.T) {
		refuses(t, db, uniqueViolation, insertInboxMessage,
			consumer, message, hashOf(message), base, nil)
	})

	t.Run("the same message for a different consumer", func(t *testing.T) {
		accepts(t, db, insertInboxMessage,
			"reporting-worker", message, hashOf(message), base, nil)
	})
}

func TestInboxCompletionFollowsReceipt(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	message := "sqs-" + wagering.NewTransactionID().String()

	t.Run("completed after it arrived", func(t *testing.T) {
		accepts(t, db, insertInboxMessage,
			"worker", message, hashOf(message), base, base.Add(time.Second))
	})

	t.Run("completed before it arrived", func(t *testing.T) {
		other := "sqs-" + wagering.NewTransactionID().String()
		refuses(t, db, checkViolation, insertInboxMessage,
			"worker", other, hashOf(other), base, base.Add(-time.Second))
	})
}

// TestInboxSharesTheTransactionOfTheWorkItDescribes is the point of having an
// inbox at all: the record that a message was handled and the domain changes it
// caused commit together, so there is no window in which one exists without the
// other.
func TestInboxSharesTheTransactionOfTheWorkItDescribes(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)
	bet := processedBet(t, db, w)

	message := "sqs-" + wagering.NewTransactionID().String()
	later := base.Add(time.Second)

	err := inTx(t, db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), insertInboxMessage, "worker", message, hashOf(message), base, nil); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), moveWallet, w.id, int64(7500), int64(2), later); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), insertLedgerEntry, debit(w, bet, 2500, 10000, 2, later).args()...); err != nil {
			return err
		}
		_, err := tx.ExecContext(t.Context(), `
			UPDATE wagering.inbox SET completed_at = $3
			WHERE consumer_name = $1 AND message_id = $2`, "worker", message, later)
		return err
	})
	if err != nil {
		t.Fatalf("handling a message alongside the work it caused was refused: %v", err)
	}

	var completed sql.NullTime
	if err := db.QueryRowContext(t.Context(), `
		SELECT completed_at FROM wagering.inbox
		WHERE consumer_name = $1 AND message_id = $2`, "worker", message).Scan(&completed); err != nil {
		t.Fatalf("read the inbox row: %v", err)
	}
	if !completed.Valid {
		t.Error("the message committed without being marked handled")
	}
}

// TestInboxIdentifiersUseTheValueDomain keeps the message identity held to the
// same shape rule as every other provider-supplied string.
func TestInboxIdentifiersUseTheValueDomain(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	refuses(t, db, checkViolation, insertInboxMessage,
		"worker", " leading-space", hashOf("x"), base, nil)
}
