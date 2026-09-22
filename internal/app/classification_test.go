package app_test

import (
	"errors"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

func TestClassification(t *testing.T) {
	cases := map[string]struct {
		err  error
		want app.Class
	}{
		"nothing went wrong": {err: nil, want: ""},
		"a malformed submission": {
			err:  failure.New(failure.MissingRequiredField, "absent"),
			want: app.Invalid,
		},
		"an amount with no scale": {
			err:  failure.New(failure.InvalidAmountScale, "25.0"),
			want: app.Invalid,
		},
		"a wallet already open": {
			err:  failure.New(failure.WalletAlreadyExists, "taken"),
			want: app.Conflict,
		},
		"a key bound elsewhere": {
			err:  failure.New(failure.IdempotencyPayloadConflict, "bound"),
			want: app.Conflict,
		},
		"books that do not balance": {
			err:  failure.New(failure.LedgerBalanceMismatch, "out by 25"),
			want: app.Audit,
		},
		// Definitive, but on an ERROR path: the operation never became anything,
		// so it is not a rejection. A rejection is an outcome.
		"a settled code on an error path": {
			err:  failure.New(failure.InsufficientFunds, "no"),
			want: app.Unretryable,
		},
		"an error nobody classified": {
			err:  errors.New("connection reset by peer"),
			want: app.Unretryable,
		},
		"an error an adapter called transient": {
			err:  app.AsRetryable(errors.New("deadlock detected")),
			want: app.Retryable,
		},
		"an error an adapter called permanent": {
			err:  app.AsUnretryable(errors.New("permission denied")),
			want: app.Unretryable,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got := app.ClassOf(c.err); got != c.want {
				t.Errorf("classified %s, want %s", got, c.want)
			}
		})
	}
}

// A classified error still answers to the domain's own predicates, so a caller
// that wants the reason rather than the class does not have to unwrap by hand.
func TestAClassifiedErrorKeepsItsDomainCodeReachable(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	_, err := f.trySubmit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "99.00"}))

	if !failure.Is(err, failure.IdempotencyPayloadConflict) {
		t.Errorf("failure.Is cannot see through the classification: %v", err)
	}
	if code, ok := failure.CodeOf(err); !ok || code != failure.IdempotencyPayloadConflict {
		t.Errorf("failure.CodeOf got %s/%t, want IDEMPOTENCY_PAYLOAD_CONFLICT", code, ok)
	}
}

// REJECTED is never inferred from an error. Whatever a code means, an error
// carrying it says the operation never became anything — and a rejection is an
// operation that became something, persisted, with an event.
func TestNoErrorEverClassifiesAsRejected(t *testing.T) {
	for _, code := range failure.All() {
		if got := app.ClassOf(failure.New(code, "whatever")); got == app.Rejected {
			t.Errorf("%s on an error path classified as %s", code, app.Rejected)
		}
	}
}

// The only way a rejection is reported: a nil error, and a status that says so.
func TestARejectionArrivesAsAnOutcomeAndNotAsAnError(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "10.00", "BRL")

	result, err := f.trySubmit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	if err != nil {
		t.Fatalf("a business rejection is not an error: %v", err)
	}
	assertStatus(t, result, wagering.Rejected, failure.InsufficientFunds)
}

// A repository failure is classified by what the adapter said about it, never
// by what this layer can guess: an error nobody recognised is not one anybody
// established is safe to repeat, and one an adapter marked transient keeps the
// mark it was given.
func TestARepositoryFailureIsClassifiedByWhatTheAdapterSaid(t *testing.T) {
	for _, tc := range []struct {
		name     string
		injected error
		want     app.Class
	}{
		{"unclassified", errors.New("connection reset by peer"), app.Unretryable},
		{"marked transient by the adapter", app.AsRetryable(errors.New("deadlock detected")), app.Retryable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.db.seedWallet(t, "player-1", "100.00", "BRL")
			f.db.failNext("wallet.LockByID", tc.injected)

			_, err := f.trySubmit(t, fields(acme, submission{
				Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00",
			}))

			assertClass(t, err, tc.want)
		})
	}
}

// A command is one transaction, so a failure anywhere in it leaves nothing
// behind — including a failure at the very last statement, by which point the
// transaction, the wallet, the ledger entry and the inbox row have all been
// written.
func TestAFailureMidCommandLeavesNoPartialWrites(t *testing.T) {
	failures := []string{"outbox.Append", "repos.Settle", "txn.Record", "inbox.Record"}

	for _, at := range failures {
		t.Run("failing at "+at, func(t *testing.T) {
			f := newFixture(t)
			wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
			transactionsBefore := f.db.transactionCount()
			entriesBefore := len(f.db.ledgerEntries())

			f.db.failNext(at, errors.New("the database went away"))
			_, err := f.fromQueue(t, fields(acme, submission{
				Kind:     "BET",
				External: "ext-1",
				Key:      "key-1",
				Amount:   "25.00",
			}), "msg-1", "hash-1")

			if err == nil {
				t.Fatal("want a failure")
			}
			if got := f.db.balanceOf(t, wallet).Amount(); got != "100.00" {
				t.Errorf("wallet holds %s, want the 100.00 it started with", got)
			}
			if got := f.db.transactionCount(); got != transactionsBefore {
				t.Errorf("%d transactions, want the %d already there", got, transactionsBefore)
			}
			if got := len(f.db.ledgerEntries()); got != entriesBefore {
				t.Errorf("%d ledger entries, want the %d already there", got, entriesBefore)
			}
			if got := len(f.db.inboxRecords()); got != 0 {
				t.Errorf("%d inbox records, want none", got)
			}
			if got := len(f.db.envelopes()); got != 0 {
				t.Errorf("%d events, want none", got)
			}
			if f.db.Commits() != 0 {
				t.Errorf("%d commits, want none", f.db.Commits())
			}

			// And the idempotency key is still free, because nothing bound it.
			f.db.failAtNothing()
			result := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))
			if result.Status != wagering.Processed {
				t.Errorf("resubmission is %s, want %s", result.Status, wagering.Processed)
			}
		})
	}
}
