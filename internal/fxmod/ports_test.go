package fxmod

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// TestTheClockIsTheResolutionStorageKeeps.
//
// timestamptz keeps microseconds. A clock that handed the application layer
// nanoseconds would stamp a row with an instant that does not compare equal to
// itself when it is read back, and the place that bites is an idempotent retry
// deciding whether it is looking at its own earlier write.
func TestTheClockIsTheResolutionStorageKeeps(t *testing.T) {
	t.Parallel()

	now := systemClock{}.Now()

	if now.Location() != time.UTC {
		t.Errorf("the clock reports %s, want UTC", now.Location())
	}
	if truncated := now.Truncate(time.Microsecond); !now.Equal(truncated) {
		t.Errorf("the clock reports %s, which carries more than microseconds", now.Format(
			time.RFC3339Nano))
	}
	if now.IsZero() {
		t.Error("the clock reports the zero time")
	}
}

// TestIdentifiersAreMintedFresh.
//
// Nothing here derives an identifier from anything a provider sent, so the one
// property worth pinning is that two calls are two identifiers and neither names
// nothing. A source that returned the zero value would be caught by a database
// constraint at the earliest, with a row half written.
func TestIdentifiersAreMintedFresh(t *testing.T) {
	t.Parallel()

	ids := mintedIDs{}

	t.Run("wallets", func(t *testing.T) {
		t.Parallel()
		first, second := ids.WalletID(), ids.WalletID()
		if first.IsZero() || second.IsZero() {
			t.Fatal("a minted wallet identifier names nothing")
		}
		if first == second {
			t.Errorf("two wallets were minted the same identifier %s", first)
		}
	})

	t.Run("transactions", func(t *testing.T) {
		t.Parallel()
		first, second := ids.TransactionID(), ids.TransactionID()
		if first.IsZero() || second.IsZero() {
			t.Fatal("a minted transaction identifier names nothing")
		}
		if first == second {
			t.Errorf("two transactions were minted the same identifier %s", first)
		}
	})

	t.Run("ledger entries", func(t *testing.T) {
		t.Parallel()
		first, second := ids.LedgerEntryID(), ids.LedgerEntryID()
		if first.IsZero() || second.IsZero() {
			t.Fatal("a minted ledger entry identifier names nothing")
		}
		if first == second {
			t.Errorf("two ledger entries were minted the same identifier %s", first)
		}
	})

	t.Run("events", func(t *testing.T) {
		t.Parallel()
		first, second := ids.EventID(), ids.EventID()
		if first == (app.EventID{}) || second == (app.EventID{}) {
			t.Fatal("a minted event identifier names nothing")
		}
		if first == second {
			t.Errorf("two events were minted the same identifier %s", first)
		}
	})
}

// recorded is a logger writing JSON into a buffer, so a test can read what was
// reported rather than that something was.
func recorded(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	var written bytes.Buffer
	return slog.New(slog.NewJSONHandler(&written, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})), &written
}

// only returns the one line the buffer holds.
func only(t *testing.T, written *bytes.Buffer) map[string]any {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(written.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("wanted one line, got %d: %q", len(lines), written.String())
	}
	var line map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &line); err != nil {
		t.Fatalf("read %q: %v", lines[0], err)
	}
	return line
}

// TestADefectIsReportedWithTheOperationAndTheCause.
//
// This hook is the only channel the condition has: the resume path returns nil
// by design because the row stays parked, so an observer that dropped either the
// identifier or the reason would leave an operator with a count of something
// going wrong and no way to find it.
func TestADefectIsReportedWithTheOperationAndTheCause(t *testing.T) {
	t.Parallel()

	logger, written := recorded(t)
	id := wagering.NewTransactionID()
	cause := errors.New("the reference view could not be rebuilt")

	loggedDefects{logger: logger}.CannotCarryForward(context.Background(), id, cause)

	line := only(t, written)
	if line["level"] != "ERROR" {
		t.Errorf("a defect was reported at %v, want ERROR", line["level"])
	}
	if line["transactionId"] != id.String() {
		t.Errorf("the report named operation %v, want %s", line["transactionId"], id)
	}
	if got, _ := line["error"].(string); !strings.Contains(got, cause.Error()) {
		t.Errorf("the report lost the reason: %q", got)
	}
}

// TestADivergenceIsReportedInMinorUnitsAndNeverAsAFloat.
//
// money.Money is int64 minor units and renders as a two-decimal string. A
// divergence reported through anything that turns it into a float — a %v on a
// float64, a JSON number — is a divergence nobody can match against the row that
// caused it, and this hook exists precisely to be matched against rows.
func TestADivergenceIsReportedInMinorUnitsAndNeverAsAFloat(t *testing.T) {
	t.Parallel()

	logger, written := recorded(t)
	wallet := wagering.NewWalletID()

	stored, err := money.Parse("10.30", "BRL")
	if err != nil {
		t.Fatalf("parse the stored balance: %v", err)
	}
	reconstructed, err := money.Parse("10.10", "BRL")
	if err != nil {
		t.Fatalf("parse the reconstructed balance: %v", err)
	}
	difference, err := money.Parse("0.20", "BRL")
	if err != nil {
		t.Fatalf("parse the difference: %v", err)
	}

	loggedDivergences{logger: logger}.WalletDiverged(context.Background(), app.Divergence{
		WalletID:      wallet,
		Stored:        stored,
		Reconstructed: reconstructed,
		Difference:    difference,
	})

	line := only(t, written)
	if line["level"] != "ERROR" {
		t.Errorf("a divergence was reported at %v, want ERROR", line["level"])
	}
	if line["walletId"] != wallet.String() {
		t.Errorf("the report named wallet %v, want %s", line["walletId"], wallet)
	}
	for _, check := range []struct {
		field string
		want  string
	}{
		{"stored", stored.String()},
		{"reconstructed", reconstructed.String()},
		{"difference", difference.String()},
	} {
		got, isString := line[check.field]
		if !isString {
			t.Errorf("the report has no %s", check.field)
			continue
		}
		if _, isNumber := got.(float64); isNumber {
			t.Errorf("%s was reported as a JSON number, which is a float: %v",
				check.field, got)
			continue
		}
		if got != check.want {
			t.Errorf("%s was reported as %v, want %q", check.field, got, check.want)
		}
	}
}
