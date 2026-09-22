//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgerrcode"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// TestTheLedgerPagesStablyAcrossAWrite is what the wallet version buys.
//
// The page boundary is drawn on (walletId, walletVersion), which is unique and
// ordered, so a movement landing between two pages appends after the reader
// rather than shifting it. An offset would have moved every later entry down by
// one — the reader would see one entry twice — and a creation instant ties,
// because two entries can share one.
func TestTheLedgerPagesStablyAcrossAWrite(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-page", "100.00", "BRL")
	for i := 1; i <= 4; i++ {
		w.apply(t, command(t, wagering.Bet, "player-page",
			"ext-page-"+string(rune('a'+i)), "1.00", "BRL"), at(i))
	}

	// The opening's own entry is version 1, so the wallet now stands at five.
	first := w.page(t, wallet.ID(), 0, 2)
	assertVersions(t, "the first page", first, 1, 2)

	// A movement lands between the pages.
	w.apply(t, command(t, wagering.Bet, "player-page", "ext-page-late", "1.00", "BRL"), at(20))

	second := w.page(t, wallet.ID(), last(first), 2)
	assertVersions(t, "the second page", second, 3, 4)
	third := w.page(t, wallet.ID(), last(second), 2)
	assertVersions(t, "the third page", third, 5, 6)
	if got := w.page(t, wallet.ID(), last(third), 2); len(got) != 0 {
		t.Fatalf("a fourth page returned %d entries, wanted none", len(got))
	}

	// And the entry written mid-read is the one at the end, rather than one the
	// reader skipped past.
	if got := third[1].Amount().Amount(); got != "1.00" {
		t.Fatalf("the last entry moves %s, wanted 1.00", got)
	}
}

// TestTheApplicationCannotRewriteTheLedger is the privilege half of
// append-only.
//
// The trigger says nobody may rewrite the ledger, including the role that owns
// the schema. This says the application may not even attempt it, so the refusal
// arrives before any row is examined — and TRUNCATE is in the list because it is
// the hole a DELETE-only revoke leaves open.
func TestTheApplicationCannotRewriteTheLedger(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-append-only", "100.00", "BRL")
	w.apply(t, command(t, wagering.Bet, "player-append-only", "ext-ao", "10.00", "BRL"), at(1))

	refused := []struct {
		name  string
		query string
	}{
		{name: "UPDATE", query: `UPDATE wagering.wallet_ledger_entry SET amount_minor = 1`},
		{name: "DELETE", query: `DELETE FROM wagering.wallet_ledger_entry`},
		{name: "TRUNCATE", query: `TRUNCATE wagering.wallet_ledger_entry`},
	}
	for _, c := range refused {
		t.Run(c.name, func(t *testing.T) {
			_, err := w.app.Exec(t.Context(), c.query)
			refusedWith(t, err, pgerrcode.InsufficientPrivilege)
		})
	}

	t.Run("reading is allowed", func(t *testing.T) {
		if got := len(w.entries(t, wallet.ID())); got != 2 {
			t.Fatalf("read %d entries, wanted 2", got)
		}
	})

	// Nothing above got as far as changing anything, which is the point of
	// checking a privilege before a trigger.
	if got := w.count(t, `SELECT count(*) FROM wagering.wallet_ledger_entry`); got != 2 {
		t.Fatalf("%d entries remain, wanted 2", got)
	}
}

// page reads one page of a wallet's ledger through the adapter.
func (w *world) page(
	t *testing.T,
	id wagering.WalletID,
	after uint64,
	limit int,
) []wagering.WalletLedgerEntry {
	t.Helper()
	var entries []wagering.WalletLedgerEntry
	err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
		var err error
		entries, err = r.Ledger.Page(ctx, id, after, limit)
		return err
	})
	if err != nil {
		t.Fatalf("page the ledger: %v", err)
	}
	return entries
}

// entries reads a wallet's whole ledger through the adapter.
func (w *world) entries(t *testing.T, id wagering.WalletID) []wagering.WalletLedgerEntry {
	t.Helper()
	var entries []wagering.WalletLedgerEntry
	err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
		var err error
		entries, err = r.Ledger.All(ctx, id)
		return err
	})
	if err != nil {
		t.Fatalf("read the ledger: %v", err)
	}
	return entries
}

// last is the cursor position a page ends at, which is the next page's start.
func last(page []wagering.WalletLedgerEntry) uint64 {
	if len(page) == 0 {
		return 0
	}
	return page[len(page)-1].WalletVersion()
}

func assertVersions(t *testing.T, what string, page []wagering.WalletLedgerEntry, want ...uint64) {
	t.Helper()
	if len(page) != len(want) {
		t.Fatalf("%s holds %d entries, wanted %d", what, len(page), len(want))
	}
	for i, entry := range page {
		if entry.WalletVersion() != want[i] {
			t.Fatalf("%s holds version %d at %d, wanted %d",
				what, entry.WalletVersion(), i, want[i])
		}
	}
}

// TestMoneyCrossesAsMinorUnits pins the one property this adapter must never
// lose: an amount survives storage exactly, at the edge of what can be
// represented.
//
// The value below is the largest money.Money there is — 9,223,372,036,854,775,807
// minor units — and it comes back byte for byte because nothing on this path
// does arithmetic on it, renders it as a decimal or lets it near a float. A
// NUMERIC column, a decimal string round-trip or a float64 anywhere between
// here and the disk would each lose it in a different digit.
func TestMoneyCrossesAsMinorUnits(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	const largest = "92233720368547758.07"
	wallet := w.openWallet(t, "player-largest", largest, "BRL")

	stored, entries := w.snapshotOfWallet(t, wallet.ID())
	if got := stored.Balance().Amount(); got != largest {
		t.Fatalf("the wallet holds %s, wanted %s", got, largest)
	}
	if got := stored.Balance().MinorUnits(); got != 9223372036854775807 {
		t.Fatalf("the wallet holds %d minor units, wanted 9223372036854775807", got)
	}
	if len(entries) != 1 {
		t.Fatalf("the opening wrote %d entries, wanted 1", len(entries))
	}
	if got := entries[0].Amount().Amount(); got != largest {
		t.Fatalf("the opening entry records %s, wanted %s", got, largest)
	}
	if got := entries[0].Amount().Currency().String(); got != "BRL" {
		t.Fatalf("the opening entry is in %s, wanted BRL", got)
	}
}
