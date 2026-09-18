package postgres_test

import (
	"database/sql"
	"regexp"
	"slices"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The catalogue tables are the schema's copy of closed sets the domain owns.
// Each test below compares the seeded rows against the Go package that defines
// them, so adding a code or an event without a migration fails the build rather
// than surfacing as a foreign key violation in production.

// classification is how a failure code is filed, on both of the axes the domain
// splits them along.
type classification struct {
	correctable bool
	audit       bool
}

func TestFailureCodeCatalogueMatchesTheDomain(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	stored := storedFailureCodes(t, db)

	for _, code := range failure.All() {
		got, ok := stored[code]
		if !ok {
			t.Errorf("%s is declared in the domain but missing from wagering.failure_code", code)
			continue
		}
		want := classification{correctable: code.Correctable(), audit: code.Audit()}
		if got != want {
			t.Errorf("%s is stored as %+v, the domain classifies it %+v", code, got, want)
		}
		delete(stored, code)
	}
	for code := range stored {
		t.Errorf("%s is seeded in wagering.failure_code but is not a declared code", code)
	}
}

// TestSettlingFailureCodesAreTheDefinitiveNonAuditOnes pins the subset a
// rejected wager transaction may name.
//
// It is the two axes taken together, which is exactly what checkSettlingCode
// asks: a correctable code means nothing was persisted, and an audit code
// reports on stored state rather than settling an operation that was in flight.
func TestSettlingFailureCodesAreTheDefinitiveNonAuditOnes(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	stored := map[failure.Code]bool{}
	for _, code := range names(t, db, `SELECT code FROM wagering.settling_failure_code`) {
		stored[failure.Code(code)] = true
	}

	for _, code := range failure.All() {
		settles := code.Definitive() && !code.Audit()
		if stored[code] != settles {
			t.Errorf("%s is stored as settling=%t, the domain says %t", code, stored[code], settles)
		}
		delete(stored, code)
	}
	for code := range stored {
		t.Errorf("%s is seeded as settling but is not a declared code", code)
	}
}

func TestEventTypeCatalogueMatchesTheDomain(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	// The zero value of each event answers for its own name and version, which
	// the constructors fix and no caller can set. Listing the types here is the
	// only place this set is written twice, and the comparison below is what
	// keeps the two in step.
	events := []wagering.Event{
		wagering.WagerTransactionProcessed{},
		wagering.WagerTransactionRejected{},
		wagering.WalletBalanceChanged{},
		wagering.WagerTransactionPendingReference{},
	}

	rows, err := db.QueryContext(t.Context(), `SELECT event_type, event_version FROM wagering.event_type`)
	if err != nil {
		t.Fatalf("read wagering.event_type: %v", err)
	}
	defer func() { _ = rows.Close() }()

	type key struct {
		name    string
		version int
	}
	stored := map[key]bool{}
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.name, &k.version); err != nil {
			t.Fatalf("scan event type: %v", err)
		}
		stored[k] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read event types: %v", err)
	}

	for _, e := range events {
		k := key{name: e.EventType(), version: e.EventVersion()}
		if !stored[k] {
			t.Errorf("%s v%d is emitted by the domain but missing from wagering.event_type", k.name, k.version)
		}
		delete(stored, k)
	}
	for k := range stored {
		t.Errorf("%s v%d is seeded in wagering.event_type but no domain event carries it", k.name, k.version)
	}
}

// TestCatalogueDescriptionsArePresent checks the reason these are tables rather
// than CHECK constraints: an operator can ask the database what a code means.
func TestCatalogueDescriptionsArePresent(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	for _, table := range []string{"failure_code", "event_type"} {
		t.Run(table, func(t *testing.T) {
			t.Parallel()
			var blank int
			query := `SELECT count(*) FROM wagering.` + table + ` WHERE description IS NULL OR btrim(description) = ''`
			if err := db.QueryRowContext(t.Context(), query).Scan(&blank); err != nil {
				t.Fatalf("count blank descriptions in %s: %v", table, err)
			}
			if blank != 0 {
				t.Errorf("%d rows in wagering.%s carry no description", blank, table)
			}
		})
	}
}

// storedFailureCodes reads the whole catalogue.
func storedFailureCodes(t *testing.T, db *sql.DB) map[failure.Code]classification {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT code, correctable, audit FROM wagering.failure_code`)
	if err != nil {
		t.Fatalf("read wagering.failure_code: %v", err)
	}
	defer func() { _ = rows.Close() }()

	stored := map[failure.Code]classification{}
	for rows.Next() {
		var (
			code string
			c    classification
		)
		if err := rows.Scan(&code, &c.correctable, &c.audit); err != nil {
			t.Fatalf("scan failure code: %v", err)
		}
		stored[failure.Code(code)] = c
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read failure codes: %v", err)
	}
	return stored
}

// TestClosedSetChecksMatchTheDomain compares the CHECK constraints holding the
// short closed sets against the Go enumerators they copy.
//
// These three are not catalogue tables — they are small enough to state inline
// — but they are the same copy of the same thing, so the mechanism described at
// the top of this file has to reach them too. Without it, a kind added in Go
// with no migration beside it surfaces as a check violation on the first
// request that uses it, which is exactly what the catalogue tests exist to stop.
func TestClosedSetChecksMatchTheDomain(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	kinds := make([]string, 0, len(wagering.Kinds()))
	for _, kind := range wagering.Kinds() {
		kinds = append(kinds, string(kind))
	}
	statuses := make([]string, 0, len(wagering.Statuses()))
	for _, status := range wagering.Statuses() {
		statuses = append(statuses, string(status))
	}

	for _, c := range []struct {
		name       string
		table      string
		constraint string
		want       []string
	}{
		{"kinds", "wagering.wager_transaction", "wager_transaction_kind_is_known", kinds},
		{"statuses", "wagering.wager_transaction", "wager_transaction_status_is_known", statuses},
		{"directions", "wagering.wallet_ledger_entry", "wallet_ledger_entry_direction_is_known",
			[]string{string(wagering.Debit), string(wagering.Credit)}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := checkedValues(t, db, c.table, c.constraint)
			want := slices.Sorted(slices.Values(c.want))
			if !slices.Equal(got, want) {
				t.Errorf("%s admits %v, the domain declares %v", c.constraint, got, want)
			}
		})
	}
}

// checkedValues returns the literals a CHECK constraint names, sorted.
//
// The constraint is read back out of the catalogue rather than parsed from the
// migration file, so what is compared is what the database is actually
// enforcing.
var quoted = regexp.MustCompile(`'([^']*)'`)

func checkedValues(t *testing.T, db *sql.DB, table, constraint string) []string {
	t.Helper()
	var definition string
	err := db.QueryRowContext(t.Context(), `
		SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conrelid = $1::regclass AND conname = $2`,
		table, constraint).Scan(&definition)
	if err != nil {
		t.Fatalf("read %s on %s: %v", constraint, table, err)
	}
	var values []string
	for _, match := range quoted.FindAllStringSubmatch(definition, -1) {
		values = append(values, match[1])
	}
	slices.Sort(values)
	return slices.Compact(values)
}
