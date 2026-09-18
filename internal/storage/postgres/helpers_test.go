package postgres_test

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// The SQLSTATE codes the schema refuses things with, for the refusals that have
// no rule to name: a value that no domain accepts, or a privilege the role does
// not hold. Where a named rule did the refusing, [refusesRule] pins the rule
// instead — see the note on it for why a name is the stronger assertion there.
//
// The values come from pgerrcode rather than being written out here, so that a
// mistyped digit is a compile error instead of an assertion that quietly holds
// for any refusal at all. The short names are kept because they read better at
// the call sites than the qualified ones.
const (
	characterNotInRepertoire = pgerrcode.CharacterNotInRepertoire
	foreignKeyViolation      = pgerrcode.ForeignKeyViolation
	uniqueViolation          = pgerrcode.UniqueViolation
	checkViolation           = pgerrcode.CheckViolation
	insufficientPrivilege    = pgerrcode.InsufficientPrivilege
)

// accepts asserts that the statement is allowed.
func accepts(t *testing.T, db execer, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Errorf("statement was refused but should have been allowed: %v\n%s", err, trim(query))
	}
}

// refuses asserts that the statement is refused with the given SQLSTATE.
func refuses(t *testing.T, db execer, state, query string, args ...any) {
	t.Helper()
	_, err := db.Exec(query, args...)
	assertState(t, err, state, "statement", trim(query))
}

// refusesRule asserts that the statement is refused by the rule named.
//
// Every rule this schema enforces names itself. PostgreSQL names a CHECK, a
// unique index and a foreign key; every trigger declares its own name with
// RAISE ... USING CONSTRAINT. All of them arrive in the same field, so one
// assertion covers the lot.
//
// The name is asserted rather than the SQLSTATE because the SQLSTATE says only
// which mechanism refused — and P0001, which is every trigger, does not even say
// that much. Nine sub-cases of wallet_guard once asserted P0001 and would all
// have passed had the whole guard been replaced by a single unconditional raise.
// Naming the rule is also what lets a rule move between a trigger and a CHECK
// without the tests noticing, which is right: the rule is the contract, and how
// the schema happens to enforce it today is not.
func refusesRule(t *testing.T, db execer, rule, query string, args ...any) {
	t.Helper()
	_, err := db.Exec(query, args...)
	assertRule(t, err, rule, "statement", trim(query))
}

// assertRule asserts that err is a refusal carrying the given rule name.
func assertRule(t *testing.T, err error, want, what, detail string) {
	t.Helper()
	if detail != "" {
		detail = "\n" + detail
	}
	if err == nil {
		t.Errorf("%s was allowed but should have been refused by %s%s", what, want, detail)
		return
	}
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		t.Errorf("%s failed without a SQLSTATE, wanted %s to refuse it: %v%s", what, want, err, detail)
		return
	}
	if pgErr.ConstraintName == "" {
		t.Errorf("%s was refused with %s by no named rule, wanted %s: %v%s",
			what, pgErr.Code, want, err, detail)
		return
	}
	if pgErr.ConstraintName != want {
		t.Errorf("%s was refused by %s, wanted %s: %v%s",
			what, pgErr.ConstraintName, want, err, detail)
	}
}

// assertState asserts that err is a refusal carrying the given SQLSTATE.
//
// The code is checked rather than only the presence of an error, because work
// can fail for reasons that have nothing to do with the constraint under test —
// a typo in the SQL raises 42601 and would otherwise read as a passing test of a
// constraint that never fired. what names the thing that should have been
// refused, and detail is anything worth printing under it.
func assertState(t *testing.T, err error, state, what, detail string) {
	t.Helper()
	if detail != "" {
		detail = "\n" + detail
	}
	if err == nil {
		t.Errorf("%s was allowed but should have been refused with %s%s", what, state, detail)
		return
	}
	got, ok := sqlState(err)
	if !ok {
		t.Errorf("%s failed without a SQLSTATE, wanted %s: %v%s", what, state, err, detail)
		return
	}
	if got != state {
		t.Errorf("%s was refused with %s, wanted %s: %v%s", what, got, state, err, detail)
	}
}

// column reads one column of a result set.
//
// Every scan loop in these tests wants this and nothing more, and writing it
// out per test is how one of them came to drop its rows.Err() check — which
// turns a read that failed half way into a result that merely looks short.
func column[T any](t *testing.T, db *sql.DB, query string, args ...any) []T {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query, args...)
	if err != nil {
		t.Fatalf("read a column: %v\n%s", err, trim(query))
	}
	defer func() { _ = rows.Close() }()

	var found []T
	for rows.Next() {
		var value T
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan a value: %v\n%s", err, trim(query))
		}
		found = append(found, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read a column: %v\n%s", err, trim(query))
	}
	return found
}

// names reads a single text column, which is the common case.
func names(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	return column[string](t, db, query, args...)
}

// execer is what both a pool and a transaction offer, so an assertion can be
// made against either without the caller choosing a helper to match.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// sqlState returns the SQLSTATE PostgreSQL refused a statement with.
func sqlState(err error) (string, bool) {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code, true
	}
	return "", false
}

// trim renders a statement on one line so a failure message stays readable.
func trim(query string) string {
	return "\t" + strings.Join(strings.Fields(query), " ")
}
