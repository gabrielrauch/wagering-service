package postgres_test

import (
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The schema's value domains are named after the SQL construct, not the DDD
// one: wagering.currency_code and friends are CREATE DOMAIN types reused by
// every table, so that a rule such as "a currency is three uppercase letters"
// is written once and cannot drift between the five columns that hold one.

func TestCurrencyCodeDomain(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	cases := []struct {
		name     string
		value    string
		accepted bool
		// goOnly marks a value the schema allows but the domain refuses. The
		// schema checks form alone, three uppercase letters; money.ParseCurrency
		// then requires a currency whose ISO 4217 minor unit is two digits, the
		// only ones a fixed scale of two can hold. The list lives in the domain
		// and nowhere else, so the schema cannot tell JPY from BRL and is the
		// coarser net by design (ADR-0001, amendment).
		goOnly bool
	}{
		{name: "a currency", value: "BRL", accepted: true},
		{name: "three uppercase letters naming no currency", value: "XQZ", accepted: true, goOnly: true},
		{name: "a zero-exponent currency, which scale two cannot hold", value: "JPY", accepted: true, goOnly: true},
		{name: "lowercase", value: "brl", accepted: false},
		{name: "mixed case", value: "Brl", accepted: false},
		{name: "two letters", value: "BR", accepted: false},
		{name: "four letters", value: "BRLX", accepted: false},
		{name: "digits", value: "BR1", accepted: false},
		{name: "empty", value: "", accepted: false},
		{name: "trailing space", value: "BRL ", accepted: false},
		{name: "leading space", value: " BRL", accepted: false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			const cast = `SELECT $1::text::wagering.currency_code`
			if c.accepted {
				accepts(t, db, cast, c.value)
			} else {
				refuses(t, db, checkViolation, cast, c.value)
			}

			// The same string put to the domain's own parser. The schema's rule
			// is the domain's form check, so the two may differ in one direction
			// only: a value the domain accepts and the schema refuses would make
			// storage a way around construction, and is a defect. A value the
			// schema accepts and the domain refuses is a documented goOnly case.
			_, err := money.ParseCurrency(c.value)
			wantGo := c.accepted && !c.goOnly
			if accepted := err == nil; accepted != wantGo {
				t.Errorf("money.ParseCurrency accepted=%t, wanted %t (schema accepted=%t)", accepted, wantGo, c.accepted)
			}
		})
	}
}

func TestOpaqueIDDomain(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	cases := []struct {
		name     string
		value    string
		accepted bool
		// goOnly marks a value the schema allows but the domain refuses. The
		// schema's whitespace class is ASCII; Go's strings.TrimSpace covers the
		// whole Unicode White_Space property. The database is the coarser net,
		// and that difference is documented rather than discovered.
		goOnly bool
		// state overrides the SQLSTATE a refusal is expected to carry, for the
		// one value the text type rejects before the domain's CHECK is reached.
		state string
	}{
		{name: "a provider name", value: "acme-gaming", accepted: true},
		{name: "an identifier with an internal space", value: "acme gaming", accepted: true},
		{name: "a single character", value: "a", accepted: true},
		{name: "exactly the maximum in octets", value: strings.Repeat("a", 128), accepted: true},
		{name: "empty", value: "", accepted: false},
		{name: "one octet over the maximum", value: strings.Repeat("a", 129), accepted: false},
		// 65 two-octet runes is 65 characters and 130 octets. The limit is
		// counted in octets, matching Go's len, so this must be refused — a
		// schema measuring characters would let it through.
		{name: "under the maximum in characters but over it in octets", value: strings.Repeat("é", 65), accepted: false},
		{name: "leading space", value: " acme", accepted: false},
		{name: "trailing space", value: "acme ", accepted: false},
		{name: "leading newline", value: "\nacme", accepted: false},
		{name: "an embedded tab", value: "ac\tme", accepted: false},
		// PostgreSQL text cannot hold a NUL at all, so this is refused by the
		// type before the domain's CHECK is ever reached — a stronger refusal
		// than the one designed for it, under a different SQLSTATE.
		{name: "an embedded NUL", value: "ac\x00me", accepted: false, state: characterNotInRepertoire},
		{name: "a leading non-breaking space", value: "\u00a0acme", accepted: true, goOnly: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			const cast = `SELECT $1::text::wagering.opaque_id`
			switch {
			case c.accepted:
				accepts(t, db, cast, c.value)
			case c.state != "":
				refuses(t, db, c.state, cast, c.value)
			default:
				refuses(t, db, checkViolation, cast, c.value)
			}

			_, err := wagering.NewProvider(c.value)
			wantGo := c.accepted && !c.goOnly
			if accepted := err == nil; accepted != wantGo {
				t.Errorf("wagering.NewProvider accepted=%t, wanted %t (schema accepted=%t)", accepted, wantGo, c.accepted)
			}
		})
	}
}

func TestSHA256HexDomain(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	const hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

	cases := []struct {
		name     string
		value    string
		accepted bool
	}{
		{"a hash", hash, true},
		{"all zeroes", strings.Repeat("0", 64), true},
		{"uppercase hex", strings.ToUpper(hash), false},
		{"one character short", hash[:63], false},
		{"one character long", hash + "0", false},
		{"not hex", strings.Repeat("g", 64), false},
		{"empty", "", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			const cast = `SELECT $1::text::wagering.sha256_hex`
			if c.accepted {
				accepts(t, db, cast, c.value)
			} else {
				refuses(t, db, checkViolation, cast, c.value)
			}
		})
	}
}

// TestRolesAreGroupRolesOwningTheSchema checks the two roles the brief names.
//
// Both are NOLOGIN on purpose: they are groups a deployment grants to a real
// login user, so that the privileges live in version control and the
// credentials do not.
func TestRolesAreGroupRolesOwningTheSchema(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	for _, role := range []string{"wagering_migrator", "wagering_app"} {
		t.Run(role, func(t *testing.T) {
			t.Parallel()
			var canLogin bool
			err := db.QueryRowContext(t.Context(), `SELECT rolcanlogin FROM pg_roles WHERE rolname = $1`, role).Scan(&canLogin)
			if err != nil {
				t.Fatalf("look up role %s: %v", role, err)
			}
			if canLogin {
				t.Errorf("role %s can log in, but it is meant to be a group role", role)
			}
		})
	}

	t.Run("the migration role owns the schema", func(t *testing.T) {
		t.Parallel()
		var owner string
		err := db.QueryRowContext(t.Context(), `
			SELECT pg_catalog.pg_get_userbyid(nspowner)
			FROM pg_catalog.pg_namespace
			WHERE nspname = 'wagering'`).Scan(&owner)
		if err != nil {
			t.Fatalf("look up the schema owner: %v", err)
		}
		if owner != "wagering_migrator" {
			t.Errorf("the wagering schema is owned by %q, wanted wagering_migrator", owner)
		}
	})
}
