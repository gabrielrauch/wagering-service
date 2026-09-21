//go:build integration

// The fixtures: what a test sends, what it reads back, and how it audits the
// tables underneath.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// currency is the one currency this suite deals in. Nothing here is testing
// what happens across two of them; the domain's own suite is.
const currency = "BRL"

// submission is a body a provider posts to /wagering/transactions.
//
// It is this suite's own type rather than the handler's, because the whole
// point of standing here is to be a caller: a test that built its request out
// of the struct the handler decodes into could not notice a member being
// renamed on one side only.
type submission struct {
	Provider                       string `json:"provider"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Money                          amount `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

// amount is money on the wire: a two-decimal string and a currency, never a
// number.
type amount struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// operation is the view a submission and a read both answer with.
type operation struct {
	TransactionID         string  `json:"transactionId"`
	ExternalTransactionID string  `json:"externalTransactionId"`
	Kind                  string  `json:"kind"`
	Status                string  `json:"status"`
	Money                 amount  `json:"money"`
	Balance               *amount `json:"balance"`
	FailureCode           string  `json:"failureCode"`
	IdempotentReplay      bool    `json:"idempotentReplay"`
}

// walletView is the view /wallets answers with.
type walletView struct {
	WalletID string     `json:"walletId"`
	PlayerID string     `json:"playerId"`
	Balance  amount     `json:"balance"`
	Version  uint64     `json:"version"`
	Opening  *operation `json:"opening"`
}

// refusalBody is the one shape every refusal takes: three members, always all
// three.
type refusalBody struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlationId"`
}

// bet builds the submission a provider makes for a wager.
func bet(provider, external, player, value string) submission {
	return submission{
		Provider:              provider,
		ExternalTransactionID: external,
		PlayerID:              player,
		RoundID:               "round-" + external,
		GameID:                "game-1",
		Kind:                  "BET",
		Money:                 amount{Amount: value, Currency: currency},
	}
}

// encode renders a body the way a provider would send it.
func encode(t *testing.T, body any) string {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode a request body: %v", err)
	}
	return string(raw)
}

// submit posts one operation as the client named.
func (s *stack) submit(t *testing.T, client string, body submission, key string) answer {
	t.Helper()
	return s.do(t, call{
		method:         http.MethodPost,
		path:           "/wagering/transactions",
		body:           encode(t, body),
		token:          tokenFor(t, client),
		idempotencyKey: key,
	})
}

// openWallet opens a wallet through the API, as the only identity that may.
func (s *stack) openWallet(t *testing.T, player, initial string) walletView {
	t.Helper()
	got := s.do(t, call{
		method: http.MethodPost,
		path:   "/wallets",
		body: encode(t, map[string]any{
			"playerId":       player,
			"initialBalance": amount{Amount: initial, Currency: currency},
		}),
		token: tokenFor(t, walletService),
	})
	if got.status != http.StatusCreated {
		t.Fatalf("opening a wallet for %s answered %s, wanted 201", player, got)
	}
	var view walletView
	decode(t, got, &view)
	return view
}

// decode reads an answer's body into v, failing the test when it will not.
func decode(t *testing.T, got answer, v any) {
	t.Helper()
	if err := json.Unmarshal(got.body, v); err != nil {
		t.Fatalf("decode %s: %v", got, err)
	}
}

// operationOf reads an answer as the operation view, insisting on the status
// first so that a refusal is reported as itself rather than as a decode error.
func operationOf(t *testing.T, got answer) operation {
	t.Helper()
	if got.status != http.StatusOK {
		t.Fatalf("wanted 200 and an operation, got %s", got)
	}
	var op operation
	decode(t, got, &op)
	return op
}

// refused asserts a refusal: the status, the code, and the three members every
// one of them carries.
func refused(t *testing.T, got answer, status int, code string) refusalBody {
	t.Helper()
	if got.status != status {
		t.Fatalf("wanted %d %s, got %s", status, code, got)
	}
	var body refusalBody
	decode(t, got, &body)
	if body.Code != code {
		t.Fatalf("the refusal is coded %q, wanted %q: %s", body.Code, code, got)
	}
	if body.Message == "" || body.CorrelationID == "" {
		t.Fatalf("a refusal carries all three members, got %s", got)
	}
	if ct := got.header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("a refusal is %q, wanted application/json", ct)
	}
	return body
}

// unauthenticated asserts the answer a credential this service would not accept
// gets: 401, the fixed sentence, and the challenge header.
func unauthenticated(t *testing.T, got answer) {
	t.Helper()
	body := refused(t, got, http.StatusUnauthorized, "UNAUTHORIZED")
	if body.Message != "the request carries no usable credential" {
		t.Fatalf("the 401 says %q, wanted the fixed sentence", body.Message)
	}
	if challenge := got.header.Get("WWW-Authenticate"); challenge != `Bearer realm="wagering"` {
		t.Fatalf("the 401 challenges with %q, wanted Bearer realm=\"wagering\"", challenge)
	}
}

// The tables a write would land in.
//
// The five the task names, and the two beside them that a write also touches:
// the outbox's per-aggregate sequence counter, and the active-reversal hold. A
// snapshot that watched only five would call a run clean that had moved one of
// the other two.
var writtenTables = []string{
	"wallet",
	"wager_transaction",
	"wallet_ledger_entry",
	"outbox",
	"inbox",
	"outbox_aggregate_sequence",
	"active_reversal",
}

// tableState is what one table held at one instant.
//
// A count alone would not do. An UPDATE leaves it unchanged, and half of what
// this service does to a wallet is an UPDATE, so the digest is over the whole
// of every row: a balance moved, a status settled or a schedule rewritten all
// change it, and a delete changes both.
type tableState struct {
	rows   int64
	digest string
}

// snapshot reads every table a write could land in.
//
// Through the owner connection rather than the service's, because the service's
// role cannot read some of this and a snapshot that inherited the application's
// blind spots would be blind to exactly the writes it is looking for.
func (s *stack) snapshot(t *testing.T) map[string]tableState {
	t.Helper()
	state := make(map[string]tableState, len(writtenTables))
	for _, table := range writtenTables {
		// The name is one of the constants above, so there is nothing to quote
		// against; a table name cannot be a placeholder in any case.
		query := fmt.Sprintf(
			`SELECT count(*), coalesce(md5(string_agg(line, '|' ORDER BY line)), '') `+
				`FROM (SELECT t::text AS line FROM wagering.%s t) rows`, table)
		var held tableState
		if err := s.owner.QueryRow(context.Background(), query).
			Scan(&held.rows, &held.digest); err != nil {
			t.Fatalf("snapshot wagering.%s: %v", table, err)
		}
		state[table] = held
	}
	return state
}

// unchanged asserts that nothing at all moved between two snapshots.
func unchanged(t *testing.T, before, after map[string]tableState, what string) {
	t.Helper()
	for _, table := range writtenTables {
		if before[table] != after[table] {
			t.Errorf("%s changed wagering.%s: %d rows %s became %d rows %s",
				what, table,
				before[table].rows, before[table].digest,
				after[table].rows, after[table].digest)
		}
	}
}

// movedTables names the tables two snapshots disagree about, which is how the
// control in the no-writes scenario shows that the snapshot can see a write at
// all.
func movedTables(before, after map[string]tableState) []string {
	var moved []string
	for _, table := range writtenTables {
		if before[table] != after[table] {
			moved = append(moved, table)
		}
	}
	slices.Sort(moved)
	return moved
}

// headersExceptDate is a response's headers without the one net/http writes
// from the wall clock.
//
// Date is excluded because it is not this API's answer: net/http stamps it, and
// two requests either side of a second boundary differ in it whatever the
// service decided. Everything the service itself sets is compared.
func headersExceptDate(h http.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for name, values := range h {
		if strings.EqualFold(name, "Date") {
			continue
		}
		out[name] = values
	}
	return out
}

// sameAnswer asserts that two responses are the same response: the status, the
// body byte for byte, and every header but Date.
func sameAnswer(t *testing.T, first, second answer, what string) {
	t.Helper()
	if first.status != second.status {
		t.Fatalf("%s: %d and %d", what, first.status, second.status)
	}
	if !bytes.Equal(first.body, second.body) {
		t.Fatalf("%s: bodies differ\n  %s\n  %s", what, first.body, second.body)
	}
	one, two := headersExceptDate(first.header), headersExceptDate(second.header)
	if len(one) != len(two) {
		t.Fatalf("%s: header sets differ\n  %v\n  %v", what, one, two)
	}
	for name, values := range one {
		other, ok := two[name]
		if !ok || strings.Join(values, ",") != strings.Join(other, ",") {
			t.Fatalf("%s: header %s differs, %v and %v", what, name, values, other)
		}
	}
}
