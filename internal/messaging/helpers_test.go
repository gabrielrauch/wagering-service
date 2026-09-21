//go:build integration

// The fixtures: what a producer puts on the queue, what the other transport
// submits, and how a scenario reads the tables underneath.
package messaging

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
	"github.com/gabrielrauch/wagering-service/internal/workers"
)

// currency is the one currency this suite deals in. Nothing here is testing
// what happens across two of them; the domain's own suite is.
const currency = "BRL"

// provider is the game operator every scenario submits as. The queue is what
// authorises it — see [stack.submitDirectly] for how the submission that does
// not arrive on the queue is authorised, and for what that leaves untested.
const provider = "provider-a"

// amount is money on the wire: a two-decimal string and a currency, never a
// number.
type amount struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// operation is the business half of an inbound message.
//
// It is this suite's own type rather than the consumer's, which is unexported
// in any case: the point of standing here is to be a producer, and a test that
// built its message out of the struct the consumer decodes into could not
// notice a member being renamed on one side only.
type operation struct {
	Provider                       string `json:"provider"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	IdempotencyKey                 string `json:"idempotencyKey"`
	PlayerID                       string `json:"playerId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Money                          amount `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

// envelope is one message as a provider's producer sends it.
type envelope struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurredAt"`
	Data       operation `json:"data"`
}

// operationOf builds one operation. The idempotency key and the round are
// derived from the provider's own external id so that two scenarios sending
// "the same operation" cannot disagree about what that means.
func operationOf(kind, external, player, value string) operation {
	return operation{
		Provider:              provider,
		ExternalTransactionID: external,
		IdempotencyKey:        "key-" + external,
		PlayerID:              player,
		RoundID:               "round-" + external,
		GameID:                "game-1",
		Kind:                  kind,
		Money:                 amount{Amount: value, Currency: currency},
	}
}

// against names the operation this one acts on.
func (o operation) against(reference string) operation {
	o.ReferenceExternalTransactionID = reference
	return o
}

// inRound puts the operation in a named round rather than the one derived from
// its external id.
//
// A reversal and the operation it undoes have to agree on the round: the domain
// refuses a refund whose round differs from its bet's, whatever else matches.
// The two scenarios that pair an operation with its reference therefore name
// the round rather than letting it fall out of two different external ids.
func (o operation) inRound(round string) operation {
	o.RoundID = round
	return o
}

// message wraps an operation in the envelope the consumer accepts.
func message(messageID string, op operation) envelope {
	return envelope{
		MessageID:  messageID,
		Type:       workers.MessageType,
		OccurredAt: time.Now().UTC().Truncate(time.Second),
		Data:       op,
	}
}

// body renders a message the way a producer would send it.
//
// The bytes matter beyond being valid JSON: the inbox fingerprints the body as
// it arrived rather than the fields it parsed to, so two scenarios turn on two
// renderings of one message id differing here.
func body(t *testing.T, e envelope) string {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("encode a message body: %v", err)
	}
	return string(raw)
}

// hashOf fingerprints a body the way the inbox does: SHA-256 over the bytes as
// they arrived, lowercase hex.
//
// Over the bytes rather than the parsed fields, because that is the whole of
// what makes one message id carrying two different bodies detectable — and the
// scenario that sends exactly that is asserting on this value.
func hashOf(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// fields is the operation as the application layer takes it, still in strings.
func (o operation) fields() app.OperationFields {
	return app.OperationFields{
		Provider:                       o.Provider,
		ExternalTransactionID:          o.ExternalTransactionID,
		IdempotencyKey:                 o.IdempotencyKey,
		PlayerID:                       o.PlayerID,
		RoundID:                        o.RoundID,
		GameID:                         o.GameID,
		Kind:                           o.Kind,
		Amount:                         o.Money.Amount,
		Currency:                       o.Money.Currency,
		ReferenceExternalTransactionID: o.ReferenceExternalTransactionID,
	}
}

// minor is an amount in the units the database stores, for an assertion about a
// balance.
//
// Through money.Parse rather than written out, because that is the only thing
// in this system which converts between a decimal string and minor units, and a
// test that did the multiplication itself would be asserting its own arithmetic.
func minor(t *testing.T, value string) int64 {
	t.Helper()
	parsed, err := money.Parse(value, currency)
	if err != nil {
		t.Fatalf("parse %s %s: %v", value, currency, err)
	}
	return parsed.MinorUnits()
}

// submitDirectly submits an operation over the application layer's own door,
// with no inbox identity — which is the shape the HTTP handler produces once it
// has authenticated.
//
// It is deliberately one step short of a real HTTP request, and it is careful
// not to be described as one. The handler verifies a bearer token, reads the
// idempotency key out of a HEADER, decodes a body that has no such member, and
// derives a correlation; then it builds exactly this principal and makes
// exactly this call. Everything from that call onwards is shared, which is what
// the scenario that uses this asserts on; the three steps before it are not
// here, and the test that uses this says so in its own documentation rather
// than leaving it to be inferred.
func (s *stack) submitDirectly(t *testing.T, op operation, correlation string) app.OperationResult {
	t.Helper()
	acting, err := wagering.NewProvider(op.Provider)
	if err != nil {
		t.Fatalf("provider %q: %v", op.Provider, err)
	}
	principal, err := app.NewProviderPrincipal(acting, "api-client")
	if err != nil {
		t.Fatalf("build a provider principal: %v", err)
	}
	result, err := s.wagering.Submit(t.Context(), app.SubmitOperation{
		Principal:   principal,
		Correlation: correlation,
		Fields:      op.fields(),
	})
	if err != nil {
		t.Fatalf("submit %s over the application's own door: %v", op.ExternalTransactionID, err)
	}
	return result
}

// openWallet creates a wallet through the only door that may, and returns its
// identifier — which is also the FIFO group a producer sends that player's
// operations under.
func (s *stack) openWallet(t *testing.T, player, initial string) string {
	t.Helper()
	principal, err := app.NewServicePrincipal("test-fixture")
	if err != nil {
		t.Fatalf("build a service principal: %v", err)
	}
	view, _, err := s.wallets.Open(t.Context(), app.OpenWalletCommand{
		Principal:     principal,
		Correlation:   "open-" + player,
		PlayerID:      player,
		InitialAmount: initial,
		Currency:      currency,
	})
	if err != nil {
		t.Fatalf("open a wallet for %s: %v", player, err)
	}
	return view.ID.String()
}

// balance reads a player's stored balance, in minor units.
func (s *stack) balance(t *testing.T, player string) int64 {
	t.Helper()
	var held int64
	if err := s.owner.QueryRow(t.Context(),
		`SELECT balance_minor FROM wagering.wallet WHERE player_id = $1 AND currency = $2`,
		player, currency).Scan(&held); err != nil {
		t.Fatalf("read %s's balance: %v", player, err)
	}
	return held
}

// storedOperation is one wager transaction as the tables hold it.
type storedOperation struct {
	id                string
	status            string
	failureCode       *string
	resolvedReference *string
	resultBalance     *int64
	referenceAttempts int
}

// operationRow reads the wager transaction a provider submitted under one
// external id, and fails when there is not exactly one.
//
// Exactly one is half the assertion wherever a scenario sends an operation
// twice: a second row for one external id is the failure those scenarios are
// looking for, and a read that took the first would hide it.
func (s *stack) operationRow(t *testing.T, external string) storedOperation {
	t.Helper()
	found, ok := s.findOperation(t, external)
	if !ok {
		t.Fatalf("no wager transaction for %s", external)
	}
	return found
}

// findOperation is [stack.operationRow] for a scenario that is waiting for the
// row to appear, and reports false while it has not.
//
// More than one row is still fatal here rather than a false, because two rows
// for one external id is never a state worth waiting through — it is the
// failure itself.
func (s *stack) findOperation(t *testing.T, external string) (storedOperation, bool) {
	t.Helper()
	rows, err := s.owner.Query(t.Context(),
		`SELECT id::text, status, failure_code, resolved_reference_id::text, `+
			`result_balance_minor, reference_attempts `+
			`FROM wagering.wager_transaction `+
			`WHERE provider = $1 AND external_transaction_id = $2`,
		provider, external)
	if err != nil {
		t.Fatalf("read the operation %s: %v", external, err)
	}
	defer rows.Close()

	var found []storedOperation
	for rows.Next() {
		var op storedOperation
		if err := rows.Scan(&op.id, &op.status, &op.failureCode,
			&op.resolvedReference, &op.resultBalance, &op.referenceAttempts); err != nil {
			t.Fatalf("read the operation %s: %v", external, err)
		}
		found = append(found, op)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the operation %s: %v", external, err)
	}
	if len(found) > 1 {
		t.Fatalf("%d wager transactions for %s, wanted at most one: %+v", len(found), external,
			found)
	}
	if len(found) == 0 {
		return storedOperation{}, false
	}
	return found[0], true
}

// rowCount counts a table, for the assertions that are about how many rows one
// operation produced rather than about what is in them.
func (s *stack) rowCount(t *testing.T, table, where string, args ...any) int {
	t.Helper()
	// table and where are this file's own constants; a table name cannot be a
	// placeholder in any case, and every value below travels as one.
	var count int
	query := fmt.Sprintf("SELECT count(*) FROM wagering.%s WHERE %s", table, where)
	if err := s.owner.QueryRow(t.Context(), query, args...).Scan(&count); err != nil {
		t.Fatalf("count wagering.%s: %v", table, err)
	}
	return count
}

// inboxRow is one record of a message a consumer has handled.
type inboxRow struct {
	consumer  string
	messageID string
	bodyHash  string
}

// inboxRows reads the whole inbox, which is never large in one of these tests.
func (s *stack) inboxRows(t *testing.T) []inboxRow {
	t.Helper()
	rows, err := s.owner.Query(t.Context(),
		`SELECT consumer_name, message_id, payload_hash FROM wagering.inbox `+
			`ORDER BY received_at, message_id`)
	if err != nil {
		t.Fatalf("read the inbox: %v", err)
	}
	defer rows.Close()

	var found []inboxRow
	for rows.Next() {
		var row inboxRow
		if err := rows.Scan(&row.consumer, &row.messageID, &row.bodyHash); err != nil {
			t.Fatalf("read the inbox: %v", err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the inbox: %v", err)
	}
	return found
}

// outboxRow is one event as the outbox holds it.
type outboxRow struct {
	eventID string
	// sequence is the event's position in its wallet's history, carried for a
	// failure message: an event published out of turn is only legible with it.
	sequence  int64
	eventType string
	attempts  int
	claimedBy *string
	published bool
}

// outboxRows reads the outbox in the order a publisher would claim it.
func (s *stack) outboxRows(t *testing.T) []outboxRow {
	t.Helper()
	rows, err := s.owner.Query(t.Context(),
		`SELECT event_id::text, aggregate_sequence, event_type, `+
			`attempts, claimed_by, published_at IS NOT NULL `+
			`FROM wagering.outbox ORDER BY aggregate_id, aggregate_sequence`)
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	defer rows.Close()

	var found []outboxRow
	for rows.Next() {
		var row outboxRow
		if err := rows.Scan(&row.eventID, &row.sequence, &row.eventType,
			&row.attempts, &row.claimedBy, &row.published); err != nil {
			t.Fatalf("read the outbox: %v", err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	return found
}

// publishedEvents names the events the outbox says have reached the queue.
func (s *stack) publishedEvents(t *testing.T) map[string]bool {
	t.Helper()
	published := map[string]bool{}
	for _, row := range s.outboxRows(t) {
		if row.published {
			published[row.eventID] = true
		}
	}
	return published
}

// eventually polls until want reports no reason to keep waiting, and fails with
// the last reason when the budget runs out.
//
// Polling rather than sleeping a fixed time, because every wait here is for a
// worker loop to come round and the interval between turns is configuration
// rather than a promise. The reason is carried out of the closure so that a
// timeout says what was still wrong rather than that something was.
func eventually(t *testing.T, within time.Duration, what string, want func() error) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last error
	for {
		last = want()
		if last == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for %s: %v", within, what, last)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// exec runs one statement as the owner, for the fixtures that have to arrange
// something the application deliberately cannot.
func (s *stack) exec(t *testing.T, statement string, args ...any) {
	t.Helper()
	if _, err := s.owner.Exec(t.Context(), statement, args...); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}
