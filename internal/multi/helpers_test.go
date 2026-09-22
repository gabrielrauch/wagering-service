//go:build multi

// The fixtures: what a scenario sends over either transport, what it reads
// back, and what it asks the database afterwards.
package multi

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// currency is the one currency this suite deals in. Nothing here is about what
// happens across two of them; the domain's own suite is.
const currency = "BRL"

// The budgets a scenario waits under.
//
// Generous rather than tight, and for one reason: every one of them bounds
// something that is allowed to take a while — a container that is still
// starting, a queue's visibility timeout, a poll interval — and a budget tuned
// to a fast laptop is a suite that fails on a loaded one for no finding.
//
// Ninety seconds rather than sixty for the three that wait on another process,
// after one run of `go test -tags multi ./...` failed under a load none of
// these scenarios produce on their own: six worlds starting at once beside the
// PostgreSQL containers the rest of the tree's suites bring up. It cost nothing
// to raise — a budget is an upper bound and not a sleep — and the alternative
// is a suite whose verdict depends on what else the machine was doing.
const (
	// pollInterval is how often anything here looks again.
	pollInterval = 50 * time.Millisecond
	// readyBudget bounds a process becoming able to serve.
	readyBudget = 90 * time.Second
	// settleBudget bounds work reaching the database once it has been asked
	// for. It covers a redelivery at the visibility timeouts these worlds use.
	settleBudget = 90 * time.Second
	// shutdownBudget bounds a process stopping when it is asked to.
	shutdownBudget = 45 * time.Second
	// faultBudget bounds an armed process reaching the instant it dies at.
	faultBudget = 90 * time.Second
)

// The clients the realm holds, by the identity each one stands for. Every
// secret in deploy/keycloak/realm-export.json is the client id with "-secret"
// after it, and it could not be otherwise: the realm is imported from a file in
// this repository.
const (
	providerA     = "provider-a"
	providerB     = "provider-b"
	walletService = "wallet-service"
)

// submission is the body a provider posts to /wagering/transactions.
//
// This suite's own type rather than the handler's, deliberately. The whole
// point of standing outside the process is to be a caller: a test that built
// its request out of the struct the handler decodes into could not notice a
// member being renamed on one side only — and the scenario that submits the
// same operation over both transports exists to catch exactly that.
//
// The idempotency key is not a member. It rides in a header, because it is
// about this delivery of the operation rather than about the operation.
type submission struct {
	Provider                       string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
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

// envelope is a message on the inbound queue.
//
// Written out here beside [submission] rather than shared with it, which is the
// point: the two contracts carry the same operation and disagree about where
// the idempotency key lives, and this is the only place in the tree where both
// spellings are stated by the same reader.
type envelope struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurredAt"`
	Data       queued    `json:"data"`
}

// queued is a submission as the queue carries it: the same business fields,
// plus the idempotency key as a member because a queue has no headers.
type queued struct {
	Provider                       string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	IdempotencyKey                 string `json:"idempotencyKey"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Money                          amount `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

// messageType is the one type the consumer accepts on the inbound queue, as
// the specification spells it.
const messageType = "WagerTransactionRequested"

// operationAnswer is the view a submission and a read both answer with.
type operationAnswer struct {
	TransactionID         string  `json:"transactionId"`
	ExternalTransactionID string  `json:"externalTransactionId"`
	Kind                  string  `json:"kind"`
	Status                string  `json:"status"`
	Money                 amount  `json:"money"`
	Balance               *amount `json:"balance"`
	FailureCode           string  `json:"failureCode"`
	IdempotentReplay      bool    `json:"idempotentReplay"`
}

// walletAnswer is the view /wallets answers with. The wallet's identifier is
// spelled id, as the specification spells it.
type walletAnswer struct {
	WalletID string `json:"id"`
	PlayerID string `json:"playerId"`
	Balance  amount `json:"balance"`
	Version  uint64 `json:"version"`
}

// reconciliation is what checking a wallet against its ledger found, in the
// specification's six members.
type reconciliation struct {
	WalletID          string `json:"walletId"`
	StoredBalance     amount `json:"storedBalance"`
	CalculatedBalance amount `json:"calculatedBalance"`
	Difference        amount `json:"difference"`
	Consistent        bool   `json:"consistent"`
	CheckedEntries    int    `json:"checkedEntries"`
}

// The statuses and failure codes this suite asserts on, as literals.
//
// Literals rather than the domain's own constants, for the same reason the wire
// types above are written out: these are what a provider reads off the
// contract, and a suite that imported the value it is checking would agree with
// the service about a rename that broke every caller.
const (
	processed        = "PROCESSED"
	rejected         = "REJECTED"
	pendingReference = "PENDING_REFERENCE"

	insufficientFunds = "INSUFFICIENT_FUNDS"
	referenceNotFound = "REFERENCE_NOT_FOUND"
)

// bet builds the submission a provider makes for a wager, against the wallet
// the scenario opened: a provider names both the player and the wallet it
// addresses, as one that was told the id at opening would.
func bet(provider, external string, wallet walletAnswer, value string) submission {
	return submission{
		Provider:              provider,
		ExternalTransactionID: external,
		PlayerID:              wallet.PlayerID,
		WalletID:              wallet.WalletID,
		RoundID:               "round-" + external,
		GameID:                "game-1",
		Kind:                  "BET",
		Money:                 amount{Amount: value, Currency: currency},
	}
}

// refund builds the submission that returns a bet's stake, naming the bet by
// the identifier the provider gave it rather than by ours.
func refund(provider, external string, wallet walletAnswer, value, reference string) submission {
	s := bet(provider, external, wallet, value)
	s.Kind = "REFUND"
	s.ReferenceExternalTransactionID = reference
	return s
}

// inRound puts an operation in a named round.
//
// A reversal and the operation it reverses must agree on provider, player,
// wallet, currency AND round, so the two halves of a reversal scenario are
// written with this rather than left to [bet]'s default of one round per
// external identifier — which would make them two rounds and the reversal a
// REFERENCE_MISMATCH.
func inRound(s submission, round string) submission {
	s.RoundID = round
	return s
}

// onTheQueue renders a submission as the message the same operation would
// arrive as, with the key moved from the header into the body.
func onTheQueue(s submission, messageID, key string) envelope {
	return envelope{
		MessageID:  messageID,
		Type:       messageType,
		OccurredAt: time.Now().UTC(),
		Data: queued{
			Provider:                       s.Provider,
			ExternalTransactionID:          s.ExternalTransactionID,
			IdempotencyKey:                 key,
			PlayerID:                       s.PlayerID,
			WalletID:                       s.WalletID,
			RoundID:                        s.RoundID,
			GameID:                         s.GameID,
			Kind:                           s.Kind,
			Money:                          s.Money,
			ReferenceExternalTransactionID: s.ReferenceExternalTransactionID,
		},
	}
}

// encode renders a body the way a provider would send it.
func encode(t *testing.T, body any) string {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode a body: %v", err)
	}
	return string(raw)
}

// call is one request to one instance.
type call struct {
	base   string
	method string
	path   string
	body   string
	// contentType is what the body is, and is JSON when it is left open. The
	// token endpoint is the one request this suite makes that is not.
	contentType    string
	token          string
	idempotencyKey string
}

// answer is what came back.
type answer struct {
	status int
	body   []byte
	header http.Header
}

func (a answer) String() string { return fmt.Sprintf("%d %s", a.status, a.body) }

// client is the one HTTP client this suite uses.
//
// The connection pool is sized for the scenario that sends fifty submissions at
// once across three instances: net/http's default of two idle connections per
// host would make forty-eight of those fifty open a connection, use it and
// throw it away, which measures the host's TIME_WAIT behaviour rather than this
// service's idempotency.
var client = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 64,
		MaxConnsPerHost:     64,
	},
}

// send makes one request and reads the whole answer.
func send(t *testing.T, c call) answer {
	t.Helper()
	got, err := attempt(c)
	if err != nil {
		t.Fatalf("%s %s%s: %v", c.method, c.base, c.path, err)
	}
	return got
}

// attempt is [send] without the assertion, for the callers that are racing
// several requests and must report the failure from their own goroutine.
func attempt(c call) (answer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var body io.Reader
	if c.body != "" {
		body = strings.NewReader(c.body)
	}
	request, err := http.NewRequestWithContext(ctx, c.method, c.base+c.path, body)
	if err != nil {
		return answer{}, fmt.Errorf("build the request: %w", err)
	}
	if c.body != "" {
		request.Header.Set("Content-Type", contentTypeOr(c.contentType))
	}
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", c.idempotencyKey)
	}
	response, err := client.Do(request)
	if err != nil {
		return answer{}, err
	}
	defer func() { _ = response.Body.Close() }()
	read, err := io.ReadAll(response.Body)
	if err != nil {
		return answer{}, fmt.Errorf("read the body: %w", err)
	}
	return answer{status: response.StatusCode, body: read, header: response.Header}, nil
}

// submit posts one operation to one instance, as the client named.
func submit(t *testing.T, base string, provider string, body submission, key string) answer {
	t.Helper()
	return send(t, call{
		base:           base,
		method:         http.MethodPost,
		path:           "/wagering/transactions",
		body:           encode(t, body),
		token:          token(t, provider),
		idempotencyKey: key,
	})
}

// openWallet opens a wallet through the API, as the only identity that may.
func openWallet(t *testing.T, base, player, initial string) walletAnswer {
	t.Helper()
	got := send(t, call{
		base:   base,
		method: http.MethodPost,
		path:   "/wallets",
		body: encode(t, map[string]any{
			"playerId":       player,
			"initialBalance": amount{Amount: initial, Currency: currency},
		}),
		token: token(t, walletService),
	})
	if got.status != http.StatusCreated {
		t.Fatalf("opening a wallet for %s answered %s, wanted 201", player, got)
	}
	var view walletAnswer
	decode(t, got, &view)
	return view
}

// decode reads an answer's body, failing the test when it will not.
func decode(t *testing.T, got answer, v any) {
	t.Helper()
	if err := json.Unmarshal(got.body, v); err != nil {
		t.Fatalf("decode %s: %v", got, err)
	}
}

// operationOf reads an answer as the operation view, insisting on the status
// first so that a refusal is reported as itself rather than as a decode error.
//
// Three statuses carry an operation, because a submitted operation can come to
// three things: 200 for one that was applied, 422 for one a business rule
// settled as REJECTED, and 202 for one parked waiting for its reference.
// Anything else is a refusal — a different body in a different shape — and the
// caller wanted to know that rather than to find out from a missing member.
func operationOf(t *testing.T, got answer) operationAnswer {
	t.Helper()
	switch got.status {
	case http.StatusOK, http.StatusAccepted, http.StatusUnprocessableEntity:
	default:
		t.Fatalf("wanted an operation, got %s", got)
	}
	var op operationAnswer
	decode(t, got, &op)
	return op
}

// tokens caches one credential per client for as long as it is usable.
//
// The realm issues five-minute tokens and this suite runs for longer than that,
// so caching without an expiry would fail the last scenarios of a run with a
// 401. Asking for a fresh one per request would instead put a few thousand
// client_credentials grants through Keycloak, which is slow enough to change
// what the concurrency scenarios are measuring.
var tokens struct {
	sync.Mutex
	held map[string]heldToken
}

// heldToken is one credential and the instant it stops being worth presenting.
type heldToken struct {
	value string
	until time.Time
}

// token fetches a client_credentials token for the client named.
//
// The realm's clients are all confidential service accounts, so there is no
// user in this and no browser: the grant is a form post, exactly as `make
// token` makes it.
func token(t *testing.T, client string) string {
	t.Helper()
	tokens.Lock()
	defer tokens.Unlock()
	if tokens.held == nil {
		tokens.held = make(map[string]heldToken)
	}
	if held, ok := tokens.held[client]; ok && time.Now().Before(held.until) {
		return held.value
	}
	value, lifetime := fetchToken(t, client)
	// A minute of margin, so that a token fetched here is never presented in
	// the second it expires.
	tokens.held[client] = heldToken{value: value, until: time.Now().Add(lifetime - time.Minute)}
	return value
}

// fetchToken asks the realm for one credential and says how long it lasts.
func fetchToken(t *testing.T, name string) (string, time.Duration) {
	t.Helper()
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {name},
		"client_secret": {name + "-secret"},
	}
	got := send(t, call{
		base:        keycloakBase,
		method:      http.MethodPost,
		path:        "/realms/wagering/protocol/openid-connect/token",
		body:        form.Encode(),
		contentType: "application/x-www-form-urlencoded",
	})
	if got.status != http.StatusOK {
		t.Fatalf("the realm refused a client_credentials grant for %s: %s", name, got)
	}
	var issued struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	decode(t, got, &issued)
	if issued.AccessToken == "" || issued.ExpiresIn <= 0 {
		t.Fatalf("the realm answered without a usable token for %s: %s", name, got)
	}
	return issued.AccessToken, time.Duration(issued.ExpiresIn) * time.Second
}

// contentTypeOr is what a body is sent as, defaulting to this API's one
// representation.
func contentTypeOr(stated string) string {
	if stated != "" {
		return stated
	}
	return "application/json"
}

// eventually runs attempt until it stops reporting an error or the budget runs
// out, and fails with the last thing it said.
//
// Polling rather than a notification, because what is being waited for is a row
// written by another process: there is nothing to subscribe to, and a fixed
// sleep would either be slower than the suite can afford or shorter than a
// loaded machine needs.
func eventually(t *testing.T, budget time.Duration, what string, attempt func() error) {
	t.Helper()
	deadline := time.Now().Add(budget)
	var last error
	for {
		last = attempt()
		if last == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for %s and it never happened: %v", budget, what, last)
			return
		}
		time.Sleep(pollInterval)
	}
}

// minor is a two-decimal amount in the units the database holds.
//
// Through money.Parse rather than through a multiplication, because it is the
// only thing in this system that converts between the two forms and a test that
// did its own arithmetic on an amount would be the one float on a money path.
func minor(t *testing.T, value string) int64 {
	t.Helper()
	parsed, err := money.Parse(value, currency)
	if err != nil {
		t.Fatalf("parse %q as money: %v", value, err)
	}
	return parsed.MinorUnits()
}

// balanceOf reads a wallet's stored balance in minor units.
func balanceOf(t *testing.T, pool *pgxpool.Pool, wallet string) int64 {
	t.Helper()
	var held int64
	if err := pool.QueryRow(context.Background(),
		`SELECT balance_minor FROM wagering.wallet WHERE id = $1`, wallet).Scan(&held); err != nil {
		t.Fatalf("read the balance of %s: %v", wallet, err)
	}
	return held
}

// ledgerRow is one balance change as the ledger recorded it.
type ledgerRow struct {
	transactionID string
	direction     string
	amountMinor   int64
	walletVersion int64
}

// ledgerOf reads every entry a wallet has, oldest first.
func ledgerOf(t *testing.T, pool *pgxpool.Pool, wallet string) []ledgerRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT transaction_id::text, direction, amount_minor, wallet_version `+
			`FROM wagering.wallet_ledger_entry WHERE wallet_id = $1 ORDER BY wallet_version`, wallet)
	if err != nil {
		t.Fatalf("read the ledger of %s: %v", wallet, err)
	}
	defer rows.Close()
	var found []ledgerRow
	for rows.Next() {
		var row ledgerRow
		if err := rows.Scan(&row.transactionID, &row.direction,
			&row.amountMinor, &row.walletVersion); err != nil {
			t.Fatalf("scan a ledger entry of %s: %v", wallet, err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the ledger of %s: %v", wallet, err)
	}
	return found
}

// operationRow is one wager transaction as the table holds it.
type operationRow struct {
	id                string
	status            string
	failureCode       string
	kind              string
	amountMinor       int64
	referenceAttempts int
	resolvedReference *string
}

// operationFor finds one provider's operation by the identifier the provider
// gave it, and says whether it is there at all.
func operationFor(
	t *testing.T, pool *pgxpool.Pool, provider, external string,
) (operationRow, bool) {
	t.Helper()
	var row operationRow
	var code *string
	err := pool.QueryRow(context.Background(),
		`SELECT id::text, status, failure_code, kind, amount_minor, reference_attempts, `+
			`resolved_reference_id::text FROM wagering.wager_transaction `+
			`WHERE provider = $1 AND external_transaction_id = $2`, provider, external).
		Scan(&row.id, &row.status, &code, &row.kind, &row.amountMinor,
			&row.referenceAttempts, &row.resolvedReference)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return operationRow{}, false
		}
		t.Fatalf("read the operation %s/%s: %v", provider, external, err)
	}
	if code != nil {
		row.failureCode = *code
	}
	return row, true
}

// awaitOperation waits for one provider's operation to reach a status.
func awaitOperation(
	t *testing.T, pool *pgxpool.Pool, provider, external, want string,
) operationRow {
	t.Helper()
	var found operationRow
	eventually(t, settleBudget,
		fmt.Sprintf("%s/%s to be %s", provider, external, want), func() error {
			row, ok := operationFor(t, pool, provider, external)
			if !ok {
				return fmt.Errorf("%s/%s has not been recorded", provider, external)
			}
			if row.status != want {
				return fmt.Errorf("%s/%s is %s", provider, external, row.status)
			}
			found = row
			return nil
		})
	return found
}

// inboxRow is what the inbox holds about one message.
type inboxRow struct {
	payloadHash string
	completed   bool
}

// inboxFor reads the row that makes a redelivery a replay, and says whether one
// is there.
func inboxFor(t *testing.T, pool *pgxpool.Pool, consumer, messageID string) (inboxRow, bool) {
	t.Helper()
	var row inboxRow
	var completed *time.Time
	err := pool.QueryRow(context.Background(),
		`SELECT payload_hash, completed_at FROM wagering.inbox `+
			`WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).
		Scan(&row.payloadHash, &completed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return inboxRow{}, false
		}
		t.Fatalf("read the inbox row for %s/%s: %v", consumer, messageID, err)
	}
	row.completed = completed != nil
	return row, true
}

// countRows answers one count, for the assertions that are about how many of
// something a wallet has rather than about what any one of them says.
func countRows(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	return count
}
