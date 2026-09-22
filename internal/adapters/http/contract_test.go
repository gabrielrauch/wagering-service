package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The member names below are the challenge specification's, and they are
// asserted on the raw JSON rather than through this package's view types: a
// view decoded into the struct that rendered it agrees with itself whatever
// the tags say, and the tags are the contract.

// specificationSubmission is the specification's own example body, verbatim,
// with only the identifiers it shows.
const specificationSubmission = `{
  "providerId": "provider-a",
  "externalTransactionId": "transaction-123",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": { "amount": "25.00", "currency": "BRL" }
}`

func TestSubmitDecodesTheSpecificationsBody(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "provider-a")
	h.wagering.result = processed(t, false)

	req := submission(specificationSubmission)
	req.headers[idempotencyKeyHeader] = "provider-a:transaction-123"
	recorder := h.do(t, req)

	assertStatus(t, recorder, http.StatusOK)
	want := app.OperationFields{
		Provider:              "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "provider-a:transaction-123",
		PlayerID:              "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		WalletID:              "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Amount:                "25.00",
		Currency:              "BRL",
	}
	if got := h.wagering.submitted().Fields; got != want {
		t.Errorf("the use case was given\n%+v\nwant the specification's body\n%+v", got, want)
	}
}

// members decodes a response body as a JSON object and returns its member
// names, sorted, so that a contract can be asserted as a set of spellings.
func members(t *testing.T, recorder *httptest.ResponseRecorder) (map[string]json.RawMessage, []string) {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &object); err != nil {
		t.Fatalf("the body is not a JSON object: %v\n%s", err, recorder.Body.String())
	}
	names := make([]string, 0, len(object))
	for name := range object {
		names = append(names, name)
	}
	slices.Sort(names)
	return object, names
}

func TestWalletIsAnsweredWithTheSpecificationsMembers(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	view := walletOfPlayer(t)
	h.wallets.view = view

	recorder := h.do(t, request{method: http.MethodGet, path: "/wallets/" + view.ID.String()})

	assertStatus(t, recorder, http.StatusOK)
	object, names := members(t, recorder)
	for _, required := range []string{"id", "playerId", "balance", "version"} {
		if _, ok := object[required]; !ok {
			t.Errorf("the wallet has no %q member; it has %v", required, names)
		}
	}
	if _, ok := object["walletId"]; ok {
		t.Errorf("the wallet still spells its identifier walletId; the specification says id")
	}
	if got := string(object["id"]); got != `"`+view.ID.String()+`"` {
		t.Errorf("id = %s, want %q", got, view.ID)
	}
}

func TestLedgerEntryIsAnsweredWithTheSpecificationsMembers(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	wallet := wagering.NewWalletID()
	entryID := wagering.NewLedgerEntryID()
	entry, err := wagering.NewWalletLedgerEntry(wagering.LedgerEntryInput{
		ID:            entryID,
		WalletID:      wallet,
		TransactionID: wagering.NewTransactionID(),
		Direction:     wagering.Credit,
		Amount:        mustMoney(t, "25.00", "BRL"),
		BalanceBefore: mustMoney(t, "0.00", "BRL"),
		BalanceAfter:  mustMoney(t, "25.00", "BRL"),
		WalletVersion: 1,
		CreatedAt:     time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("building a ledger entry: %v", err)
	}
	h.wallets.page = app.LedgerPage{Entries: []wagering.WalletLedgerEntry{entry}}

	recorder := h.do(t, request{method: http.MethodGet, path: "/wallets/" + wallet.String() + "/ledger"})

	assertStatus(t, recorder, http.StatusOK)
	// The page names its wallet too; bodyOf decodes strictly, so it is declared
	// rather than ignored.
	var page struct {
		WalletID string                       `json:"walletId"`
		Entries  []map[string]json.RawMessage `json:"entries"`
	}
	bodyOf(t, recorder, &page)
	if len(page.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(page.Entries))
	}
	got := page.Entries[0]
	if string(got["id"]) != `"`+entryID.String()+`"` {
		t.Errorf("entry id = %s, want %q", got["id"], entryID)
	}
	if string(got["walletId"]) != `"`+wallet.String()+`"` {
		t.Errorf("entry walletId = %s, want %q: every entry names its wallet", got["walletId"], wallet)
	}
	if _, ok := got["ledgerEntryId"]; ok {
		t.Error("the entry still spells its identifier ledgerEntryId; the specification says id")
	}
}

func TestReconciliationIsAnsweredWithTheSpecificationsMembers(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	id := wagering.NewWalletID()
	difference, err := mustMoney(t, "20.00", "BRL").Sub(mustMoney(t, "25.00", "BRL"))
	if err != nil {
		t.Fatalf("subtracting: %v", err)
	}
	h.wallets.report = app.Reconciliation{
		WalletID:       id,
		Stored:         mustMoney(t, "20.00", "BRL"),
		Reconstructed:  mustMoney(t, "25.00", "BRL"),
		Difference:     difference,
		Consistent:     false,
		CheckedEntries: 3,
	}

	recorder := h.do(t, request{
		method: http.MethodPost, path: "/wallets/" + id.String() + "/reconciliation",
	})

	assertStatus(t, recorder, http.StatusOK)
	object, names := members(t, recorder)
	want := []string{"calculatedBalance", "checkedEntries", "consistent", "difference", "storedBalance", "walletId"}
	if !slices.Equal(names, want) {
		t.Errorf("the reconciliation has members %v, want exactly %v", names, want)
	}
	var (
		stored, calculated, diff moneyView
		checked                  int
	)
	for name, into := range map[string]any{
		"storedBalance": &stored, "calculatedBalance": &calculated, "difference": &diff, "checkedEntries": &checked,
	} {
		if err := json.Unmarshal(object[name], into); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if stored.Amount != "20.00" || calculated.Amount != "25.00" || diff.Amount != "-5.00" {
		t.Errorf("stored %s, calculated %s, difference %s; want 20.00, 25.00 and -5.00 (stored less calculated)",
			stored.Amount, calculated.Amount, diff.Amount)
	}
	if checked != 3 {
		t.Errorf("checkedEntries = %d, want 3", checked)
	}
}
