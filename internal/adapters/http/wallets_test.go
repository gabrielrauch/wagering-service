package httpapi

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

func walletOfPlayer(t *testing.T) app.WalletView {
	t.Helper()
	at := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	return app.WalletView{
		ID:        wagering.NewWalletID(),
		PlayerID:  "player-1",
		Balance:   mustMoney(t, "25.00", "BRL"),
		Version:   1,
		CreatedAt: at,
		UpdatedAt: at,
	}
}

func TestOpenWalletAnswersTheWalletAndItsOpening(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	view := walletOfPlayer(t)
	h.wallets.view = view
	opening := app.OperationResult{
		TransactionID: wagering.NewTransactionID(),
		Kind:          wagering.Opening,
		Status:        wagering.Processed,
		Money:         mustMoney(t, "25.00", "BRL"),
		Balance:       &view.Balance,
	}
	h.wallets.opening = &opening

	recorder := h.do(t, request{
		method: http.MethodPost, path: "/wallets", body: openWalletBody,
	})

	assertStatus(t, recorder, http.StatusCreated)
	assertJSON(t, recorder)
	if got, want := recorder.Header().Get("Location"), "/wallets/"+view.ID.String(); got != want {
		t.Errorf("Location = %q, want %q", got, want)
	}
	var body walletView
	bodyOf(t, recorder, &body)
	if body.ID != view.ID.String() || body.Balance.Amount != "25.00" {
		t.Errorf("wallet = %+v, want %s holding 25.00", body, view.ID)
	}
	if body.Opening == nil || body.Opening.Kind != string(wagering.Opening) {
		t.Fatalf("opening = %+v, want the OPENING that recorded the starting balance", body.Opening)
	}
	// An opening is raised by this system and has no provider, so there is no
	// provider identifier to report.
	if body.Opening.ExternalTransactionID != "" {
		t.Errorf("the opening named external id %q", body.Opening.ExternalTransactionID)
	}
}

func TestOpenWalletReportsNoOpeningForAWalletOpenedAtZero(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	view := walletOfPlayer(t)
	view.Balance = mustMoney(t, "0.00", "BRL")
	h.wallets.view = view
	h.wallets.opening = nil

	recorder := h.do(t, request{
		method: http.MethodPost, path: "/wallets",
		body: `{"playerId":"player-1","initialBalance":{"amount":"0.00","currency":"BRL"}}`,
	})

	assertStatus(t, recorder, http.StatusCreated)
	var body walletView
	bodyOf(t, recorder, &body)
	if body.Opening != nil {
		t.Errorf("opening = %+v, want none: a wallet opened at zero records no starting balance",
			body.Opening)
	}
}

func TestOpenWalletPassesTheBodyThroughUntouched(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.wallets.view = walletOfPlayer(t)

	h.do(t, request{
		method: http.MethodPost, path: "/wallets",
		body: `{"playerId":" Player-1 ","initialBalance":{"amount":"25.000","currency":"brl"}}`,
	})

	opened := h.wallets.opened()
	if opened.PlayerID != " Player-1 " || opened.InitialAmount != "25.000" || opened.Currency != "brl" {
		t.Errorf("the use case was given %+v, want the submitted bytes", opened)
	}
}

func TestOpenWalletAnswersAWalletThatAlreadyExistsWithAConflict(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.wallets.err = failure.New(failure.WalletAlreadyExists,
		`player "player-1" already holds a BRL wallet`)

	recorder := h.do(t, request{
		method: http.MethodPost, path: "/wallets", body: openWalletBody,
	})

	assertStatus(t, recorder, http.StatusConflict)
	if got := errorOf(t, recorder).Code; got != string(failure.WalletAlreadyExists) {
		t.Errorf("code = %q, want WALLET_ALREADY_EXISTS", got)
	}
}

func TestReadWalletAnswersTheWallet(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	view := walletOfPlayer(t)
	h.wallets.view = view

	recorder := h.do(t, request{method: http.MethodGet, path: "/wallets/" + view.ID.String()})

	assertStatus(t, recorder, http.StatusOK)
	var body walletView
	bodyOf(t, recorder, &body)
	if body.ID != view.ID.String() || body.Version != 1 {
		t.Errorf("wallet = %+v, want %s at version 1", body, view.ID)
	}
	if body.Opening != nil {
		t.Error("reading a wallet reported an opening; the opening is in its ledger")
	}
	if h.wallets.lastWalletID != view.ID {
		t.Errorf("the use case was asked for %s, want %s", h.wallets.lastWalletID, view.ID)
	}
}

func TestReadWalletRefusesAnIdentifierThatIsNotOne(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	recorder := h.do(t, request{method: http.MethodGet, path: "/wallets/not-a-uuid"})

	assertStatus(t, recorder, http.StatusBadRequest)
	if h.wallets.called() != 0 {
		t.Error("an identifier that is not one still reached the use case")
	}
}

func TestReadLedgerPagesAndCarriesTheCursor(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	id := wagering.NewWalletID()
	entry, err := wagering.NewWalletLedgerEntry(wagering.LedgerEntryInput{
		ID:            wagering.NewLedgerEntryID(),
		WalletID:      id,
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
	h.wallets.page = app.LedgerPage{
		Entries:    []wagering.WalletLedgerEntry{entry},
		NextCursor: "MDE5OTowMQ",
	}

	recorder := h.do(t, request{
		method: http.MethodGet,
		path:   "/wallets/" + id.String() + "/ledger?cursor=MDE5OTowMA&limit=25",
	})

	assertStatus(t, recorder, http.StatusOK)
	var body ledgerPageView
	bodyOf(t, recorder, &body)
	if len(body.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(body.Entries))
	}
	got := body.Entries[0]
	if got.Direction != string(wagering.Credit) || got.Money.Amount != "25.00" ||
		got.BalanceAfter.Amount != "25.00" || got.WalletVersion != 1 {
		t.Errorf("entry = %+v, want a credit of 25.00 taking the wallet to 25.00 at version 1", got)
	}
	if body.NextCursor != "MDE5OTowMQ" {
		t.Errorf("nextCursor = %q, want the one the use case returned", body.NextCursor)
	}
	// The cursor is opaque: it is carried, never read.
	if h.wallets.lastQuery.Cursor != "MDE5OTowMA" || h.wallets.lastQuery.Limit != 25 {
		t.Errorf("the use case was asked %+v, want cursor MDE5OTowMA and limit 25",
			h.wallets.lastQuery)
	}
}

func TestReadLedgerOmitsTheCursorOnTheLastPage(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	id := wagering.NewWalletID()
	h.wallets.page = app.LedgerPage{}

	recorder := h.do(t, request{
		method: http.MethodGet, path: "/wallets/" + id.String() + "/ledger",
	})

	assertStatus(t, recorder, http.StatusOK)
	if got := recorder.Body.String(); strings.Contains(got, "nextCursor") {
		t.Errorf("the last page offered a cursor: %s", got)
	}
	if !strings.Contains(recorder.Body.String(), `"entries":[]`) {
		t.Errorf("an empty page rendered as %s, want an empty array", recorder.Body.String())
	}
	// A caller that asked for nothing is not given a limit of this package's
	// choosing; the application layer decides what a page holds.
	if h.wallets.lastQuery.Limit != 0 {
		t.Errorf("limit = %d, want 0 for a caller that named none", h.wallets.lastQuery.Limit)
	}
}

func TestReadLedgerRefusesALimitThatIsNotANumber(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	id := wagering.NewWalletID()

	recorder := h.do(t, request{
		method: http.MethodGet, path: "/wallets/" + id.String() + "/ledger?limit=lots",
	})

	assertStatus(t, recorder, http.StatusBadRequest)
	if got := errorOf(t, recorder).Message; got != "limit: must be a whole number" {
		t.Errorf("message = %q, want the field and the reason", got)
	}
	if h.wallets.called() != 0 {
		t.Error("a limit that is not a number still reached the use case")
	}
}

func TestReconcileReportsWhatItFound(t *testing.T) {
	t.Parallel()
	id := wagering.NewWalletID()

	t.Run("consistent", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.wallets.report = app.Reconciliation{
			Consistent:    true,
			WalletID:      id,
			Stored:        mustMoney(t, "25.00", "BRL"),
			Reconstructed: mustMoney(t, "25.00", "BRL"),
			Difference:    mustMoney(t, "0.00", "BRL"),
		}

		recorder := h.do(t, request{
			method: http.MethodPost, path: "/wallets/" + id.String() + "/reconciliation",
		})

		assertStatus(t, recorder, http.StatusOK)
		var body reconciliationView
		bodyOf(t, recorder, &body)
		if !body.Consistent || body.Difference.Amount != "0.00" {
			t.Errorf("report = %+v, want a consistent wallet", body)
		}
	})

	t.Run("a wallet that is out", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		// Stored less reconstructed, and negative: money.Money refuses to
		// marshal a negative, so this is the amount that proves nothing here
		// renders money by marshalling it.
		difference, err := mustMoney(t, "20.00", "BRL").Sub(mustMoney(t, "25.00", "BRL"))
		if err != nil {
			t.Fatalf("subtracting: %v", err)
		}
		h.wallets.report = app.Reconciliation{
			WalletID:      id,
			Stored:        mustMoney(t, "20.00", "BRL"),
			Reconstructed: mustMoney(t, "25.00", "BRL"),
			Difference:    difference,
		}

		recorder := h.do(t, request{
			method: http.MethodPost, path: "/wallets/" + id.String() + "/reconciliation",
		})

		// A wallet that does not balance is still a successful read: the check
		// ran and the report says what it found.
		assertStatus(t, recorder, http.StatusOK)
		var body reconciliationView
		bodyOf(t, recorder, &body)
		if body.Consistent {
			t.Error("a wallet that is out was reported consistent")
		}
		if body.Difference.Amount != "-5.00" {
			t.Errorf("difference = %q, want -5.00", body.Difference.Amount)
		}
	})
}

func TestReconcileAnswersAnAuditFindingAsThisServicesProblem(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	id := wagering.NewWalletID()
	// A correctable code on an audit finding, which is exactly the case the
	// application layer warns about: what makes a finding an audit finding is
	// where it was found, not the code it carries. The class has to win, or a
	// duplicated ledger entry is answered as a malformed request.
	h.wallets.err = classified(app.Audit, failure.InvalidFieldFormat,
		"two entries record one transaction")

	recorder := h.do(t, request{
		method: http.MethodPost, path: "/wallets/" + id.String() + "/reconciliation",
	})

	// Nothing the caller submitted was wrong and nothing they can send again
	// will change it: the finding is about stored records.
	assertStatus(t, recorder, http.StatusInternalServerError)
	body := errorOf(t, recorder)
	if body.Code != string(failure.InvalidFieldFormat) {
		t.Errorf("code = %q, want the finding's own code", body.Code)
	}
	if body.Message != auditMessage {
		t.Errorf("message = %q, want the fixed sentence", body.Message)
	}
}
