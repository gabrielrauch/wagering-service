package httpapi

import (
	"net/http"
	"strconv"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// openWallet creates a wallet for a player in a currency.
//
// It answers 201 and names the wallet in Location. The statuses the response
// mapping enumerates describe what a submitted operation came to, and opening a
// wallet is not one — it creates a resource with an address, which is what 201
// is for. The conflict half of the same story is in that list, and is answered
// the way every conflict here is.
func (a *API) openWallet(w http.ResponseWriter, r *http.Request, principal app.Principal) {
	var body openWalletRequest
	if err := decodeBody(r, &body); err != nil {
		a.fail(w, r, err)
		return
	}

	view, opening, err := a.wallets.Open(r.Context(), app.OpenWalletCommand{
		Principal:     principal,
		Correlation:   correlationFrom(r.Context()),
		PlayerID:      body.PlayerID,
		InitialAmount: body.InitialBalance.Amount,
		Currency:      body.InitialBalance.Currency,
	})
	if err != nil {
		a.fail(w, r, err)
		return
	}

	rendered := walletOf(view)
	if opening != nil {
		operation := operationOf(*opening)
		rendered.Opening = &operation
	}
	w.Header().Set("Location", "/wallets/"+view.ID.String())
	a.writeJSON(w, r, http.StatusCreated, rendered)
}

// readWallet reads a wallet.
func (a *API) readWallet(w http.ResponseWriter, r *http.Request, principal app.Principal) {
	id, err := walletIDOf(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	view, err := a.wallets.ByID(r.Context(), principal, id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.writeJSON(w, r, http.StatusOK, walletOf(view))
}

// readLedger reads one page of a wallet's ledger, oldest first.
//
// The cursor is passed through untouched. It is opaque by design — a caller
// that learned to read it would be depending on how a page is found — so there
// is nothing here to validate against: the application layer decodes it, and
// refuses one that belongs to another wallet.
func (a *API) readLedger(w http.ResponseWriter, r *http.Request, principal app.Principal) {
	id, err := walletIDOf(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	limit, err := limitOf(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	page, err := a.wallets.Ledger(r.Context(), principal, app.LedgerQuery{
		WalletID: id,
		Cursor:   r.URL.Query().Get("cursor"),
		Limit:    limit,
	})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.writeJSON(w, r, http.StatusOK, ledgerPageOf(id, page))
}

// reconcileWallet checks a wallet's stored balance against its ledger.
//
// A wallet that does not balance is still a successful read: the check ran, it
// found what it found, and the report says so. It is a POST because it is the
// wallet's reconciliation being produced, and because a check that reads every
// entry a wallet has is not something a caching intermediary should be invited
// to repeat on its own.
func (a *API) reconcileWallet(w http.ResponseWriter, r *http.Request, principal app.Principal) {
	id, err := walletIDOf(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	report, err := a.wallets.Reconcile(r.Context(), principal, id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.writeJSON(w, r, http.StatusOK, reconciliationOf(report))
}

// walletIDOf reads the wallet a path names.
func walletIDOf(r *http.Request) (wagering.WalletID, error) {
	return wagering.ParseWalletID(r.PathValue("walletId"))
}

// limitOf reads how many ledger entries a page may hold.
//
// An absent limit is nothing at all rather than a number of this package's
// choosing, because the application layer already decides what a caller who
// asked for nothing gets, and a default here would be a second answer to that
// question. A limit that is not a number is refused rather than quietly
// defaulted: a caller who sent one meant something by it.
func limitOf(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		return 0, failure.New(failure.InvalidFieldFormat, "must be a whole number").WithField("limit")
	}
	return limit, nil
}
