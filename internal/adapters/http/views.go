package httpapi

import (
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// moneyView is how money crosses the wire: a two-decimal string and a currency
// code, which is what money.Money marshals itself as.
//
// Every amount this package writes goes through it, including the ones
// money.Money would happily marshal on its own. That is deliberate.
// [app.Divergence.Difference] is the system's one signed amount and
// money.Money refuses to marshal a negative, so one amount on this contract
// cannot be rendered by the type that renders the others — and a package with
// two ways to write an amount would eventually write that one the wrong way.
// One way, taken from the value's own rendering, cannot.
//
// No float is involved at any point: Amount is the digits money.Money keeps in
// an int64, rendered by money.Money.
type moneyView struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func moneyOf(m money.Money) moneyView {
	return moneyView{Amount: m.Amount(), Currency: m.Currency().String()}
}

// moneyInput is how money arrives: two strings, handed to the application layer
// exactly as they were sent.
//
// It is not a money.Money. Unmarshalling into one would parse the amount here,
// and the amount has to be parsed exactly once, in the application layer, or
// the value that is hashed is not the value the provider sent.
type moneyInput struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// operationView is one wager transaction as a caller sees it.
//
// Balance is present exactly when the operation was processed, and is the
// balance recorded at that moment rather than the wallet's balance now — which
// is what makes a replay answer what the first submission answered, however far
// the wallet has moved since.
//
// FailureCode is present exactly when the operation was rejected.
// ExternalTransactionID is absent on an opening, which has no provider and so
// no identifier of one.
type operationView struct {
	TransactionID         string     `json:"transactionId"`
	ExternalTransactionID string     `json:"externalTransactionId,omitzero"`
	Kind                  string     `json:"kind"`
	Status                string     `json:"status"`
	Money                 moneyView  `json:"money"`
	Balance               *moneyView `json:"balance,omitzero"`
	FailureCode           string     `json:"failureCode,omitzero"`
	IdempotentReplay      bool       `json:"idempotentReplay"`
}

func operationOf(result app.OperationResult) operationView {
	view := operationView{
		TransactionID:         result.TransactionID.String(),
		ExternalTransactionID: result.ExternalTransactionID.String(),
		Kind:                  result.Kind.String(),
		Status:                string(result.Status),
		Money:                 moneyOf(result.Money),
		FailureCode:           string(result.FailureCode),
		IdempotentReplay:      result.IdempotentReplay,
	}
	if result.Balance != nil {
		balance := moneyOf(*result.Balance)
		view.Balance = &balance
	}
	return view
}

// walletView is a wallet as a caller sees it, spelled as the specification
// spells it: the wallet's identifier is id, because the object is the wallet.
//
// Opening is the wager transaction that recorded the starting balance, and is
// absent when the wallet was opened at zero — there is no opening to report,
// because an opening records a starting balance and a wallet opened at zero has
// none. It is present only in the answer to a wallet being opened; reading a
// wallet back reports the wallet, and the opening is in its ledger.
type walletView struct {
	ID        string         `json:"id"`
	PlayerID  string         `json:"playerId"`
	Balance   moneyView      `json:"balance"`
	Version   uint64         `json:"version"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
	Opening   *operationView `json:"opening,omitzero"`
}

func walletOf(view app.WalletView) walletView {
	return walletView{
		ID:        view.ID.String(),
		PlayerID:  view.PlayerID.String(),
		Balance:   moneyOf(view.Balance),
		Version:   view.Version,
		CreatedAt: view.CreatedAt,
		UpdatedAt: view.UpdatedAt,
	}
}

// ledgerEntryView is one balance change.
//
// Its identifier is id, as the specification spells it, and every entry names
// its wallet even though the page it sits in names the same one: an entry is
// a record in its own right, and one copied out of a page should still say
// which wallet it belongs to.
//
// It carries the wallet version its change produced, because (walletId,
// walletVersion) is what orders a ledger exactly: two entries can share a
// creation instant, so a caller sorting on createdAt alone would not get the
// order the cursor pages in.
type ledgerEntryView struct {
	ID            string    `json:"id"`
	WalletID      string    `json:"walletId"`
	TransactionID string    `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         moneyView `json:"money"`
	BalanceBefore moneyView `json:"balanceBefore"`
	BalanceAfter  moneyView `json:"balanceAfter"`
	WalletVersion uint64    `json:"walletVersion"`
	CreatedAt     time.Time `json:"createdAt"`
}

// ledgerPageView is one page of a ledger, oldest first.
//
// NextCursor is absent on the last page. Its absence is how a caller knows to
// stop, which is why there is no count beside it: counting what is left would
// mean reading it.
type ledgerPageView struct {
	WalletID   string            `json:"walletId"`
	Entries    []ledgerEntryView `json:"entries"`
	NextCursor string            `json:"nextCursor,omitzero"`
}

func ledgerPageOf(id wagering.WalletID, page app.LedgerPage) ledgerPageView {
	entries := make([]ledgerEntryView, 0, len(page.Entries))
	for _, entry := range page.Entries {
		entries = append(entries, ledgerEntryView{
			ID:            entry.ID().String(),
			WalletID:      entry.WalletID().String(),
			TransactionID: entry.TransactionID().String(),
			Direction:     entry.Direction().String(),
			Money:         moneyOf(entry.Amount()),
			BalanceBefore: moneyOf(entry.BalanceBefore()),
			BalanceAfter:  moneyOf(entry.BalanceAfter()),
			WalletVersion: entry.WalletVersion(),
			CreatedAt:     entry.CreatedAt(),
		})
	}
	return ledgerPageView{WalletID: id.String(), Entries: entries, NextCursor: page.NextCursor}
}

// reconciliationView is what checking a wallet against its ledger found, in
// the specification's six members.
//
// StoredBalance is what the wallet row holds and CalculatedBalance is what its
// ledger sums to. Difference is stored less calculated and may be negative —
// which way a wallet is out is the first thing an operator asks. It is never a
// correction: a disagreement means either the balance or the ledger is wrong,
// and which one is a question for an operator rather than something to be
// papered over by adjusting the number that is easier to change.
//
// CheckedEntries is how many ledger entries the calculation summed, the
// opening included, so that a wallet found consistent over its whole ledger
// can be told from one found consistent over nothing.
type reconciliationView struct {
	WalletID          string    `json:"walletId"`
	StoredBalance     moneyView `json:"storedBalance"`
	CalculatedBalance moneyView `json:"calculatedBalance"`
	Difference        moneyView `json:"difference"`
	Consistent        bool      `json:"consistent"`
	CheckedEntries    int       `json:"checkedEntries"`
}

func reconciliationOf(report app.Reconciliation) reconciliationView {
	return reconciliationView{
		WalletID:          report.WalletID.String(),
		StoredBalance:     moneyOf(report.Stored),
		CalculatedBalance: moneyOf(report.Reconstructed),
		Difference:        moneyOf(report.Difference),
		Consistent:        report.Consistent,
		CheckedEntries:    report.CheckedEntries,
	}
}

// openWalletRequest is the body of a wallet being opened.
//
// InitialBalance is required and names the currency, because a wallet is a
// player's balance in one currency and a wallet with no currency is not a
// wallet. A wallet with nothing in it is opened with an amount of "0.00", which
// records no opening.
type openWalletRequest struct {
	PlayerID       string     `json:"playerId"`
	InitialBalance moneyInput `json:"initialBalance"`
}

// submitRequest is the body of an operation being submitted, as the
// specification spells it.
//
// The member names are the ones wagering.CanonicalPayload hashes, deliberately:
// the bytes a provider sends, the bytes that are hashed and the bytes that come
// back should be one vocabulary, so that a provider comparing a submission with
// what was recorded is comparing like with like. The wallet is among them: the
// provider names the wallet it addresses, and the service checks that it is the
// player's rather than choosing one on the provider's behalf.
//
// The idempotency key is not among them. It arrives in a header, because it is
// about this delivery of the operation rather than about the operation, and
// because the hash exists to decide whether one key has been reused for two
// different payloads — a key inside the payload would make every submission
// trivially unique and the question unanswerable.
type submitRequest struct {
	Provider                       string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	PlayerID                       string     `json:"playerId"`
	WalletID                       string     `json:"walletId"`
	RoundID                        string     `json:"roundId"`
	GameID                         string     `json:"gameId"`
	Kind                           string     `json:"kind"`
	Money                          moneyInput `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitzero"`
}
