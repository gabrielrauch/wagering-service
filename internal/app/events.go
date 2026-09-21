package app

import "github.com/gabrielrauch/wagering-service/internal/domain/money"

// The published payloads, one per domain event.
//
// They are unexported because they are a wire format, not an API: a caller that
// wanted one would be reaching past the envelope to build a payload by hand,
// which is the one thing keeping the events and their published form in step.
//
// money.Money marshals as {"amount":"25.00","currency":"BRL"}, so money crosses
// this boundary as a decimal string without anything here converting it. Field
// names follow the canonical payload's spelling — externalTransactionId, with a
// lowercase d — so that a consumer reading an event and a provider reading back
// their own submission see one vocabulary.
//
// Every balance in these payloads is non-negative by construction: a wallet never
// holds less than nothing, and money.MarshalJSON's refusal of a negative is
// therefore unreachable here. The one signed amount in the system is a
// reconciliation difference, which is deliberately not an event.

type wagerTransactionProcessed struct {
	TransactionID string      `json:"transactionId"`
	WalletID      string      `json:"walletId"`
	PlayerID      string      `json:"playerId"`
	Kind          string      `json:"kind"`
	Money         money.Money `json:"money"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	// ExternalTransactionID is omitted for an opening, which has none, rather
	// than emitted as null — the same rule the canonical payload applies to an
	// absent reference, so that "no external id" has one representation.
	ExternalTransactionID string `json:"externalTransactionId,omitempty"`
}

type wagerTransactionRejected struct {
	TransactionID         string      `json:"transactionId"`
	WalletID              string      `json:"walletId"`
	PlayerID              string      `json:"playerId"`
	Kind                  string      `json:"kind"`
	Money                 money.Money `json:"money"`
	FailureCode           string      `json:"failureCode"`
	ExternalTransactionID string      `json:"externalTransactionId,omitempty"`
}

type walletBalanceChanged struct {
	WalletID      string      `json:"walletId"`
	TransactionID string      `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion uint64      `json:"walletVersion"`
}

type wagerTransactionPendingReference struct {
	TransactionID string `json:"transactionId"`
	WalletID      string `json:"walletId"`
	PlayerID      string `json:"playerId"`
	Kind          string `json:"kind"`
	// A parked operation always names the reference it waits for and always has
	// an external id of its own: only a provider submission can park.
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
	Attempts                       int    `json:"attempts"`
	ExternalTransactionID          string `json:"externalTransactionId"`
}
