// Package wagering models player wallets and the operations that move money in
// and out of them.
//
// The package depends only on the standard library, [failure] and [money]. It
// knows nothing of HTTP, queues, databases or dependency injection: every
// collaborator arrives as a value and every result leaves as one.
//
// # Shape
//
//   - [Wallet] is the financial aggregate. It owns its balance and is the only
//     thing that may change it.
//   - [WagerTransaction] is the durable record of one operation, owning its own
//     status transitions.
//   - [WalletLedgerEntry] is the immutable record of one balance change.
//   - [Processor] composes the three. It validates a [Command], consults the
//     reference when there is one, asks the wallet to move the money, drives the
//     transaction's status, and returns an [Outcome].
//
// # Identifiers
//
// Identifiers the system owns — wallets, transactions, ledger entries — are
// UUIDs. Identifiers a provider owns — its own transaction ids, player ids,
// rounds, games, idempotency keys — are opaque strings, validated for shape but
// never interpreted, because the idempotency hash must reproduce exactly what
// the provider sent.
//
// The package can mint an identifier on request but never does so implicitly:
// no constructor invents one behind the caller's back, which keeps every
// operation reproducible in a test.
package wagering
