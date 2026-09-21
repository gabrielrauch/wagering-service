// Package app orchestrates the wagering domain: it decides nothing about money
// and everything about when a decision is persisted, in what order, and to whom
// the answer means what.
//
// It depends on the domain and on the port interfaces declared here, and on
// nothing else. There is no pgx, no database/sql, no HTTP, no AWS SDK and no
// dependency-injection framework in this package, and there is no import of any
// of them anywhere below it. The adapters implement these ports; this package
// never learns which ones.
//
// # Two services
//
// [Wagering] is the provider's write path — submitting an operation, reading one
// back — plus [Wagering.Resume], the worker door that carries parked work
// forward. [Wallets] is the service's: opening, reading, paging and reconciling.
// The split is the authorisation boundary, so "a provider cannot reach a wallet"
// is visible in the type a caller holds rather than in a check it must remember.
//
// # One transaction per attempt
//
// Every command runs inside exactly one [TxManager] callback. The manager opens
// the transaction, commits when the callback returns nil, and rolls back
// otherwise; there is no Begin and no Commit for a use case to call, so a
// transaction cannot be leaked, committed twice, or held across a return.
//
// One command can need a second transaction, and exactly one case does: a
// submission that loses a duplicate-key race. The unique violation aborts the
// transaction, so the operation that won can only be read in a new one. That is
// a re-classification rather than a retry — the work is not attempted again, the
// winner is read and reported — and it is bounded at one.
//
// # Lock order
//
// A movement transaction's whole write set is one wallet's rows plus that
// wallet's outbox counter, taken in this order:
//
//	inbox -> wallet lock -> wager_transaction -> (trigger: active_reversal) -> outbox
//
// The outbox keeps a per-aggregate sequence counter, which makes it a second
// per-wallet serialisation point; taking it before the wallet while another
// command holds the wallet and wants the counter is a deadlock. Appending to the
// outbox is therefore the last statement of every callback.
//
// [Wagering.Resume] obeys the same order, which is why it finds its work with an
// unlocked SELECT: claiming the row first and then wanting its wallet closes a
// cycle with a submission holding that wallet and wanting the row — and because a
// reversal must agree with its reference on the wallet, the two are the same
// wallet by construction rather than by coincidence.
//
// # Time and identifiers
//
// Both arrive as ports. Nothing here calls time.Now or mints an identifier out of
// a package-level function, so every command is reproducible in a test. The clock
// is read once per command, under the wallet lock, and that one value stamps the
// transaction, the ledger entry, the wallet and the envelopes.
//
// # Errors
//
// Every error leaving this package carries a [Class], so an HTTP handler and an
// SQS consumer can map the same failure to their own vocabularies without either
// re-deriving what happened. [ClassOf] documents the order and the reasoning.
//
// The one rule worth stating here: a rejection is an outcome, never an error. A
// business rule settling an operation returns a nil error and an
// [OperationResult] whose status says so — it was persisted, it emitted an event,
// and it bound the idempotency key to its payload for good. A failure code
// arriving on an error path means the opposite: the operation never became
// anything.
//
// # What this package does not do
//
// It does not publish; a separate worker reads the outbox. It does not retry; a
// retryable class is a statement to the caller, not a loop. It does not correct a
// balance; reconciliation reports and returns. It does not open a wallet on a
// provider's behalf. It does not commit PENDING — the only non-terminal status
// ever written is PENDING_REFERENCE, and it is always written with a schedule.
// And it does not re-implement a domain rule: what an operation comes to is the
// domain's answer, carried here and stored.
package app
