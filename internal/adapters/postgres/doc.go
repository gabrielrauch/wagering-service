// Package postgres is the PostgreSQL side of the application's ports.
//
// It talks to the database through pgx with explicit SQL and positional
// parameters — no ORM and no query builder — because every statement here is
// load-bearing in a way a generated one could not be: the wallet lock is
// FOR NO KEY UPDATE rather than FOR UPDATE, the duplicate insert is
// ON CONFLICT DO NOTHING rather than a pre-read, and the claim queries carry a
// head-of-line predicate. A builder would let any of those be changed by
// somebody who did not know why they were written that way.
//
// # What is where
//
//   - [TxManager] opens the one transaction a command runs in and hands the
//     callback the repositories bound to it.
//   - [Health] answers readiness, bounded.
//   - [OutboxClaims] is the publisher's side of the outbox. It is not an
//     internal/app port, because the application layer explicitly does not
//     publish.
//
// # The transaction is not in the context
//
// Every repository here is built from a pgx.Tx and holds it in a field. There
// is no constructor that takes a pool, so a repository obtained anywhere other
// than from a [TxManager] callback does not exist — which is the guarantee a
// context-carried transaction can only ask for. It also means "reject writes
// outside a transaction" is enforced by the compiler rather than by a check
// that has to be remembered at every entry point.
//
// # Rows become domain objects only through rehydration
//
// Nothing here constructs a wallet, a wager transaction or a ledger entry by
// any route but the domain's Rehydrate functions. A row that the domain would refuse is corruption, and corruption
// is reported rather than loaded.
//
// Money crosses as BIGINT minor units plus a currency code, scanned into int64
// and rebuilt with money.FromMinorUnits. There is no float on any path here,
// and no decimal string round-trip either.
//
// # Lock order
//
// The order is the application's, restated because this package is where it is
// actually taken:
//
//	inbox -> wallet lock -> wager_transaction -> (trigger: active_reversal) -> outbox
//
// Nothing in this package reorders it, and the settle door writes the wager
// transaction before the ledger entry and the wallet for that reason: the
// active_reversal trigger fires on the transaction's UPDATE, and it must fire
// while the wallet is already held.
//
// # Errors
//
// Every rule the schema enforces names itself in pgconn.PgError.ConstraintName,
// whether it is a CHECK, a unique index, a foreign key or a trigger. The
// mapping in errors.go branches on that name and never on the SQLSTATE, because
// each SQLSTATE class holds both ordinary business outcomes and signs that
// something is wrong — 23505 on active_reversal_pkey is a reference already
// reversed, while P0001 on wallet_ledger_entry_is_append_only means somebody
// tried to rewrite the ledger. The name tells them apart; the class does not.
package postgres
