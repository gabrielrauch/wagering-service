package app_test

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"
	"uuid"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The fakes store rows, not objects.
//
// Everything below keeps snapshots — the same shapes the domain's Rehydrate
// functions take — and rebuilds a domain object on every read. That is what the
// SQL adapter will do, and it buys the tests something a map of live pointers
// would quietly take away: a fake holding objects can hand back a wallet whose
// balance was mutated by a command that later rolled back, and can store a
// transaction the domain would refuse to load. Going through rehydration means
// every value a use case sees has passed the same validation a real row would.
//
// What these reproduce on purpose, because the design rests on it: atomicity,
// every unique constraint, the schedule-iff-parked equivalence, monotonic
// updated_at, the ledger version chain and the wallet-iff-ledger pairing, one
// active reversal per reference with release-on-reversal, read-your-own-writes,
// and two transactions open at once.

type txnRow struct {
	snap          wagering.TransactionSnapshot
	correlation   string
	nextAttemptAt *time.Time
}

func (r txnRow) clone() txnRow {
	out := r
	if r.snap.External != nil {
		ext := *r.snap.External
		out.snap.External = &ext
	}
	if r.snap.Result != nil {
		res := *r.snap.Result
		out.snap.Result = &res
	}
	if r.nextAttemptAt != nil {
		at := *r.nextAttemptAt
		out.nextAttemptAt = &at
	}
	return out
}

// state is the whole database at one instant.
type state struct {
	wallets map[wagering.WalletID]wagering.WalletSnapshot
	byKey   map[wagering.WalletKey]wagering.WalletID
	txns    map[wagering.TransactionID]txnRow
	entries []wagering.LedgerEntryInput
	inbox   map[app.InboxKey]app.InboxRecord
	outbox  []app.Envelope

	// holder is wagering.active_reversal, keyed by the reference being held. A
	// reversal that is itself reversed releases its hold, which the schema does
	// by deleting the row rather than by marking it.
	holder map[wagering.TransactionID]hold
}

// hold is one row of active_reversal. The kind is kept because the release rule
// needs it: a rollback's hold is permanent and a refund's is not.
type hold struct {
	reversalID wagering.TransactionID
	kind       wagering.Kind
}

func newState() *state {
	return &state{
		wallets: map[wagering.WalletID]wagering.WalletSnapshot{},
		byKey:   map[wagering.WalletKey]wagering.WalletID{},
		txns:    map[wagering.TransactionID]txnRow{},
		inbox:   map[app.InboxKey]app.InboxRecord{},
		holder:  map[wagering.TransactionID]hold{},
	}
}

func (s *state) clone() *state {
	out := newState()
	maps.Copy(out.wallets, s.wallets)
	maps.Copy(out.byKey, s.byKey)
	// Not maps.Copy: a txnRow owns pointers, so each one is deep-copied.
	for k, v := range s.txns {
		out.txns[k] = v.clone()
	}
	out.entries = slices.Clone(s.entries)
	maps.Copy(out.inbox, s.inbox)
	out.outbox = slices.Clone(s.outbox)
	maps.Copy(out.holder, s.holder)
	return out
}

// fakeDB is the store every port below reads and writes.
type fakeDB struct {
	committed *state

	begins  int
	commits int

	// failAt makes the named port call return an error once, so a test can drop
	// a command in the middle and observe that nothing it had written survives.
	failAt map[string]error

	// blindToKeyOnce makes the next ByIdempotencyKey answer "not there" for a
	// row that is. It is the one thing the fake cannot produce on its own and
	// the real database produces routinely: a movement transaction is READ
	// COMMITTED, so its two idempotency lookups see two instants, and a
	// submission racing its own twin can read the key before the winner commits
	// and the external id after it. The fake commits atomically into one state,
	// so without this the second half of that sequence is unreachable here.
	blindToKeyOnce bool

	// blindToActiveReversals makes ReferenceFor omit a hold that is really
	// there, which is the one way the ErrReferenceAlreadyReversed backstop can
	// be reached: the domain rejects a held reference properly when the view
	// shows it, so only a view built wrongly gets as far as the trigger. It is
	// how the schema would behave against a reference query that had drifted
	// out of step with active_reversal.
	blindToActiveReversals bool

	// duringRecord runs in the window between a submission's read and its
	// insert, which is where a duplicate race is actually decided. A test uses
	// it to let another submission commit in that window, so the loser's
	// pre-read finds nothing and its insert collides — the real sequence, driven
	// by explicit steps rather than by two goroutines and a hope.
	duringRecord func()

	// afterNextDue runs between the unlocked SELECT that finds due work
	// and the locked re-read that claims it. Under READ COMMITTED a change
	// another transaction commits in that window IS visible to the re-read,
	// which is the whole reason the re-read exists.
	afterNextDue func(*state)
}

func newFakeDB() *fakeDB {
	return &fakeDB{committed: newState(), failAt: map[string]error{}}
}

// Begins counts transactions opened. A test asserting that an authorization
// failure had no effect asserts on this: an empty store proves nothing was
// written, and Begins() == 0 proves nothing was even attempted.
func (d *fakeDB) Begins() int  { return d.begins }
func (d *fakeDB) Commits() int { return d.commits }

func (d *fakeDB) failNext(op string, err error) { d.failAt[op] = err }

// hideNextKeyLookup arranges for the next ByIdempotencyKey to miss a row that
// exists, reproducing the older snapshot the write path's first lookup can be
// answered from.
func (d *fakeDB) hideNextKeyLookup() { d.blindToKeyOnce = true }

// blindToKey reports, once, that the key lookup should answer from before the
// winner committed.
func (d *fakeDB) blindToKey() bool {
	if !d.blindToKeyOnce {
		return false
	}
	d.blindToKeyOnce = false
	return true
}

func (d *fakeDB) fail(op string) error {
	if err, ok := d.failAt[op]; ok {
		delete(d.failAt, op)
		return err
	}
	return nil
}

// WithinMovement runs fn against a working copy and adopts it only on success,
// which is the whole of the atomicity the tests need: a command that fails part
// way leaves the committed state untouched, and a command that succeeds sees its
// own writes while it runs.
func (d *fakeDB) WithinMovement(ctx context.Context, fn func(context.Context, *app.Repos) error) error {
	d.begins++
	if err := ctx.Err(); err != nil {
		return err
	}
	working := d.committed.clone()
	repos := d.movementRepos(working)
	if err := fn(ctx, repos); err != nil {
		return err
	}
	d.committed = working
	d.commits++
	return nil
}

func (d *fakeDB) WithinSnapshot(ctx context.Context, fn func(context.Context, *app.ReadRepos) error) error {
	d.begins++
	if err := ctx.Err(); err != nil {
		return err
	}
	// A snapshot reads a copy, so a write committed while it runs is invisible
	// to it, the way REPEATABLE READ behaves.
	view := &stores{db: d, st: d.committed.clone(), readOnly: true}
	return fn(ctx, &app.ReadRepos{
		Wallets:      walletStore{view},
		Transactions: txnStore{view},
		Ledger:       ledgerStore{view},
	})
}

func (d *fakeDB) movementRepos(st *state) *app.Repos {
	b := &stores{db: d, st: st}
	return &app.Repos{
		Wallets:      walletStore{b},
		Transactions: txnStore{b},
		Inbox:        inboxStore{b},
		Outbox:       outboxStore{b},
		Open:         b.open,
		Settle:       b.settle,
	}
}

// Committed views, for assertions.

func (d *fakeDB) wagerTransactions() []wagering.TransactionSnapshot {
	out := make([]wagering.TransactionSnapshot, 0, len(d.committed.txns))
	for _, r := range d.committed.txns {
		out = append(out, r.clone().snap)
	}
	slices.SortFunc(out, func(a, b wagering.TransactionSnapshot) int {
		return a.CreatedAt.Compare(b.CreatedAt)
	})
	return out
}

// transactionCount is for the assertions that only want "how many", which is
// most of them: wagerTransactions clones and sorts every row to answer it.
func (d *fakeDB) transactionCount() int { return len(d.committed.txns) }

func (d *fakeDB) transaction(id wagering.TransactionID) (txnRow, bool) {
	r, ok := d.committed.txns[id]
	if !ok {
		return txnRow{}, false
	}
	return r.clone(), true
}

func (d *fakeDB) ledgerEntries() []wagering.LedgerEntryInput {
	return slices.Clone(d.committed.entries)
}

func (d *fakeDB) envelopes() []app.Envelope { return slices.Clone(d.committed.outbox) }
func (d *fakeDB) inboxRecords() []app.InboxRecord {
	return slices.Collect(maps.Values(d.committed.inbox))
}

func (d *fakeDB) empty() bool {
	s := d.committed
	return len(s.wallets) == 0 && len(s.txns) == 0 && len(s.entries) == 0 &&
		len(s.inbox) == 0 && len(s.outbox) == 0
}

// Seeding and assertion helpers. These put the store into a state a test needs,
// or read one back out of it; nothing here implements a port.

// seedWallet puts a funded wallet in the store the way one really comes into
// existence — through wagering.OpenWallet, with its opening transaction and the
// ledger entry that records it — so that a test which later reconciles the
// wallet is reconciling something that could exist.
func (d *fakeDB) seedWallet(t *testing.T, player, amount, currency string) wagering.WalletID {
	t.Helper()
	playerID, err := wagering.NewPlayerID(player)
	if err != nil {
		t.Fatalf("seed player: %v", err)
	}
	balance := mustMoney(amount, currency)
	in := wagering.OpenWalletInput{
		WalletID:       wagering.NewWalletID(),
		PlayerID:       playerID,
		InitialBalance: balance,
	}
	if balance.IsPositive() {
		in.TransactionID = wagering.NewTransactionID()
		in.LedgerEntryID = wagering.NewLedgerEntryID()
	}
	at := time.Date(2026, 9, 19, 11, 0, 0, 0, time.UTC)
	wallet, outcome, err := wagering.OpenWallet(in, nil, at)
	if err != nil {
		t.Fatalf("seed wallet: %v", err)
	}
	b := &stores{db: d, st: d.committed}
	if err := b.open(t.Context(), wallet, outcome, "seed"); err != nil {
		t.Fatalf("seed wallet: %v", err)
	}
	return wallet.ID()
}

// corruptPayloadHash rewrites a stored row's fingerprint, which is how a test
// produces an operation this system can no longer carry forward: the hash no
// longer matches the fields, so the domain refuses to continue it. It stands in
// for any reconstruction defect, which is the class of fault that matters here.
func (d *fakeDB) corruptPayloadHash(t *testing.T, id wagering.TransactionID) {
	t.Helper()
	row, ok := d.committed.txns[id]
	if !ok || row.snap.External == nil {
		t.Fatalf("no provider submission %s to corrupt", id)
	}
	row = row.clone()
	row.snap.External.PayloadHash = wagering.PayloadHash(
		"0000000000000000000000000000000000000000000000000000000000000000")
	d.committed.txns[id] = row
}

// corruptBalance rewrites a wallet's stored balance without touching its ledger,
// which is the divergence reconciliation exists to find. Nothing in this system
// can produce it — that is the point: it is what a bad migration, a manual UPDATE
// or a bug in another service would leave behind.
func (d *fakeDB) corruptBalance(t *testing.T, id wagering.WalletID, amount, currency string) {
	t.Helper()
	snap, ok := d.committed.wallets[id]
	if !ok {
		t.Fatalf("no wallet %s", id)
	}
	snap.Balance = mustMoney(amount, currency)
	d.committed.wallets[id] = snap
}

// duplicateLedgerEntry copies a wallet's last ledger entry under a fresh entry
// id, leaving two entries for one transaction.
//
// Like corruptBalance, nothing in this system can produce it: Processor returns
// at most one entry per operation and a wallet has no other way to move. It is
// what a bad migration or a double-write in another service would leave behind,
// and it is the kind of finding reconciliation reports that is NOT a balance
// mismatch.
func (d *fakeDB) duplicateLedgerEntry(t *testing.T, id wagering.WalletID) {
	t.Helper()
	for _, entry := range slices.Backward(d.committed.entries) {
		if entry.WalletID != id {
			continue
		}
		duplicate := entry
		duplicate.ID = wagering.NewLedgerEntryID()
		d.committed.entries = append(d.committed.entries, duplicate)
		return
	}
	t.Fatalf("wallet %s has no ledger entry to duplicate", id)
}

// balanceOf reads a wallet's committed balance.
func (d *fakeDB) balanceOf(t *testing.T, id wagering.WalletID) money.Money {
	t.Helper()
	snap, ok := d.committed.wallets[id]
	if !ok {
		t.Fatalf("no wallet %s", id)
	}
	return snap.Balance
}

func (d *fakeDB) walletVersion(t *testing.T, id wagering.WalletID) uint64 {
	t.Helper()
	snap, ok := d.committed.wallets[id]
	if !ok {
		t.Fatalf("no wallet %s", id)
	}
	return snap.Version
}

// eventTypes lists what was published, in order.
func (d *fakeDB) eventTypes() []string {
	out := make([]string, 0, len(d.committed.outbox))
	for _, e := range d.committed.outbox {
		out = append(out, e.EventType)
	}
	return out
}

// failAtNothing clears any armed failure, for a test that fails a command and
// then checks that the same submission succeeds afterwards.
func (d *fakeDB) failAtNothing() { clear(d.failAt) }

// stores is the shared body of every port implementation. The ports are split
// across five types rather than carried on one, because WalletReader and
// TransactionReader both declare ByID and no single type can answer both.
type stores struct {
	db       *fakeDB
	st       *state
	readOnly bool
}

type (
	walletStore struct{ *stores }
	txnStore    struct{ *stores }
	ledgerStore struct{ *stores }
	inboxStore  struct{ *stores }
	outboxStore struct{ *stores }
)

func (b *stores) write(op string) error {
	if b.readOnly {
		return fmt.Errorf("fake: %s attempted in a read-only transaction", op)
	}
	return b.db.fail(op)
}

// Wallets.

func (w walletStore) ByID(ctx context.Context, id wagering.WalletID) (*wagering.Wallet, error) {
	if err := w.db.fail("wallet.ByID"); err != nil {
		return nil, err
	}
	snap, ok := w.st.wallets[id]
	if !ok {
		return nil, nil
	}
	return wagering.RehydrateWallet(snap)
}

func (w walletStore) ByKey(ctx context.Context, key wagering.WalletKey) (*wagering.Wallet, error) {
	if err := w.db.fail("wallet.ByKey"); err != nil {
		return nil, err
	}
	id, ok := w.st.byKey[key]
	if !ok {
		return nil, nil
	}
	return wagering.RehydrateWallet(w.st.wallets[id])
}

func (w walletStore) LockForMovement(ctx context.Context, key wagering.WalletKey) (*wagering.Wallet, error) {
	if err := w.db.fail("wallet.LockForMovement"); err != nil {
		return nil, err
	}
	return w.ByKey(ctx, key)
}

func (w walletStore) LockByID(ctx context.Context, id wagering.WalletID) (*wagering.Wallet, error) {
	if err := w.db.fail("wallet.LockByID"); err != nil {
		return nil, err
	}
	return w.ByID(ctx, id)
}

// Wager transactions.

func (t txnStore) ByID(ctx context.Context, id wagering.TransactionID) (*app.StoredTransaction, error) {
	if err := t.db.fail("txn.ByID"); err != nil {
		return nil, err
	}
	row, ok := t.st.txns[id]
	if !ok {
		return nil, nil
	}
	return storedFrom(row)
}

func (t txnStore) ByExternal(
	ctx context.Context,
	p wagering.Provider,
	e wagering.ExternalTransactionID,
) (*app.StoredTransaction, error) {
	if err := t.db.fail("txn.ByExternal"); err != nil {
		return nil, err
	}
	row, ok := t.find(func(r txnRow) bool {
		return r.snap.External != nil && r.snap.External.Provider == p && r.snap.External.ExternalTransactionID == e
	})
	if !ok {
		return nil, nil
	}
	return storedFrom(row)
}

func (t txnStore) ByIdempotencyKey(
	ctx context.Context,
	p wagering.Provider,
	k wagering.IdempotencyKey,
) (*app.StoredTransaction, error) {
	if err := t.db.fail("txn.ByIdempotencyKey"); err != nil {
		return nil, err
	}
	if t.db.blindToKey() {
		return nil, nil
	}
	row, ok := t.find(func(r txnRow) bool {
		return r.snap.External != nil && r.snap.External.Provider == p && r.snap.External.IdempotencyKey == k
	})
	if !ok {
		return nil, nil
	}
	return storedFrom(row)
}

// ReferenceFor builds the view the domain reasons over, rehydrating the reversal
// that holds the reference rather than signalling it with a flag — a view whose
// Transaction is nil is skipped by ActiveReversal, which would put
// REFERENCE_ALREADY_REVERSED out of the domain's reach entirely.
func (t txnStore) ReferenceFor(
	ctx context.Context,
	p wagering.Provider,
	e wagering.ExternalTransactionID,
) (*wagering.ReferenceView, error) {
	if err := t.db.fail("txn.ReferenceFor"); err != nil {
		return nil, err
	}
	row, ok := t.find(func(r txnRow) bool {
		return r.snap.External != nil && r.snap.External.Provider == p && r.snap.External.ExternalTransactionID == e
	})
	if !ok {
		return nil, nil
	}
	reference, err := wagering.RehydrateWagerTransaction(row.snap)
	if err != nil {
		return nil, err
	}
	view := &wagering.ReferenceView{Transaction: reference}
	if h, held := t.st.holder[row.snap.ID]; held && !t.db.blindToActiveReversals {
		holder, err := wagering.RehydrateWagerTransaction(t.st.txns[h.reversalID].snap)
		if err != nil {
			return nil, err
		}
		// Reversed is always false: a released hold is deleted rather than
		// marked, so a row that is here is a hold that still stands.
		view.Reversals = append(view.Reversals, wagering.ReversalView{Transaction: holder})
	}
	return view, nil
}

func (t txnStore) find(match func(txnRow) bool) (txnRow, bool) {
	ids := slices.SortedFunc(maps.Keys(t.st.txns), func(a, b wagering.TransactionID) int {
		return t.st.txns[a].snap.CreatedAt.Compare(t.st.txns[b].snap.CreatedAt)
	})
	for _, id := range ids {
		if row := t.st.txns[id]; match(row) {
			return row, true
		}
	}
	return txnRow{}, false
}

// Record claims the idempotency key and the provider's external id before any
// money work. Both unique keys report the same sentinel: a true replay collides
// on both at once and a database names only one of them, so a caller that could
// tell them apart would be relying on index creation order.
func (t txnStore) Record(ctx context.Context, tx *wagering.WagerTransaction, correlation string) error {
	if err := t.write("txn.Record"); err != nil {
		return err
	}
	if during := t.db.duringRecord; during != nil {
		t.db.duringRecord = nil
		during()
	}
	snap := snapshotOf(tx)
	if snap.External != nil {
		ext := *snap.External
		collides := func(r txnRow) bool {
			return r.snap.External != nil && r.snap.External.Provider == ext.Provider &&
				(r.snap.External.ExternalTransactionID == ext.ExternalTransactionID ||
					r.snap.External.IdempotencyKey == ext.IdempotencyKey)
		}
		// Both, and the committed set is the one that matters: an index is not
		// isolated, so a row another transaction inserted first is visible to
		// this one's uniqueness check even though its reads cannot see it.
		if _, taken := t.find(collides); taken {
			return app.ErrDuplicateSubmission
		}
		for _, r := range t.db.committed.txns {
			if collides(r) {
				return app.ErrDuplicateSubmission
			}
		}
	}
	if _, exists := t.st.txns[snap.ID]; exists {
		return app.ErrDuplicateSubmission
	}
	t.st.txns[snap.ID] = txnRow{snap: snap, correlation: correlation}
	return nil
}

// MakeDue wakes the operations waiting on one reference, scoped to the wallet
// the caller already holds. Scoping is the rule, not an optimisation: waking a
// row on another wallet writes outside this transaction's declared write set.
func (t txnStore) MakeDue(ctx context.Context, on app.SettledOperation, at time.Time) (int, error) {
	if err := t.write("txn.MakeDue"); err != nil {
		return 0, err
	}
	var woke int
	for id, row := range t.st.txns {
		if row.snap.WalletID != on.WalletID || row.snap.Status != wagering.PendingReference {
			continue
		}
		if row.snap.External == nil || row.snap.External.Provider != on.Provider ||
			row.snap.External.ReferenceExternalTransactionID != on.External {
			continue
		}
		due := at
		row.nextAttemptAt = &due
		t.st.txns[id] = row
		woke++
	}
	return woke, nil
}

// NextDue scans for the row that has been due longest, breaking ties on the
// transaction id.
//
// The tie-break is not decoration. MakeDue stamps every operation it wakes with
// one instant, so a commit that wakes two leaves them due at exactly the same
// time, and ordering on the schedule alone would leave the choice to Go's map
// iteration order — a different row on every run. A deterministic fake is worth
// more here than a faithful one: the real query is equally free to return
// either, so a test that depended on which came back was never testing anything
// the database promised.
//
// The minimum is scanned for rather than sorted into place, because exactly one
// row is ever returned and materialising the rest to throw them away is work
// that only looks like ordering.
func (t txnStore) NextDue(ctx context.Context, at time.Time) (*app.DueCandidate, error) {
	if err := t.db.fail("txn.NextDue"); err != nil {
		return nil, err
	}
	var (
		best   app.DueCandidate
		bestAt time.Time
		found  bool
	)
	for _, row := range t.st.txns {
		if row.snap.Status != wagering.PendingReference || row.nextAttemptAt == nil {
			continue
		}
		if row.nextAttemptAt.After(at) {
			continue
		}
		if found && cmp.Or(
			row.nextAttemptAt.Compare(bestAt),
			uuid.UUID(row.snap.ID).Compare(uuid.UUID(best.TransactionID)),
		) >= 0 {
			continue
		}
		best = app.DueCandidate{TransactionID: row.snap.ID, WalletID: row.snap.WalletID}
		bestAt = *row.nextAttemptAt
		found = true
	}
	// Before the empty return as much as after it: the hook models a commit
	// landing between the unlocked SELECT and the locked re-read, and that
	// window exists whether or not this call found anything.
	if hook := t.db.afterNextDue; hook != nil {
		t.db.afterNextDue = nil
		hook(t.st)
	}
	if !found {
		return nil, nil
	}
	return &best, nil
}

func (t txnStore) ClaimForUpdate(
	ctx context.Context,
	id wagering.TransactionID,
	at time.Time,
) (*app.StoredTransaction, error) {
	if err := t.db.fail("txn.ClaimForUpdate"); err != nil {
		return nil, err
	}
	row, ok := t.st.txns[id]
	claimable := ok &&
		row.snap.Status == wagering.PendingReference &&
		row.nextAttemptAt != nil &&
		!row.nextAttemptAt.After(at)
	if !claimable {
		return nil, nil
	}
	return storedFrom(row)
}

func (t txnStore) Reschedule(ctx context.Context, id wagering.TransactionID, at time.Time) error {
	if err := t.write("txn.Reschedule"); err != nil {
		return err
	}
	row, ok := t.st.txns[id]
	if !ok || row.snap.Status != wagering.PendingReference {
		return fmt.Errorf("fake: only a parked operation has a schedule: %s", id)
	}
	next := at
	row.nextAttemptAt = &next
	t.st.txns[id] = row
	return nil
}

func (t txnStore) Fail(ctx context.Context, tx *wagering.WagerTransaction) error {
	if err := t.write("txn.Fail"); err != nil {
		return err
	}
	row, ok := t.st.txns[tx.ID()]
	if !ok {
		return fmt.Errorf("fake: no transaction %s to fail", tx.ID())
	}
	snap := snapshotOf(tx)
	if err := guard(row.snap, snap); err != nil {
		return err
	}
	// FAILED leaves PENDING_REFERENCE, so the schedule goes with it.
	t.st.txns[tx.ID()] = txnRow{snap: snap, correlation: row.correlation}
	return nil
}

// Ledger.

func (l ledgerStore) Page(
	ctx context.Context,
	w wagering.WalletID,
	afterVersion uint64,
	limit int,
) ([]wagering.WalletLedgerEntry, error) {
	if err := l.db.fail("ledger.Page"); err != nil {
		return nil, err
	}
	rows := l.rowsFor(w)
	slices.SortFunc(rows, func(a, b wagering.LedgerEntryInput) int {
		return cmp.Compare(a.WalletVersion, b.WalletVersion)
	})
	out := make([]wagering.WalletLedgerEntry, 0, len(rows))
	for _, in := range rows {
		if in.WalletVersion <= afterVersion {
			continue
		}
		entry, err := wagering.RehydrateWalletLedgerEntry(in)
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}

func (l ledgerStore) All(ctx context.Context, w wagering.WalletID) ([]wagering.WalletLedgerEntry, error) {
	if err := l.db.fail("ledger.All"); err != nil {
		return nil, err
	}
	rows := l.rowsFor(w)
	out := make([]wagering.WalletLedgerEntry, 0, len(rows))
	for _, in := range rows {
		entry, err := wagering.RehydrateWalletLedgerEntry(in)
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, nil
}

func (l ledgerStore) rowsFor(w wagering.WalletID) []wagering.LedgerEntryInput {
	out := make([]wagering.LedgerEntryInput, 0, len(l.st.entries))
	for _, in := range l.st.entries {
		if in.WalletID == w {
			out = append(out, in)
		}
	}
	return out
}

// Inbox.

func (i inboxStore) Find(ctx context.Context, k app.InboxKey) (*app.InboxRecord, error) {
	if err := i.db.fail("inbox.Find"); err != nil {
		return nil, err
	}
	rec, ok := i.st.inbox[k]
	if !ok {
		return nil, nil
	}
	return &rec, nil
}

func (i inboxStore) Record(ctx context.Context, m app.InboxMessage, at time.Time) error {
	if err := i.write("inbox.Record"); err != nil {
		return err
	}
	key := m.Key()
	if _, taken := i.st.inbox[key]; taken {
		return fmt.Errorf("fake: inbox_pkey: %s/%s already recorded", key.Consumer, key.MessageID)
	}
	i.st.inbox[key] = app.InboxRecord{
		Consumer: m.Consumer, MessageID: m.MessageID, BodyHash: m.BodyHash,
		ReceivedAt: at, CompletedAt: at,
	}
	return nil
}

// Outbox.

func (o outboxStore) Append(ctx context.Context, envelopes []app.Envelope) error {
	if err := o.write("outbox.Append"); err != nil {
		return err
	}
	o.st.outbox = append(o.st.outbox, envelopes...)
	return nil
}

// open writes a new wallet, with its opening and that opening's entry when the
// wallet was created with money in it.
func (b *stores) open(ctx context.Context, w *wagering.Wallet, o wagering.Outcome, correlation string) error {
	if err := b.write("repos.Open"); err != nil {
		return err
	}
	if _, taken := b.st.byKey[w.Key()]; taken {
		return app.ErrWalletExists
	}
	b.st.wallets[w.ID()] = walletSnapshotOf(w)
	b.st.byKey[w.Key()] = w.ID()
	if !o.Recorded() {
		return nil
	}
	b.st.txns[o.Transaction.ID()] = txnRow{snap: snapshotOf(o.Transaction), correlation: correlation}
	if o.LedgerEntry != nil {
		b.st.entries = append(b.st.entries, entryInputOf(*o.LedgerEntry))
	}
	return nil
}

// settle writes what an outcome produced.
//
// The wallet's new state is taken from the ledger entry rather than passed
// alongside it, and that is deliberate: an entry already carries the balance
// after and the version the change produced, so reading the wallet out of the
// entry makes "the wallet agrees with its ledger" true by construction instead of
// by two writers being handed consistent arguments. There is no second value that
// could disagree.
func (b *stores) settle(ctx context.Context, o wagering.Outcome, nextAttemptAt time.Time) error {
	if err := b.write("repos.Settle"); err != nil {
		return err
	}
	if !o.Recorded() {
		return fmt.Errorf("fake: settle needs a transaction")
	}
	snap, err := b.settleTransaction(o.Transaction, nextAttemptAt)
	if err != nil {
		return err
	}
	// No entry, no wallet write and no hold to maintain: a settle that moved no
	// money stops here, exactly as it did when this was one function.
	if o.LedgerEntry == nil {
		return nil
	}
	if err := b.applyLedgerEntry(*o.LedgerEntry); err != nil {
		return err
	}
	return b.maintainActiveReversal(snap)
}

// settleTransaction writes the transaction's new state and returns the snapshot
// it wrote, having enforced the guard and the schedule equivalence.
func (b *stores) settleTransaction(
	tx *wagering.WagerTransaction,
	nextAttemptAt time.Time,
) (wagering.TransactionSnapshot, error) {
	old, ok := b.st.txns[tx.ID()]
	if !ok {
		return wagering.TransactionSnapshot{}, fmt.Errorf("fake: no transaction %s to settle", tx.ID())
	}
	snap := snapshotOf(tx)
	if err := guard(old.snap, snap); err != nil {
		return wagering.TransactionSnapshot{}, err
	}
	// wager_transaction_only_waiting_is_scheduled, which is an equivalence: a
	// schedule exists exactly while the operation is parked.
	parked := snap.Status == wagering.PendingReference
	scheduled := !nextAttemptAt.IsZero()
	if parked != scheduled {
		return wagering.TransactionSnapshot{}, fmt.Errorf(
			"fake: only_waiting_is_scheduled: status %s with schedule set=%t",
			snap.Status, scheduled)
	}
	// The row takes the address of its own copy. The column is nullable, so the
	// row still holds a pointer, but it is one nothing outside this store can
	// reach — which is what a real INSERT gives you for free.
	var schedule *time.Time
	if scheduled {
		at := nextAttemptAt
		schedule = &at
	}
	b.st.txns[tx.ID()] = txnRow{snap: snap, correlation: old.correlation, nextAttemptAt: schedule}
	return snap, nil
}

// applyLedgerEntry advances the wallet by one entry, enforcing the version chain,
// the balance chain and the monotonic clock the schema enforces.
func (b *stores) applyLedgerEntry(entry wagering.WalletLedgerEntry) error {
	wallet, ok := b.st.wallets[entry.WalletID()]
	if !ok {
		return fmt.Errorf("fake: no wallet %s for entry %s", entry.WalletID(), entry.ID())
	}
	if entry.WalletVersion() != wallet.Version+1 {
		return fmt.Errorf("fake: ledger_chain: entry version %d does not follow wallet version %d",
			entry.WalletVersion(), wallet.Version)
	}
	if !entry.BalanceBefore().Equal(wallet.Balance) {
		return fmt.Errorf("fake: ledger_chain: entry opens at %s, wallet holds %s",
			entry.BalanceBefore().Amount(), wallet.Balance.Amount())
	}
	if entry.CreatedAt().Before(wallet.UpdatedAt) {
		return fmt.Errorf("fake: wallet_clock_moves_forward: %s does not follow %s",
			entry.CreatedAt(), wallet.UpdatedAt)
	}
	wallet.Balance = entry.BalanceAfter()
	wallet.Version = entry.WalletVersion()
	wallet.UpdatedAt = entry.CreatedAt()
	b.st.wallets[entry.WalletID()] = wallet
	b.st.entries = append(b.st.entries, entryInputOf(entry))
	return nil
}

// maintainActiveReversal does what maintain_active_reversal does: first release
// whatever the thing being reversed was itself holding, then refuse what the
// domain would refuse, then take the new hold.
func (b *stores) maintainActiveReversal(snap wagering.TransactionSnapshot) error {
	isNewReversal := snap.Status == wagering.Processed &&
		snap.Kind.IsReversal() &&
		snap.External != nil
	if !isNewReversal {
		return nil
	}
	reference := snap.External.ResolvedReferenceID
	if released, found := b.release(reference); found {
		// The conditional release is the whole of ADR-0003. Releasing on reversal
		// id alone would also release a ROLLBACK's hold, and a rollback applied
		// straight to a bet is permanent.
		if snap.Kind == wagering.Refund || released == wagering.Rollback {
			return fmt.Errorf("fake: active_reversal_reference_is_reversible: a %s cannot reverse a %s",
				snap.Kind, released)
		}
	}
	if _, held := b.st.holder[reference]; held {
		return app.ErrReferenceAlreadyReversed
	}
	b.st.holder[reference] = hold{reversalID: snap.ID, kind: snap.Kind}
	return nil
}

// release removes the hold held BY the given transaction, and reports what kind
// of reversal it was. A bet, a win and an already-released reversal hold nothing.
func (b *stores) release(reversalID wagering.TransactionID) (wagering.Kind, bool) {
	for reference, h := range b.st.holder {
		if h.reversalID == reversalID {
			delete(b.st.holder, reference)
			return h.kind, true
		}
	}
	return "", false
}

// guard is wagering.wager_transaction_guard: what the operation IS arrived with
// the submission and is history, settling is the only thing an update is for,
// and a terminal status is final.
func guard(old, updated wagering.TransactionSnapshot) error {
	sameOperation := old.ID == updated.ID &&
		old.WalletID == updated.WalletID &&
		old.PlayerID == updated.PlayerID &&
		old.Kind == updated.Kind &&
		old.Money.Equal(updated.Money) &&
		old.CreatedAt.Equal(updated.CreatedAt)
	if !sameOperation {
		return fmt.Errorf("fake: wager_transaction_operation_is_immutable: %s", old.ID)
	}
	if (old.External == nil) != (updated.External == nil) {
		return fmt.Errorf("fake: wager_transaction_provider_fields_are_immutable: %s", old.ID)
	}
	if old.External != nil {
		o, n := *old.External, *updated.External
		sameProviderFields := o.Provider == n.Provider &&
			o.ExternalTransactionID == n.ExternalTransactionID &&
			o.IdempotencyKey == n.IdempotencyKey &&
			o.PayloadHash == n.PayloadHash &&
			o.RoundID == n.RoundID &&
			o.GameID == n.GameID &&
			o.ReferenceExternalTransactionID == n.ReferenceExternalTransactionID
		if !sameProviderFields {
			return fmt.Errorf("fake: wager_transaction_provider_fields_are_immutable: %s", old.ID)
		}
		if !o.ResolvedReferenceID.IsZero() && o.ResolvedReferenceID != n.ResolvedReferenceID {
			return fmt.Errorf("fake: wager_transaction_resolution_is_decided_once: %s", old.ID)
		}
	}
	if old.Status != updated.Status {
		if old.Status.IsTerminal() {
			return fmt.Errorf("fake: wager_transaction_terminal_status_is_final: %s is %s", old.ID, old.Status)
		}
		if updated.Status == wagering.Pending {
			return fmt.Errorf("fake: wager_transaction_does_not_return_to_pending: %s", old.ID)
		}
	}
	if updated.UpdatedAt.Before(old.UpdatedAt) {
		return fmt.Errorf("fake: wager_transaction_clock_moves_forward: %s does not follow %s",
			updated.UpdatedAt, old.UpdatedAt)
	}
	return nil
}

// Row mapping. The real adapter will need exactly these, which is the point of
// writing them as row shapes rather than keeping live objects.

func walletSnapshotOf(w *wagering.Wallet) wagering.WalletSnapshot {
	return wagering.WalletSnapshot{
		ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(),
		Version: w.Version(), CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt(),
	}
}

func snapshotOf(tx *wagering.WagerTransaction) wagering.TransactionSnapshot {
	snap := wagering.TransactionSnapshot{
		ID: tx.ID(), WalletID: tx.WalletID(), PlayerID: tx.PlayerID(),
		Kind: tx.Kind(), Money: tx.Money(), Status: tx.Status(),
		CreatedAt: tx.CreatedAt(), UpdatedAt: tx.UpdatedAt(),
		ReferenceAttempts: tx.ReferenceAttempts(),
	}
	if result, ok := tx.Result(); ok {
		snap.Result = &result
	}
	if code, ok := tx.FailureCode(); ok {
		snap.FailureCode = code
	}
	if deadline, ok := tx.ReferenceDeadline(); ok {
		snap.ReferenceDeadline = deadline
	}
	if tx.IsExternal() {
		provider, _ := tx.Provider()
		external, _ := tx.ExternalTransactionID()
		key, _ := tx.IdempotencyKey()
		hash, _ := tx.PayloadHash()
		round, _ := tx.RoundID()
		game, _ := tx.GameID()
		reference, _ := tx.ReferenceExternalTransactionID()
		resolved, _ := tx.ResolvedReferenceID()
		snap.External = &wagering.ExternalSnapshot{
			Provider: provider, ExternalTransactionID: external, IdempotencyKey: key,
			PayloadHash: hash, RoundID: round, GameID: game,
			ReferenceExternalTransactionID: reference, ResolvedReferenceID: resolved,
		}
	}
	return snap
}

func entryInputOf(e wagering.WalletLedgerEntry) wagering.LedgerEntryInput {
	return wagering.LedgerEntryInput{
		ID: e.ID(), WalletID: e.WalletID(), TransactionID: e.TransactionID(),
		Direction: e.Direction(), Amount: e.Amount(),
		BalanceBefore: e.BalanceBefore(), BalanceAfter: e.BalanceAfter(),
		WalletVersion: e.WalletVersion(), CreatedAt: e.CreatedAt(),
	}
}

func storedFrom(row txnRow) (*app.StoredTransaction, error) {
	tx, err := wagering.RehydrateWagerTransaction(row.snap)
	if err != nil {
		return nil, err
	}
	return &app.StoredTransaction{Transaction: tx, Correlation: row.correlation}, nil
}

// Clock, identifiers and the observers.

type fakeClock struct{ at time.Time }

func newFakeClock() *fakeClock {
	return &fakeClock{at: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time          { return c.at }
func (c *fakeClock) advance(d time.Duration) { c.at = c.at.Add(d) }

// fakeIDs mints real identifiers. It remembers none of them: what a command
// produced is read back off the committed rows, which is the same thing the
// production code has to do.
type fakeIDs struct{}

func (f *fakeIDs) WalletID() wagering.WalletID           { return wagering.NewWalletID() }
func (f *fakeIDs) TransactionID() wagering.TransactionID { return wagering.NewTransactionID() }
func (f *fakeIDs) LedgerEntryID() wagering.LedgerEntryID { return wagering.NewLedgerEntryID() }
func (f *fakeIDs) EventID() app.EventID                  { return app.NewEventID() }

type fakeObserver struct {
	divergences []app.Divergence
	defects     []wagering.TransactionID
}

func (o *fakeObserver) WalletDiverged(ctx context.Context, d app.Divergence) {
	o.divergences = append(o.divergences, d)
}

func (o *fakeObserver) CannotCarryForward(ctx context.Context, id wagering.TransactionID, err error) {
	o.defects = append(o.defects, id)
}

// mustMoney is for fixtures, where an unparseable amount is a broken test rather
// than a case under test.
func mustMoney(amount, currency string) money.Money {
	m, err := money.Parse(amount, currency)
	if err != nil {
		panic(err)
	}
	return m
}
