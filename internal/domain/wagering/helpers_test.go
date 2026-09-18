package wagering

import (
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// A fixed clock. Every test passes time in explicitly, so nothing here depends
// on when it runs.
var baseTime = time.Date(2026, 9, 17, 20, 0, 0, 0, time.UTC)

const (
	testProvider = Provider("acme-games")
	testPlayer   = PlayerID("player-1")
	testRound    = RoundID("round-1")
	testGame     = GameID("game-1")
)

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatalf("money.Parse(%q, BRL): %v", amount, err)
	}
	return m
}

func usd(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "USD")
	if err != nil {
		t.Fatalf("money.Parse(%q, USD): %v", amount, err)
	}
	return m
}

// negative builds a negative amount, which Parse cannot produce: the external
// contract has no spelling for one. Only Sub and Neg make them, and they exist
// so that corruption can be written in a test that storage could otherwise hand
// the domain.
func negative(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := brl(t, amount).Neg()
	if err != nil {
		t.Fatalf("negating %s: %v", amount, err)
	}
	return m
}

func currency(t *testing.T, code string) money.Currency {
	t.Helper()
	c, err := money.ParseCurrency(code)
	if err != nil {
		t.Fatalf("money.ParseCurrency(%q): %v", code, err)
	}
	return c
}

// newWallet opens a wallet holding the given balance, supplying the opening
// identifiers exactly when there is an opening to record.
func newWallet(t *testing.T, initial string) *Wallet {
	t.Helper()
	w, _, err := OpenWallet(openInput(t, initial), nil, baseTime)
	if err != nil {
		t.Fatalf("OpenWallet(%s): %v", initial, err)
	}
	return w
}

func openInput(t *testing.T, initial string) OpenWalletInput {
	t.Helper()
	in := OpenWalletInput{
		WalletID:       NewWalletID(),
		PlayerID:       testPlayer,
		InitialBalance: brl(t, initial),
	}
	if in.InitialBalance.IsPositive() {
		in.TransactionID = NewTransactionID()
		in.LedgerEntryID = NewLedgerEntryID()
	}
	return in
}

func newProcessor(t *testing.T) *Processor {
	t.Helper()
	p, err := NewProcessor(ReferencePolicy{MaxAttempts: 3, TTL: time.Minute})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	return p
}

type cmdOption func(*Command)

func withReference(id ExternalTransactionID) cmdOption {
	return func(c *Command) { c.ReferenceExternalTransactionID = id }
}
func withProvider(p Provider) cmdOption { return func(c *Command) { c.Provider = p } }
func withPlayer(p PlayerID) cmdOption   { return func(c *Command) { c.PlayerID = p } }
func withRound(r RoundID) cmdOption     { return func(c *Command) { c.RoundID = r } }
func withMoney(m money.Money) cmdOption { return func(c *Command) { c.Money = m } }

// command builds a valid submission of the given kind, which options may then
// spoil in whatever way a test is about.
func command(t *testing.T, kind Kind, amount string, opts ...cmdOption) Command {
	t.Helper()
	txID := NewTransactionID()
	c := Command{
		TransactionID:         txID,
		Provider:              testProvider,
		ExternalTransactionID: ExternalTransactionID("ext-" + txID.String()),
		IdempotencyKey:        IdempotencyKey("key-" + txID.String()),
		PlayerID:              testPlayer,
		RoundID:               testRound,
		GameID:                testGame,
		Kind:                  kind,
		Money:                 brl(t, amount),
	}
	if kind.MovesMoney() {
		c.LedgerEntryID = NewLedgerEntryID()
	}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// submitOK submits a command and fails the test if the domain reported the
// submission malformed.
func submitOK(t *testing.T, p *Processor, w *Wallet, cmd Command, ref *ReferenceView, now time.Time) Outcome {
	t.Helper()
	out, err := p.Submit(cmd, w, ref, now)
	if err != nil {
		t.Fatalf("Submit(%s): unexpected error %v", cmd.Kind, err)
	}
	return out
}

// mustSubmit submits a command and requires that it completed successfully.
func mustSubmit(t *testing.T, p *Processor, w *Wallet, cmd Command, ref *ReferenceView, now time.Time) Outcome {
	t.Helper()
	out := submitOK(t, p, w, cmd, ref, now)
	if got := out.Transaction.Status(); got != Processed {
		code, _ := out.Transaction.FailureCode()
		t.Fatalf("Submit(%s) reached %s (%s), want %s", cmd.Kind, got, code, Processed)
	}
	return out
}

func assertStatus(t *testing.T, out Outcome, want Status) {
	t.Helper()
	if out.Transaction == nil {
		t.Fatal("outcome carries no transaction")
	}
	if got := out.Transaction.Status(); got != want {
		code, _ := out.Transaction.FailureCode()
		t.Fatalf("status = %s (%s), want %s", got, code, want)
	}
}

func assertRejected(t *testing.T, out Outcome, want failure.Code) {
	t.Helper()
	assertStatus(t, out, Rejected)
	got, ok := out.Transaction.FailureCode()
	if !ok {
		t.Fatal("rejected transaction carries no failure code")
	}
	if got != want {
		t.Errorf("failure code = %s, want %s", got, want)
	}
	if !got.Definitive() {
		t.Errorf("failure code %s is not definitive, but a persisted rejection must be", got)
	}
	if out.LedgerEntry != nil {
		t.Error("a rejected operation produced a ledger entry")
	}
	assertEventTypes(t, out, typeWagerTransactionRejected)
}

func assertBalance(t *testing.T, w *Wallet, want string) {
	t.Helper()
	if got := w.Balance().Amount(); got != want {
		t.Errorf("balance = %s, want %s", got, want)
	}
}

func assertVersion(t *testing.T, w *Wallet, want uint64) {
	t.Helper()
	if got := w.Version(); got != want {
		t.Errorf("version = %d, want %d", got, want)
	}
}

func eventTypes(events []Event) []string {
	types := make([]string, len(events))
	for i, e := range events {
		types[i] = e.EventType()
	}
	return types
}

func assertEventTypes(t *testing.T, out Outcome, want ...string) {
	t.Helper()
	got := eventTypes(out.Events)
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
	for _, e := range out.Events {
		if e.EventVersion() != eventVersion {
			t.Errorf("event %s has version %d, want %d", e.EventType(), e.EventVersion(), eventVersion)
		}
	}
}

// referenceTo builds the view of a transaction that has no reversals yet.
func referenceTo(tx *WagerTransaction) *ReferenceView {
	return &ReferenceView{Transaction: tx}
}

// referenceWith builds the view of a transaction together with the reversals
// pointing at it.
func referenceWith(tx *WagerTransaction, reversals ...ReversalView) *ReferenceView {
	return &ReferenceView{Transaction: tx, Reversals: reversals}
}

// externalID reads a transaction's provider-side identifier.
func externalID(t *testing.T, tx *WagerTransaction) ExternalTransactionID {
	t.Helper()
	id, ok := tx.ExternalTransactionID()
	if !ok {
		t.Fatal("transaction carries no external identifier")
	}
	return id
}
