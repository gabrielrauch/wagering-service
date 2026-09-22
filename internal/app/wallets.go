package app

import (
	"context"
	"errors"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// How many ledger entries a page may hold. A caller that asks for nothing gets
// the default; one that asks for more than the maximum gets the maximum, because
// the alternative is refusing a request over a number the caller had no way to
// know.
const (
	defaultLedgerPage = 50
	maxLedgerPage     = 200
)

// Wallets is the service's door onto wallets: opening them, reading them, paging
// their ledgers and reconciling them. No provider reaches any of it.
type Wallets struct {
	tx       TxManager
	clock    Clock
	ids      IDs
	observer ReconciliationObserver
}

// NewWallets wires the wallet path. A nil observer means no divergence hook.
func NewWallets(tx TxManager, clock Clock, ids IDs, observer ReconciliationObserver) (*Wallets, error) {
	switch {
	case tx == nil:
		return nil, defect("wallets needs a transaction manager")
	case clock == nil:
		return nil, defect("wallets needs a clock")
	case ids == nil:
		return nil, defect("wallets needs an identifier source")
	}
	return &Wallets{tx: tx, clock: clock, ids: ids, observer: observer}, nil
}

// Open creates a wallet for a player in a currency, recording its starting
// balance when it has one.
//
// The result is nil when the wallet was opened at zero. There is no opening to
// report, because an opening records a starting balance and a wallet opened at
// zero has none — the domain's rule, surfaced rather than smoothed over with an
// empty value a caller would have to know to ignore.
func (s *Wallets) Open(ctx context.Context, cmd OpenWalletCommand) (WalletView, *OperationResult, error) {
	if err := cmd.Principal.MayAdministerWallets(); err != nil {
		return WalletView{}, nil, err
	}
	if cmd.Correlation == "" {
		return WalletView{}, nil, invalidField("correlationId", failure.MissingRequiredField,
			"opening a wallet needs a correlation id")
	}
	player, err := wagering.NewPlayerID(cmd.PlayerID)
	if err != nil {
		return WalletView{}, nil, classify(err)
	}
	balance, err := money.Parse(cmd.InitialAmount, cmd.Currency)
	if err != nil {
		return WalletView{}, nil, classify(err)
	}

	in := wagering.OpenWalletInput{
		WalletID:       s.ids.WalletID(),
		PlayerID:       player,
		InitialBalance: balance,
	}
	// Minted exactly when there is an opening to record. Minting them
	// unconditionally would hand the domain identifiers for a transaction and an
	// entry it is not going to write, which OpenWalletInput refuses.
	if balance.IsPositive() {
		in.TransactionID = s.ids.TransactionID()
		in.LedgerEntryID = s.ids.LedgerEntryID()
	}

	var (
		view   WalletView
		result *OperationResult
	)
	err = s.tx.WithinMovement(ctx, func(ctx context.Context, r *Repos) error {
		key := wagering.WalletKey{PlayerID: player, Currency: balance.Currency()}
		existing, err := r.Wallets.ByKey(ctx, key)
		if err != nil {
			return err
		}
		now := s.clock.Now()

		// The domain decides, from the value it was handed. A check against a
		// value cannot win a race on its own, which is why the unique index
		// underneath gives the same answer — see the ErrWalletExists branch below.
		wallet, outcome, err := wagering.OpenWallet(in, existing, now)
		if err != nil {
			return classify(err)
		}

		// No wallet lock: the row this would lock is the row it is creating.
		if err := r.Open(ctx, wallet, outcome, cmd.Correlation); err != nil {
			return err
		}
		view = viewOf(wallet)

		if !outcome.Recorded() {
			return nil
		}
		opening := resultOf(outcome.Transaction, false)
		result = &opening

		return emit(ctx, r.Outbox, s.ids, outcome.Events, Trace{Correlation: cmd.Correlation}, now)
	})
	if errors.Is(err, ErrWalletExists) {
		// The same answer the domain gives from `existing`, arrived at the other
		// way. Both exist because only one of them can win a race and only one of
		// them keeps the rule testable without a database.
		return WalletView{}, nil, conflict(failure.WalletAlreadyExists, err,
			"player %q already holds a %s wallet", player, balance.Currency())
	}
	if err != nil {
		return WalletView{}, nil, classify(err)
	}
	return view, result, nil
}

// ByID reads a wallet.
func (s *Wallets) ByID(ctx context.Context, principal Principal, id wagering.WalletID) (WalletView, error) {
	if err := principal.MayAdministerWallets(); err != nil {
		return WalletView{}, err
	}
	var view WalletView
	err := s.tx.WithinSnapshot(ctx, func(ctx context.Context, r *ReadRepos) error {
		wallet, err := requireWallet(ctx, r.Wallets, id)
		if err != nil {
			return err
		}
		view = viewOf(wallet)
		return nil
	})
	if err != nil {
		return WalletView{}, classify(err)
	}
	return view, nil
}

// Ledger reads one page of a wallet's ledger, oldest first.
//
// The order is by wallet version, which is unique per wallet and advances only
// when the balance changes. Ordering by time would not be stable — two entries
// can share an instant — and ordering by an offset would shift under a write.
func (s *Wallets) Ledger(ctx context.Context, principal Principal, q LedgerQuery) (LedgerPage, error) {
	if err := principal.MayAdministerWallets(); err != nil {
		return LedgerPage{}, err
	}
	after, err := decodeCursor(q.Cursor, q.WalletID)
	if err != nil {
		return LedgerPage{}, err
	}
	limit := q.Limit
	switch {
	case limit <= 0:
		limit = defaultLedgerPage
	case limit > maxLedgerPage:
		limit = maxLedgerPage
	}

	var page LedgerPage
	err = s.tx.WithinSnapshot(ctx, func(ctx context.Context, r *ReadRepos) error {
		if _, err := requireWallet(ctx, r.Wallets, q.WalletID); err != nil {
			return err
		}
		entries, err := r.Ledger.Page(ctx, q.WalletID, after, limit)
		if err != nil {
			return err
		}
		page.Entries = entries
		// A full page implies there may be another. A cursor is offered rather
		// than a count, because counting the remainder would mean reading it.
		if len(entries) == limit {
			page.NextCursor = encodeCursor(q.WalletID, entries[len(entries)-1].WalletVersion())
		}
		return nil
	})
	if err != nil {
		return LedgerPage{}, classify(err)
	}
	return page, nil
}

// Reconcile checks a wallet's stored balance against its ledger.
//
// It reads in one consistent snapshot, so the balance and the entries it is
// compared against are the same instant rather than two. It never writes: a
// disagreement means either the balance or the ledger is wrong, and which one is
// a question for an operator, not something to be papered over by adjusting the
// number that is easier to change.
func (s *Wallets) Reconcile(ctx context.Context, principal Principal, id wagering.WalletID) (Reconciliation, error) {
	if err := principal.MayAdministerWallets(); err != nil {
		return Reconciliation{}, err
	}

	var report Reconciliation
	err := s.tx.WithinSnapshot(ctx, func(ctx context.Context, r *ReadRepos) error {
		wallet, err := requireWallet(ctx, r.Wallets, id)
		if err != nil {
			return err
		}
		entries, err := r.Ledger.All(ctx, id)
		if err != nil {
			return err
		}

		finding := wagering.Reconcile(wallet, entries)
		if finding == nil {
			zero, err := money.Zero(wallet.Currency())
			if err != nil {
				return classify(err)
			}
			report = Reconciliation{
				Consistent:     true,
				CheckedEntries: len(entries),
				WalletID:       id,
				Stored:         wallet.Balance(),
				Reconstructed:  wallet.Balance(),
				Difference:     zero,
			}
			return nil
		}

		mismatch, ok := errors.AsType[*wagering.ReconciliationError](finding)
		if !ok {
			// Everything else Reconcile reports — an entry belonging to another
			// wallet, one transaction with two entries, an entry in the wrong
			// currency, an entry that was never constructed — is corruption found
			// in stored state rather than a refusal of anything submitted, so it
			// takes the same class as a balance that does not add up.
			//
			// It keeps its own code, though. This branch is by construction the
			// one where the finding is NOT a balance mismatch, and stamping
			// LEDGER_BALANCE_MISMATCH on it named a failure that had not
			// happened: the catalogue defines that code as a stored balance
			// disagreeing with the ledger summed, and Reconcile returns these
			// before it sums anything. An operator paged for money that is out
			// went looking for a missing amount when what was found was a
			// duplicated entry.
			return auditFinding(finding)
		}

		// Stored less reconstructed, and signed: which way a wallet is out is the
		// first thing an operator asks.
		difference, err := mismatch.Actual().Sub(mismatch.Expected())
		if err != nil {
			return classify(err)
		}
		report = Reconciliation{
			Consistent:     false,
			CheckedEntries: len(entries),
			WalletID:       id,
			Stored:         mismatch.Actual(),
			Reconstructed:  mismatch.Expected(),
			Difference:     difference,
		}
		return nil
	})
	if err != nil {
		return Reconciliation{}, classify(err)
	}

	// After the transaction has closed, and returning nothing. A metric or a log
	// failing must not turn a finding that was successfully made into an error,
	// and holding a snapshot open across a call into an adapter would make the
	// length of the read somebody else's decision.
	if !report.Consistent && s.observer != nil {
		s.observer.WalletDiverged(ctx, report.Divergence)
	}
	return report, nil
}

// requireWallet reads a wallet and refuses the read when there is none.
//
// Absence is (nil, nil) at the port, which is the right shape there — every
// caller has a branch for it. All three of this service's reads then write the
// same branch, so it is written once rather than three times.
func requireWallet(ctx context.Context, r WalletReader, id wagering.WalletID) (*wagering.Wallet, error) {
	wallet, err := r.ByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if wallet == nil {
		return nil, notFound("no wallet %s", id)
	}
	return wallet, nil
}

func viewOf(w *wagering.Wallet) WalletView {
	return WalletView{
		ID:        w.ID(),
		PlayerID:  w.PlayerID(),
		Balance:   w.Balance(),
		Version:   w.Version(),
		CreatedAt: w.CreatedAt(),
		UpdatedAt: w.UpdatedAt(),
	}
}
