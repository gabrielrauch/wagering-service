package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// WageringService is the part of the application's write path this adapter
// calls.
//
// It is declared here rather than taken as *app.Wagering so that a handler test
// can supply a fake, and it names exactly the three methods the routes below
// reach: Resume is the worker's door and has no route, so admitting it would
// advertise a door this transport does not open.
type WageringService interface {
	Submit(ctx context.Context, cmd app.SubmitOperation) (app.OperationResult, error)
	TransactionByID(
		ctx context.Context, principal app.Principal, id wagering.TransactionID,
	) (app.OperationResult, error)
	TransactionByExternalID(
		ctx context.Context, principal app.Principal, id wagering.ExternalTransactionID,
	) (app.OperationResult, error)
}

// WalletService is the part of the application's wallet path this adapter
// calls.
type WalletService interface {
	Open(ctx context.Context, cmd app.OpenWalletCommand) (app.WalletView, *app.OperationResult, error)
	ByID(ctx context.Context, principal app.Principal, id wagering.WalletID) (app.WalletView, error)
	Ledger(ctx context.Context, principal app.Principal, q app.LedgerQuery) (app.LedgerPage, error)
	Reconcile(
		ctx context.Context, principal app.Principal, id wagering.WalletID,
	) (app.Reconciliation, error)
}

// Authenticator turns a bearer credential into the principal a request acts
// under.
//
// *oidc.Authenticator satisfies it. Depending on the interface rather than on
// that type keeps key material out of every handler test: a fake here needs no
// issuer, no JWKS and no signing key to answer either way.
type Authenticator interface {
	Authenticate(ctx context.Context, bearer string) (app.Principal, error)
}

// ReadinessCheck reports whether one dependency this process needs is
// answering. A nil error is ready.
//
// *postgres.Health satisfies it. The queue's equivalent does not exist yet, so
// readiness takes a set of named checks and lets the composition root say what
// is in it — an adapter that invented a queue client in order to probe one
// would be guessing at a contract another task owns.
type ReadinessCheck interface {
	Ready(ctx context.Context) error
}

// Config is everything the API is built from.
//
// A struct rather than a long positional list, so a dependency is named at the
// call site rather than counted.
type Config struct {
	// Wagering and Wallets are the two application services.
	Wagering WageringService
	Wallets  WalletService
	// Authenticator turns a credential into a principal. Every route but the
	// two health checks goes through it.
	Authenticator Authenticator
	// Readiness names the dependencies /health/ready reports on. It may be
	// empty, which reports ready and says so with an empty set rather than
	// inventing confidence it has no check for.
	Readiness map[string]ReadinessCheck
	// ReadinessTimeout bounds the whole readiness probe, not each check in it.
	// Without a bound the endpoint inherits whatever deadline its caller set,
	// and an orchestrator that sets none waits on a dependency that is not
	// answering for as long as the connection stays open — reporting neither
	// ready nor unready, which is the one answer nothing can act on.
	ReadinessTimeout time.Duration
	// MaxBodyBytes bounds a request body.
	//
	// It lives here rather than on [Server] although it is a transport limit,
	// because refusing a body is answering a request and only this side knows
	// the shape an answer has to take.
	MaxBodyBytes int64
	// Logger is where a refusal is counted. It is required: every distinction
	// this package keeps out of a response — a foreign operation, which of the
	// credential refusals happened — exists to be visible on the inside, and an
	// absent logger does not degrade that, it deletes it.
	Logger *slog.Logger
}

// API routes and answers every request this service serves. It implements
// [net/http.Handler].
type API struct {
	wagering         WageringService
	wallets          WalletService
	authenticator    Authenticator
	readiness        map[string]ReadinessCheck
	readinessTimeout time.Duration
	maxBody          int64
	logger           *slog.Logger
	mux              *http.ServeMux
}

// New wires the API.
//
// Every dependency is refused when absent, because an API built without one
// would fail at the first request instead of at start-up, and by then a
// provider is waiting.
func New(cfg Config) (*API, error) {
	switch {
	case cfg.Wagering == nil:
		return nil, errors.New("httpapi: the API needs a wagering service")
	case cfg.Wallets == nil:
		return nil, errors.New("httpapi: the API needs a wallet service")
	case cfg.Authenticator == nil:
		return nil, errors.New("httpapi: the API needs an authenticator")
	case cfg.Logger == nil:
		return nil, errors.New("httpapi: the API needs a logger")
	case cfg.MaxBodyBytes <= 0:
		return nil, errors.New("httpapi: the API needs a positive request body limit")
	case cfg.ReadinessTimeout <= 0:
		return nil, errors.New("httpapi: the API needs a positive readiness timeout")
	}
	for name, check := range cfg.Readiness {
		if check == nil {
			return nil, errors.New("httpapi: readiness check " + name + " is nil")
		}
	}

	api := &API{
		wagering:         cfg.Wagering,
		wallets:          cfg.Wallets,
		authenticator:    cfg.Authenticator,
		readiness:        cfg.Readiness,
		readinessTimeout: cfg.ReadinessTimeout,
		maxBody:          cfg.MaxBodyBytes,
		logger:           cfg.Logger,
	}
	api.mux = api.routes()
	return api, nil
}

// routes registers every pattern this service answers.
//
// The paths are written in net/http's wildcard notation. The task text spells
// them ":walletId"; that is how a path parameter is written down, not a
// requirement to take a routing dependency that reads it.
func (a *API) routes() *http.ServeMux {
	mux := http.NewServeMux()

	// Wallets are the service's to administer. There is no check for that here:
	// Principal.MayAdministerWallets is the one statement of the rule, it is
	// tested where it lives, and a second copy in this package could only ever
	// drift from it.
	mux.Handle("POST /wallets", dispatched(a.authenticated(a.openWallet)))
	mux.Handle("GET /wallets/{walletId}", dispatched(a.authenticated(a.readWallet)))
	mux.Handle("GET /wallets/{walletId}/ledger", dispatched(a.authenticated(a.readLedger)))
	mux.Handle("POST /wallets/{walletId}/reconciliation",
		dispatched(a.authenticated(a.reconcileWallet)))

	mux.Handle("POST /wagering/transactions", dispatched(a.authenticated(a.submitOperation)))
	mux.Handle("GET /wagering/transactions/{transactionId}",
		dispatched(a.authenticated(a.readOperation)))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}",
		dispatched(a.authenticated(a.readProviderOperation)))

	// Public. A liveness probe that needed a credential would restart a healthy
	// process the moment the identity provider went away, and a readiness probe
	// that needed one would take this service out of rotation for the same
	// reason — which is exactly when its dependencies most need reporting on.
	mux.Handle("GET /health/live", dispatched(http.HandlerFunc(a.live)))
	mux.Handle("GET /health/ready", dispatched(http.HandlerFunc(a.ready)))

	return mux
}

// ServeHTTP answers one request.
//
// The correlation is established before anything else, so that every response
// this package writes — including one for a request that never reached a route
// — carries one.
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	correlation, err := correlationOf(r)
	r = r.WithContext(withCorrelation(r.Context(), correlation))
	w.Header().Set(correlationHeader, correlation)
	if err != nil {
		a.fail(w, r, err)
		return
	}

	// Applied to the real writer rather than to the wrapper below, because
	// MaxBytesReader marks the connection as unreusable through the writer it
	// is handed and can only recognise net/http's own.
	r.Body = http.MaxBytesReader(w, r.Body, a.maxBody)

	a.mux.ServeHTTP(&routed{ResponseWriter: w, api: a, request: r}, r)
}

// routed is the writer the mux dispatches through.
//
// It exists for the mux's own two refusals. ServeMux answers a path it does not
// know with plain text and a path it knows under another method with plain text
// and an Allow header, and neither can be replaced: there is no hook on a mux
// for either. Registering a catch-all "/" would capture the first and destroy
// the second, because a pattern with no method matches every method and so
// turns every method mismatch into a miss.
//
// So the refusals are recognised on the way out instead. A handler that ran
// marks this writer, and a 404 or a 405 arriving unmarked can only be the mux's
// own — restated in the contract's shape, with the Allow header the mux already
// set left where it is.
type routed struct {
	http.ResponseWriter
	api     *API
	request *http.Request
	// matched reports that a registered handler ran.
	matched bool
	// restated reports that the mux's refusal has been answered, so the plain
	// text it is about to write is discarded.
	restated bool
}

// WriteHeader restates the mux's own refusals and passes everything else
// through.
func (w *routed) WriteHeader(status int) {
	if !w.matched && !w.restated {
		switch status {
		case http.StatusNotFound:
			w.restated = true
			w.api.refuse(w.ResponseWriter, w.request, status, codeNotFound,
				"no route answers this path")
			return
		case http.StatusMethodNotAllowed:
			w.restated = true
			w.api.refuse(w.ResponseWriter, w.request, status, codeMethodNotAllowed,
				"this path does not answer "+w.request.Method)
			return
		}
	}
	w.ResponseWriter.WriteHeader(status)
}

// Write discards the body of a refusal that has been restated, and passes
// everything else through.
func (w *routed) Write(b []byte) (int, error) {
	if w.restated {
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap exposes the writer underneath, which is what [net/http.ResponseController]
// looks for.
func (w *routed) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// dispatched marks the writer as belonging to a route that matched, which is
// the whole of how [routed] tells a handler's 404 from the mux's.
func dispatched(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rw, ok := w.(*routed); ok {
			rw.matched = true
		}
		h.ServeHTTP(w, r)
	})
}
