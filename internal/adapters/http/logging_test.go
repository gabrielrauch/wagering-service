package httpapi

import (
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The log is where the distinctions this package keeps off the wire survive.
// Both of the tests below assert something no response body can: that a
// difference deliberately erased from the answer was nonetheless written down.

// A provider walking another provider's identifiers and a caller asking for
// something absent are one answer. They are two mornings for whoever runs this.
func TestAForeignReadIsCalledOutByNameAndAnOrdinaryMissIsNot(t *testing.T) {
	t.Parallel()
	const foreignLine = "a provider asked for another provider's operation"
	const message = `no operation "0199c0de-0000-7000-8000-000000000001"`

	t.Run("foreign", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.auth.principal = providerPrincipal(t, "acme")
		h.wagering.err = classified(app.NotFound, "", message, app.ErrForeignOperation)

		recorder := h.do(t, request{
			method: http.MethodGet,
			path:   "/wagering/transactions/" + wagering.NewTransactionID().String(),
		})

		assertStatus(t, recorder, http.StatusNotFound)
		record := h.logs.find(foreignLine)
		if record == nil {
			t.Fatalf("a foreign read was logged as an ordinary refusal:\n%s", h.logs.rendered())
		}
		if got := attr(record, "correlationId"); got != recorder.Header().Get(correlationHeader) {
			t.Errorf("the line carries correlation %q, want the request's", got)
		}
		if got := attr(record, "status"); got != "404" {
			t.Errorf("the line carries status %q, want 404", got)
		}
	})

	t.Run("an ordinary miss", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.auth.principal = providerPrincipal(t, "acme")
		h.wagering.err = classified(app.NotFound, "", message)

		h.do(t, request{
			method: http.MethodGet,
			path:   "/wagering/transactions/" + wagering.NewTransactionID().String(),
		})

		if record := h.logs.find(foreignLine); record != nil {
			t.Errorf("an ordinary miss was reported as a provider walking identifiers:\n%s",
				h.logs.rendered())
		}
	})
}

// One sentence to the caller, five reasons in the log. The two refinements are
// also ErrKeyUnavailable, so a branch order that tested the broad one first
// would swallow them and a steady stream of invented key identifiers would be
// indistinguishable from a rotation this process missed.
func TestEachCredentialRefusalIsCountedUnderItsOwnName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		sentinels []error
		want      string
	}{
		{
			name:      "an invented key identifier",
			sentinels: []error{oidc.ErrUnknownKey, oidc.ErrKeyUnavailable},
			want:      "unknown key identifier",
		},
		{
			name:      "a refresh the rate limit turned away",
			sentinels: []error{oidc.ErrRefreshDeclined, oidc.ErrKeyUnavailable},
			want:      "key set refreshed too recently",
		},
		{
			name:      "no usable key, with no refinement",
			sentinels: []error{oidc.ErrKeyUnavailable},
			want:      "no usable signing key",
		},
		{
			name:      "a token naming no principal",
			sentinels: []error{oidc.ErrPrincipalUnresolved},
			want:      "token names no principal",
		},
		{
			name:      "a token this service examined and rejected",
			sentinels: []error{oidc.ErrTokenRejected},
			want:      "token rejected",
		},
		{
			name:      "a refusal carrying no sentinel at all",
			sentinels: nil,
			want:      "unclassified",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.auth.err = refusedCredential(c.sentinels...)

			recorder := h.do(t, submission(submitBody))

			// The caller is told the same thing every time.
			assertStatus(t, recorder, http.StatusUnauthorized)
			if got := errorOf(t, recorder).Message; got != unauthenticatedMessage {
				t.Errorf("message = %q, want the one sentence", got)
			}
			record := h.logs.find("credential refused")
			if record == nil {
				t.Fatalf("nothing recorded the refusal:\n%s", h.logs.rendered())
			}
			if got := attr(record, "reason"); got != c.want {
				t.Errorf("reason = %q, want %q", got, c.want)
			}
		})
	}
}

// A credential never reaches a log line, whether it was accepted or refused.
func TestNoLogLineCarriesTheCredential(t *testing.T) {
	t.Parallel()

	for _, refusal := range []error{nil, refusedCredential(oidc.ErrTokenRejected)} {
		h := newHarness(t)
		h.auth.principal = providerPrincipal(t, "acme")
		h.auth.err = refusal
		h.wagering.result = processed(t, false)

		h.do(t, submission(submitBody))

		if strings.Contains(h.logs.rendered(), testCredential) {
			t.Errorf("a log line carried the credential:\n%s", h.logs.rendered())
		}
	}
}

// An operation that reached an answer over HTTP leaves exactly one line, and
// the line is the HTTP door's counterpart of the consumer's "the operation was
// applied": the same identifiers, so that an operation is found by correlation,
// transaction, wallet or provider whichever door it came in by, and `source`
// says which door. Before it existed an HTTP-submitted operation was traceable
// through its spans and through nothing else.
//
// The set of keys is asserted exactly, in both directions. A key missing is an
// operator who cannot pivot; a key added is the one review this line gets, and
// the keys that must never be added — an amount, a balance, a player — are what
// the whole rule is about.
func TestAnAnsweredOperationIsLoggedWithItsIdentifiersAndNothingItWasWorth(t *testing.T) {
	t.Parallel()
	const line = "the operation was answered"

	answered := func(t *testing.T, status wagering.Status, code failure.Code, replay bool) app.OperationResult {
		t.Helper()
		balance := mustMoney(t, "75.00", "BRL")
		return app.OperationResult{
			TransactionID:         wagering.NewTransactionID(),
			ExternalTransactionID: "acme-tx-1",
			WalletID:              wagering.NewWalletID(),
			ProviderID:            "acme",
			Kind:                  wagering.Bet,
			Status:                status,
			Money:                 mustMoney(t, "25.00", "BRL"),
			Balance:               &balance,
			FailureCode:           code,
			IdempotentReplay:      replay,
		}
	}

	cases := []struct {
		name   string
		result func(*testing.T) app.OperationResult
	}{
		{
			name: "a processed submission",
			result: func(t *testing.T) app.OperationResult {
				return answered(t, wagering.Processed, "", false)
			},
		},
		{
			name: "a rejected submission",
			result: func(t *testing.T) app.OperationResult {
				return answered(t, wagering.Rejected, failure.InsufficientFunds, false)
			},
		},
		{
			name: "a replayed submission",
			result: func(t *testing.T) app.OperationResult {
				return answered(t, wagering.Processed, "", true)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.auth.principal = providerPrincipal(t, "acme")
			result := c.result(t)
			h.wagering.result = result

			recorder := h.do(t, submission(submitBody))

			record := h.logs.find(line)
			if record == nil {
				t.Fatalf("no line saying %q:\n%s", line, h.logs.rendered())
			}
			if record.Level != slog.LevelInfo {
				t.Errorf("the line is %s, want INFO: an answer is not an alert", record.Level)
			}
			want := map[string]string{
				"source":        "http",
				"correlationId": recorder.Header().Get(correlationHeader),
				"transactionId": result.TransactionID.String(),
				"walletId":      result.WalletID.String(),
				"providerId":    "acme",
				"kind":          "BET",
				"status":        result.Status.String(),
				"failureCode":   result.FailureCode.String(),
				"replay":        strconv.FormatBool(result.IdempotentReplay),
			}
			if got := attrsOf(record); !maps.Equal(got, want) {
				t.Errorf("the line carries %v\nwant exactly %v", got, want)
			}
			for _, forbidden := range []string{"amount", "balance", "25.00", "75.00", "player-1"} {
				if strings.Contains(h.logs.rendered(), forbidden) {
					t.Errorf("a log line carried %q, which belongs in the database:\n%s",
						forbidden, h.logs.rendered())
				}
			}
		})
	}
}

// A refusal is logged exactly as it was before there was a line for an answer:
// the refusal's own line, with the refusal's own keys, and no answer beside it.
// The line for an answer is written after the use case returned WITHOUT error,
// and a refusal — whether this package's own or the application layer's — is
// the other branch.
func TestARefusedSubmissionIsNotLoggedAsAnswered(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		adjust func(*harness, *request)
		status int
		line   string
		keys   []string
	}{
		{
			name: "a submission with no idempotency key, refused here",
			adjust: func(_ *harness, req *request) {
				req.without = []string{idempotencyKeyHeader}
			},
			status: http.StatusBadRequest,
			line:   "request refused",
			keys:   []string{"method", "route", "status", "class", "code", "correlationId"},
		},
		{
			name: "a submission the application layer could not answer",
			adjust: func(h *harness, _ *request) {
				h.wagering.err = app.AsRetryable(errors.New("the connection went away"))
			},
			status: http.StatusServiceUnavailable,
			line:   "request failed",
			keys:   []string{"method", "route", "status", "class", "code", "correlationId", "error"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.auth.principal = providerPrincipal(t, "acme")
			req := submission(submitBody)
			c.adjust(h, &req)

			recorder := h.do(t, req)

			assertStatus(t, recorder, c.status)
			if h.logs.find("the operation was answered") != nil {
				t.Errorf("a refused submission was logged as answered:\n%s", h.logs.rendered())
			}
			record := h.logs.find(c.line)
			if record == nil {
				t.Fatalf("no line saying %q:\n%s", c.line, h.logs.rendered())
			}
			got := attrsOf(record)
			if len(got) != len(c.keys) {
				t.Errorf("the refusal carries %v, want exactly the keys %v", got, c.keys)
			}
			for _, key := range c.keys {
				if _, carried := got[key]; !carried {
					t.Errorf("the refusal does not carry %s: %v", key, got)
				}
			}
		})
	}
}

// A wallet that was opened leaves one line naming the thread it was opened
// under and the wallet itself — which names the wallet without naming whose it
// is. The player is not on it, for the reason the span does not carry one: it
// is the one identifier in the request that is a person rather than a record.
// The balance it was opened with is not on it, for the reason no line here
// carries money.
func TestAnOpenedWalletIsLoggedByItsIdentifierAndNotItsOwnerOrItsBalance(t *testing.T) {
	t.Parallel()
	const line = "the wallet was opened"

	for _, c := range []struct {
		name    string
		opening bool
	}{
		{name: "opened empty", opening: false},
		{name: "opened with money in it", opening: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			view := walletOfPlayer(t)
			h.wallets.view = view
			if c.opening {
				opening := app.OperationResult{
					TransactionID: wagering.NewTransactionID(),
					WalletID:      view.ID,
					Kind:          wagering.Opening,
					Status:        wagering.Processed,
					Money:         mustMoney(t, "25.00", "BRL"),
					Balance:       &view.Balance,
				}
				h.wallets.opening = &opening
			}

			recorder := h.do(t, request{
				method: http.MethodPost, path: "/wallets", body: openWalletBody,
			})

			assertStatus(t, recorder, http.StatusCreated)
			record := h.logs.find(line)
			if record == nil {
				t.Fatalf("no line saying %q:\n%s", line, h.logs.rendered())
			}
			if record.Level != slog.LevelInfo {
				t.Errorf("the line is %s, want INFO", record.Level)
			}
			want := map[string]string{
				"correlationId": recorder.Header().Get(correlationHeader),
				"walletId":      view.ID.String(),
			}
			if got := attrsOf(record); !maps.Equal(got, want) {
				t.Errorf("the line carries %v\nwant exactly %v", got, want)
			}
			for _, forbidden := range []string{"amount", "balance", "25.00", "player-1", "playerId"} {
				if strings.Contains(h.logs.rendered(), forbidden) {
					t.Errorf("a log line carried %q:\n%s", forbidden, h.logs.rendered())
				}
			}
		})
	}
}

// An infrastructure failure is reported to an operator, since the caller is
// told nothing but a fixed sentence.
func TestAServerSideFailureIsRecordedWithItsMessage(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.wallets.err = app.AsUnretryable(errors.New("the connection went away"))

	h.do(t, request{
		method: http.MethodGet, path: "/wallets/" + wagering.NewWalletID().String(),
	})

	record := h.logs.find("request failed")
	if record == nil {
		t.Fatalf("a 500 was not recorded:\n%s", h.logs.rendered())
	}
	if !strings.Contains(attr(record, "error"), "the connection went away") {
		t.Errorf("the line says %q, want the failure's own message", attr(record, "error"))
	}
}
