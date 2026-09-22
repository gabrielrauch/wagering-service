package httpapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

func mustMoney(t *testing.T, amount, currency string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, currency)
	if err != nil {
		t.Fatalf("parsing %s %s: %v", amount, currency, err)
	}
	return m
}

// processed is the result of a bet that went through.
func processed(t *testing.T, replay bool) app.OperationResult {
	t.Helper()
	balance := mustMoney(t, "75.00", "BRL")
	return app.OperationResult{
		TransactionID:         wagering.NewTransactionID(),
		ExternalTransactionID: "acme-tx-1",
		Kind:                  wagering.Bet,
		Status:                wagering.Processed,
		Money:                 mustMoney(t, "25.00", "BRL"),
		Balance:               &balance,
		IdempotentReplay:      replay,
	}
}

func TestSubmitAnswersWhatTheOperationCameTo(t *testing.T) {
	t.Parallel()

	rejected := app.OperationResult{
		TransactionID:         wagering.NewTransactionID(),
		ExternalTransactionID: "acme-tx-3",
		Kind:                  wagering.Bet,
		Status:                wagering.Rejected,
		Money:                 mustMoney(t, "25.00", "BRL"),
		FailureCode:           failure.InsufficientFunds,
	}
	parked := app.OperationResult{
		TransactionID:         wagering.NewTransactionID(),
		ExternalTransactionID: "acme-tx-2",
		Kind:                  wagering.Refund,
		Status:                wagering.PendingReference,
		Money:                 mustMoney(t, "25.00", "BRL"),
	}

	cases := []struct {
		name       string
		result     app.OperationResult
		wantStatus int
		assert     func(t *testing.T, view operationView)
	}{
		{
			name:       "processed",
			result:     processed(t, false),
			wantStatus: http.StatusOK,
			assert: func(t *testing.T, view operationView) {
				t.Helper()
				if view.Status != string(wagering.Processed) {
					t.Errorf("status = %q, want PROCESSED", view.Status)
				}
				if view.IdempotentReplay {
					t.Error("a first submission was reported as a replay")
				}
				if view.Balance == nil || view.Balance.Amount != "75.00" {
					t.Errorf("balance = %+v, want 75.00 BRL", view.Balance)
				}
				if view.FailureCode != "" {
					t.Errorf("failureCode = %q on a processed operation", view.FailureCode)
				}
			},
		},
		{
			name:       "replay",
			result:     processed(t, true),
			wantStatus: http.StatusOK,
			assert: func(t *testing.T, view operationView) {
				t.Helper()
				if !view.IdempotentReplay {
					t.Error("a replay was not reported as one")
				}
				if view.Balance == nil || view.Balance.Amount != "75.00" {
					t.Errorf("a replay answered balance %+v, want the balance it first reported",
						view.Balance)
				}
			},
		},
		{
			name:       "pending reference",
			result:     parked,
			wantStatus: http.StatusAccepted,
			assert: func(t *testing.T, view operationView) {
				t.Helper()
				if view.Status != string(wagering.PendingReference) {
					t.Errorf("status = %q, want PENDING_REFERENCE", view.Status)
				}
				if view.Balance != nil {
					t.Errorf("balance = %+v on an operation that has not moved money", view.Balance)
				}
			},
		},
		{
			name:       "rejected",
			result:     rejected,
			wantStatus: http.StatusUnprocessableEntity,
			assert: func(t *testing.T, view operationView) {
				t.Helper()
				if view.TransactionID == "" {
					t.Error("a rejection named no transaction")
				}
				if view.Status != string(wagering.Rejected) {
					t.Errorf("status = %q, want REJECTED", view.Status)
				}
				if view.FailureCode != string(failure.InsufficientFunds) {
					t.Errorf("failureCode = %q, want INSUFFICIENT_FUNDS", view.FailureCode)
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.auth.principal = providerPrincipal(t, "acme")
			h.wagering.result = c.result

			recorder := h.do(t, submission(submitBody))

			assertStatus(t, recorder, c.wantStatus)
			assertJSON(t, recorder)
			var view operationView
			bodyOf(t, recorder, &view)
			if view.TransactionID != c.result.TransactionID.String() {
				t.Errorf("transactionId = %q, want %q", view.TransactionID, c.result.TransactionID)
			}
			c.assert(t, view)
		})
	}
}

func TestSubmitPassesEveryFieldThroughUntouched(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.result = processed(t, false)

	// None of this is well-formed, and that is the point: the application layer
	// parses a submission exactly once, and a value repaired on the way in
	// would be hashed as something the provider never sent. An amount with
	// three decimals, a lower-case currency and OPENING all have to reach the
	// service to be refused by it.
	body := `{
		"providerId": "acme",
		"externalTransactionId": " acme-TX-1 ",
		"playerId": "Player-1",
		"walletId": "not-a-uuid",
		"roundId": "round-1",
		"gameId": "game-1",
		"kind": "OPENING",
		"money": {"amount": "25.000", "currency": "brl"},
		"referenceExternalTransactionId": "acme-tx-0"
	}`
	h.do(t, submission(body))

	want := app.OperationFields{
		Provider:                       "acme",
		ExternalTransactionID:          " acme-TX-1 ",
		IdempotencyKey:                 "acme-key-1",
		PlayerID:                       "Player-1",
		WalletID:                       "not-a-uuid",
		RoundID:                        "round-1",
		GameID:                         "game-1",
		Kind:                           "OPENING",
		Amount:                         "25.000",
		Currency:                       "brl",
		ReferenceExternalTransactionID: "acme-tx-0",
	}
	if got := h.wagering.submitted().Fields; got != want {
		t.Errorf("the use case was given\n%+v\nwant\n%+v", got, want)
	}
}

func TestSubmitRefusesOpeningThroughTheDomainsCode(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.err = failure.New(failure.UnsupportedTransactionKind,
		"OPENING is raised only when a wallet is opened").WithField("kind")

	recorder := h.do(t, submission(
		`{"providerId":"acme","externalTransactionId":"acme-tx-1","playerId":"p",
		  "walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"r",
		  "gameId":"g","kind":"OPENING","money":{"amount":"25.00","currency":"BRL"}}`))

	assertStatus(t, recorder, http.StatusBadRequest)
	body := errorOf(t, recorder)
	if body.Code != string(failure.UnsupportedTransactionKind) {
		t.Errorf("code = %q, want UNSUPPORTED_TRANSACTION_KIND", body.Code)
	}
	if body.Message != "kind: OPENING is raised only when a wallet is opened" {
		t.Errorf("message = %q, want the refusal's own words behind the field", body.Message)
	}
}

func TestSubmitRequiresAnIdempotencyKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		prepare func(req *request)
	}{
		{"absent", func(req *request) { req.without = []string{idempotencyKeyHeader} }},
		{"present and empty", func(req *request) { req.headers[idempotencyKeyHeader] = "" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.auth.principal = providerPrincipal(t, "acme")

			req := submission(submitBody)
			c.prepare(&req)
			recorder := h.do(t, req)

			assertStatus(t, recorder, http.StatusBadRequest)
			if got := errorOf(t, recorder).Code; got != string(failure.MissingRequiredField) {
				t.Errorf("code = %q, want MISSING_REQUIRED_FIELD", got)
			}
			// The key is what makes a submission idempotent, so a submission
			// without one must not be applied on the way to being refused.
			if h.wagering.called() != 0 {
				t.Errorf("the use case was called %d times for a submission with no key",
					h.wagering.called())
			}
		})
	}
}

func TestSubmitCarriesTheIdempotencyKeyUnmodified(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.result = processed(t, false)

	// Mixed case, punctuation and a body that names a different operation: the
	// key is the provider's, read from the header and never derived from what
	// the payload happens to say.
	const key = "Acme/Key+1=ROUND_1"
	req := submission(submitBody)
	req.headers[idempotencyKeyHeader] = key
	h.do(t, req)

	if got := h.wagering.submitted().Fields.IdempotencyKey; got != key {
		t.Errorf("the use case was given the key %q, want %q", got, key)
	}
}

func TestSubmitRefusesABodyItCannotRead(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, body, want string }{
		{
			name: "unknown field",
			body: `{"providerId":"acme","externalTransactionId":"x","playerId":"p",
			        "walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"r",
			        "gameId":"g","kind":"BET","money":{"amount":"1.00","currency":"BRL"},
			        "idempotencyKey":"smuggled"}`,
			want: "body: the request body names a field this endpoint does not have",
		},
		{
			name: "field named twice",
			body: `{"kind":"BET","kind":"LOSS"}`,
			want: "body: the request body names the same field twice",
		},
		{
			name: "not an object",
			body: `["bet"]`,
			want: "body: the request body is not a JSON object this endpoint can read",
		},
		{
			name: "a second value after the first",
			body: `{"kind":"BET"} {"kind":"LOSS"}`,
			want: "body: the request body is not a JSON object this endpoint can read",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.auth.principal = providerPrincipal(t, "acme")

			recorder := h.do(t, submission(c.body))

			assertStatus(t, recorder, http.StatusBadRequest)
			body := errorOf(t, recorder)
			if body.Code != string(failure.InvalidFieldFormat) {
				t.Errorf("code = %q, want INVALID_FIELD_FORMAT", body.Code)
			}
			if body.Message != c.want {
				t.Errorf("message = %q, want %q", body.Message, c.want)
			}
			if h.wagering.called() != 0 {
				t.Error("a body that could not be read still reached the use case")
			}
		})
	}
}

func TestReadOperationScopesByPrincipalAndTellsAMissFromAForeignOne(t *testing.T) {
	t.Parallel()

	// The two answers the application layer gives: an operation that is not
	// there, and one that belongs to somebody else. They are built to render
	// identically and to stay distinguishable in the chain.
	const message = `no operation "0199c0de-0000-7000-8000-000000000001"`
	miss := classified(app.NotFound, "", message)
	foreign := classified(app.NotFound, "", message, app.ErrForeignOperation)

	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	id := wagering.NewTransactionID()

	h.wagering.err = miss
	absent := h.do(t, request{method: http.MethodGet, path: "/wagering/transactions/" + id.String()})
	assertStatus(t, absent, http.StatusNotFound)

	h.wagering.err = foreign
	rival := h.do(t, request{method: http.MethodGet, path: "/wagering/transactions/" + id.String()})
	assertStatus(t, rival, http.StatusNotFound)

	// Byte for byte but the correlation, which is per request. Anything else
	// would make this route an oracle a provider could walk a competitor's
	// identifiers through.
	absentBody, rivalBody := errorOf(t, absent), errorOf(t, rival)
	absentBody.CorrelationID, rivalBody.CorrelationID = "", ""
	if absentBody != rivalBody {
		t.Errorf("a foreign operation answered\n%+v\nand an absent one\n%+v", rivalBody, absentBody)
	}
	if got := rival.Body.String(); strings.Contains(got, "another provider") {
		t.Errorf("the response published the sentinel: %s", got)
	}
}

func TestReadOperationRefusesAnIdentifierThatIsNotOne(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")

	recorder := h.do(t, request{method: http.MethodGet, path: "/wagering/transactions/not-a-uuid"})

	assertStatus(t, recorder, http.StatusBadRequest)
	if got := errorOf(t, recorder).Code; got != string(failure.InvalidFieldFormat) {
		t.Errorf("code = %q, want INVALID_FIELD_FORMAT", got)
	}
	if h.wagering.called() != 0 {
		t.Error("an identifier that is not one still reached the use case")
	}
}

func TestReadOperationPassesThePrincipalToTheUseCase(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	principal := providerPrincipal(t, "acme")
	h.auth.principal = principal
	h.wagering.result = processed(t, false)
	id := wagering.NewTransactionID()

	h.do(t, request{method: http.MethodGet, path: "/wagering/transactions/" + id.String()})

	if h.wagering.lastByID != id {
		t.Errorf("the use case was asked for %s, want %s", h.wagering.lastByID, id)
	}
	if got, _ := h.wagering.lastPrincipal.Provider(); got != "acme" {
		t.Errorf("the use case was given provider %q, want acme", got)
	}
}

// The provider in the path is the provider the read is made as. Whether this
// caller may read as that provider is app.Principal.MayReadAs's to answer, so
// the request reaches the use case carrying both.
func TestReadByExternalIDPassesThePathsProviderToTheUseCase(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		principal func(*testing.T) app.Principal
		path      string
		want      wagering.Provider
	}{
		{
			name:      "a provider reading as itself",
			principal: func(t *testing.T) app.Principal { return providerPrincipal(t, "acme") },
			path:      "/providers/acme/wagering/transactions/acme-tx-1",
			want:      "acme",
		},
		{
			name:      "the service reading as a provider",
			principal: func(t *testing.T) app.Principal { return servicePrincipal(t) },
			path:      "/providers/acme/wagering/transactions/acme-tx-1",
			want:      "acme",
		},
		{
			name:      "the service reading as another provider",
			principal: func(t *testing.T) app.Principal { return servicePrincipal(t) },
			path:      "/providers/rival/wagering/transactions/rival-tx-1",
			want:      "rival",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.auth.principal = c.principal(t)
			h.wagering.result = processed(t, false)

			recorder := h.do(t, request{method: http.MethodGet, path: c.path})

			assertStatus(t, recorder, http.StatusOK)
			if h.wagering.lastProvider != c.want {
				t.Errorf("the use case was asked as %q, want %q", h.wagering.lastProvider, c.want)
			}
			if h.wagering.lastByExternal == "" {
				t.Error("the use case was given no external id")
			}
		})
	}
}

// A provider naming another provider is refused by the application layer, and
// the refusal is 403 here rather than the 404 a refused read carries elsewhere:
// it is answered from the token and the path alone, before anything is looked
// for, so it is the same answer for every external id.
func TestReadByExternalIDAnswersARefusedProviderWithoutLookingAnythingUp(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.err = classified(app.Unauthorized, "", `provider acme may not read as "rival"`)

	recorder := h.do(t, request{
		method: http.MethodGet,
		path:   "/providers/rival/wagering/transactions/rival-tx-1",
	})

	assertStatus(t, recorder, http.StatusForbidden)
	if got := errorOf(t, recorder).Code; got != string(app.Unauthorized) {
		t.Errorf("code = %q, want UNAUTHORIZED", got)
	}
}

func TestReadByExternalIDRefusesAProviderThatIsNotOne(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")

	recorder := h.do(t, request{
		method: http.MethodGet,
		path:   "/providers/%20acme/wagering/transactions/acme-tx-1",
	})

	assertStatus(t, recorder, http.StatusBadRequest)
	if h.wagering.called() != 0 {
		t.Error("a provider that is not one still reached the use case")
	}
}

// A read answers 200 whatever the operation came to. The status line says the
// read succeeded; the body says what was read.
func TestReadingAnOperationAnswersTwoHundredWhateverItCameTo(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		status wagering.Status
	}{
		{"processed", wagering.Processed},
		{"waiting for its reference", wagering.PendingReference},
		{"rejected", wagering.Rejected},
		{"failed", wagering.Failed},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.auth.principal = providerPrincipal(t, "acme")
			h.wagering.result = app.OperationResult{
				TransactionID:         wagering.NewTransactionID(),
				ExternalTransactionID: "acme-tx-1",
				Kind:                  wagering.Bet,
				Status:                c.status,
				Money:                 mustMoney(t, "25.00", "BRL"),
			}

			byID := h.do(t, request{
				method: http.MethodGet,
				path:   "/wagering/transactions/" + h.wagering.result.TransactionID.String(),
			})
			byExternal := h.do(t, request{
				method: http.MethodGet,
				path:   "/providers/acme/wagering/transactions/acme-tx-1",
			})

			assertStatus(t, byID, http.StatusOK)
			assertStatus(t, byExternal, http.StatusOK)
			var view operationView
			bodyOf(t, byID, &view)
			if view.Status != string(c.status) {
				t.Errorf("status = %q, want %q in the body", view.Status, c.status)
			}
		})
	}
}
