//go:build integration

// What one provider can see, replay and do to another provider's work. The
// answer, everywhere below, is nothing.
package integration

import (
	"net/http"
	"testing"
)

// TestTwoProvidersMayUseTheSameExternalTransactionID.
//
// An external transaction id names an operation only within the provider that
// issued it, so provider-b submitting the id provider-a used is a different
// operation and not a duplicate of it. The two must come out with different
// transaction ids, both must apply, and neither provider may reach the other's
// through the id they share.
func TestTwoProvidersMayUseTheSameExternalTransactionID(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	wallet := s.openWallet(t, "player-shared", "100.00")

	const shared = "ext-shared-1"
	first := operationOf(t, s.submit(t, providerA,
		bet(providerA, shared, wallet, "10.00"), "key-a-1"))
	second := operationOf(t, s.submit(t, providerB,
		bet(providerB, shared, wallet, "20.00"), "key-b-1"))

	if first.Status != "PROCESSED" || second.Status != "PROCESSED" {
		t.Fatalf("the two submissions came to %s and %s, wanted both PROCESSED",
			first.Status, second.Status)
	}
	if first.TransactionID == second.TransactionID {
		t.Fatalf("both providers were given transaction %s: one external id became one operation",
			first.TransactionID)
	}
	if first.ExternalTransactionID != shared || second.ExternalTransactionID != shared {
		t.Fatalf("the operations report %q and %q, wanted %q from both",
			first.ExternalTransactionID, second.ExternalTransactionID, shared)
	}
	if second.IdempotentReplay {
		t.Fatal("provider-b's submission was answered as a replay of provider-a's")
	}
	// Both applied, in order, to the one wallet the player holds: 100 less 10
	// less 20. A submission that had been folded into the other's identity
	// would have moved the balance once.
	if first.Balance == nil || first.Balance.Amount != "90.00" {
		t.Fatalf("provider-a's bet left %v, wanted 90.00", first.Balance)
	}
	if second.Balance == nil || second.Balance.Amount != "70.00" {
		t.Fatalf("provider-b's bet left %v, wanted 70.00", second.Balance)
	}

	// Each reaches its own by the id they share, and gets its own.
	mine := operationOf(t, s.do(t, call{
		method: http.MethodGet,
		path:   "/providers/" + providerA + "/wagering/transactions/" + shared,
		token:  tokenFor(t, providerA),
	}))
	if mine.TransactionID != first.TransactionID {
		t.Fatalf("provider-a reading %q got %s, wanted its own %s",
			shared, mine.TransactionID, first.TransactionID)
	}
	theirs := operationOf(t, s.do(t, call{
		method: http.MethodGet,
		path:   "/providers/" + providerB + "/wagering/transactions/" + shared,
		token:  tokenFor(t, providerB),
	}))
	if theirs.TransactionID != second.TransactionID {
		t.Fatalf("provider-b reading %q got %s, wanted its own %s",
			shared, theirs.TransactionID, second.TransactionID)
	}

	// And neither reaches the other's by this system's identifier for it.
	refused(t, s.do(t, call{
		method: http.MethodGet,
		path:   "/wagering/transactions/" + first.TransactionID,
		token:  tokenFor(t, providerB),
	}), http.StatusNotFound, "NOT_FOUND")
	refused(t, s.do(t, call{
		method: http.MethodGet,
		path:   "/wagering/transactions/" + second.TransactionID,
		token:  tokenFor(t, providerA),
	}), http.StatusNotFound, "NOT_FOUND")
}

// TestAProviderMayNotSubmitAsAnother.
//
// The provider is carried in the body rather than filled in from the token, so
// that this check is a live one rather than one the transport has already
// satisfied. A provider naming somebody else is 403 — a principal was
// established and may not do this — and nothing is recorded.
func TestAProviderMayNotSubmitAsAnother(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	wallet := s.openWallet(t, "player-impersonated", "100.00")

	before := s.snapshot(t)
	got := s.submit(t, providerB,
		bet(providerA, "ext-impersonated", wallet, "10.00"), "key-impersonated")
	body := refused(t, got, http.StatusForbidden, "UNAUTHORIZED")
	if body.Message != `provider provider-b may not submit as "provider-a"` {
		t.Fatalf("the refusal says %q", body.Message)
	}
	unchanged(t, before, s.snapshot(t), "a submission naming another provider")

	// The control: the same body from the provider it names is accepted, so the
	// refusal above is about who sent it and not about what it said.
	accepted := s.submit(t, providerA,
		bet(providerA, "ext-impersonated", wallet, "10.00"), "key-impersonated")
	if accepted.status != http.StatusOK {
		t.Fatalf("provider-a's own submission was answered %s", accepted)
	}
}

// TestAProviderMayNotReadAsAnother.
//
// Answered from the token and the path alone, before anything is looked for, so
// it is the same answer for every external id and confirms the existence of
// none of them. 403 here rather than the 404 a refused read carries elsewhere,
// because this route never went to find out whether the operation exists.
func TestAProviderMayNotReadAsAnother(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	wallet := s.openWallet(t, "player-read-as", "100.00")
	mine := operationOf(t, s.submit(t, providerA,
		bet(providerA, "ext-read-as", wallet, "10.00"), "key-read-as"))

	for _, external := range []string{"ext-read-as", "an-id-that-exists-nowhere"} {
		got := s.do(t, call{
			method: http.MethodGet,
			path:   "/providers/" + providerA + "/wagering/transactions/" + external,
			token:  tokenFor(t, providerB),
		})
		body := refused(t, got, http.StatusForbidden, "UNAUTHORIZED")
		if body.Message != `provider provider-b may not read as "provider-a"` {
			t.Fatalf("reading %q as provider-a says %q", external, body.Message)
		}
	}

	// The control: provider-a reaches the same operation on the same route.
	if got := operationOf(t, s.do(t, call{
		method: http.MethodGet,
		path:   "/providers/" + providerA + "/wagering/transactions/ext-read-as",
		token:  tokenFor(t, providerA),
	})); got.TransactionID != mine.TransactionID {
		t.Fatalf("provider-a read %s, wanted %s", got.TransactionID, mine.TransactionID)
	}
}

// TestAForeignOperationIsAnsweredExactlyAsAnAbsentOneIs is the existence-oracle
// test, and it is the one scenario here that needs two databases.
//
// The claim is byte-identity, and the only variable in the answer is the
// identifier the message names — so the two questions have to be the same
// question. They are asked with one transaction id, once of a service where it
// exists and belongs to provider-a, and once of a service where it does not
// exist at all. Same correlation, same route, same caller; the only difference
// in the world is whether the row is there.
//
// If the two answers differed, a provider could walk a competitor's identifiers
// and learn which of them landed, which is exactly what scoping a read is
// meant to deny.
func TestAForeignOperationIsAnsweredExactlyAsAnAbsentOneIs(t *testing.T) {
	t.Parallel()
	holds := newStack(t)
	empty := newStack(t)

	wallet := holds.openWallet(t, "player-oracle", "100.00")
	mine := operationOf(t, holds.submit(t, providerA,
		bet(providerA, "ext-oracle", wallet, "10.00"), "key-oracle"))

	// The same correlation on both, because it is the one member of the body
	// the caller controls and it would otherwise be the only difference.
	const correlation = "01a0c512-b9b1-7ec4-b8aa-c4852b7e7e37"
	ask := call{
		method:      http.MethodGet,
		path:        "/wagering/transactions/" + mine.TransactionID,
		token:       tokenFor(t, providerB),
		correlation: correlation,
	}
	foreign := holds.do(t, ask)
	absent := empty.do(t, ask)

	if foreign.status != http.StatusNotFound {
		t.Fatalf("reading another provider's operation answered %s, wanted 404", foreign)
	}
	sameAnswer(t, foreign, absent,
		"a foreign operation and an absent one are not answered alike")

	// The control, and the whole force of the test: the operation really is
	// there on the first service, so the 404 above was scoping and not absence.
	if got := operationOf(t, holds.do(t, call{
		method:      http.MethodGet,
		path:        "/wagering/transactions/" + mine.TransactionID,
		token:       tokenFor(t, providerA),
		correlation: correlation,
	})); got.TransactionID != mine.TransactionID {
		t.Fatalf("the owner read %s, wanted %s", got.TransactionID, mine.TransactionID)
	}
}

// TestAProvidersIdempotencyKeyIsItsOwn.
//
// Idempotency is scoped to the provider that submitted under it, in the
// database, by wager_transaction_provider_idempotency_key. So provider-b
// arriving with provider-a's key and provider-a's submission gets an operation
// of its own — not provider-a's result, not a replay, and with nothing changed
// about provider-a's.
func TestAProvidersIdempotencyKeyIsItsOwn(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	wallet := s.openWallet(t, "player-replay", "100.00")

	const key = "key-replay-shared"
	const external = "ext-replay-shared"
	const correlation = "01a0c512-b9b1-7a7e-a26a-52633f063ab0"

	body := bet(providerA, external, wallet, "10.00")
	first := operationOf(t, s.submit(t, providerA, body, key))
	if first.IdempotentReplay {
		t.Fatal("the first submission was answered as a replay")
	}

	// The control that replay works at all. Without it, a service that had
	// simply stopped recognising retries would pass the assertion below for the
	// wrong reason.
	again := operationOf(t, s.submit(t, providerA, body, key))
	if !again.IdempotentReplay || again.TransactionID != first.TransactionID {
		t.Fatalf("provider-a's own retry came back as %+v, wanted a replay of %s",
			again, first.TransactionID)
	}

	read := call{
		method:      http.MethodGet,
		path:        "/wagering/transactions/" + first.TransactionID,
		token:       tokenFor(t, providerA),
		correlation: correlation,
	}
	before := s.do(t, read)

	// provider-b, provider-a's key, provider-a's payload in every respect but
	// the provider — which it may not name, so it names itself.
	theirs := bet(providerB, external, wallet, "10.00")
	got := operationOf(t, s.submit(t, providerB, theirs, key))
	if got.IdempotentReplay {
		t.Fatal("provider-b's submission was answered as a replay of provider-a's")
	}
	if got.TransactionID == first.TransactionID {
		t.Fatalf("provider-b was handed provider-a's operation %s", first.TransactionID)
	}
	if got.Balance == nil || got.Balance.Amount != "80.00" {
		t.Fatalf("provider-b's bet left %v, wanted 80.00 — it was applied, not replayed",
			got.Balance)
	}

	// provider-a's operation is exactly what it was, down to the bytes.
	sameAnswer(t, before, s.do(t, read),
		"provider-b's submission changed provider-a's operation")

	// And provider-b naming provider-a under that key is refused outright,
	// rather than resolving to provider-a's record.
	refused(t, s.submit(t, providerB, body, key), http.StatusForbidden, "UNAUTHORIZED")
	sameAnswer(t, before, s.do(t, read),
		"a refused submission naming provider-a changed provider-a's operation")
}
