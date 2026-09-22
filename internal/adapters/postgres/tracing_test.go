// The statement tracer's one rule, which needs no database and so carries no
// build tag.

package postgres

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// TestAStatementSpanNeverCarriesTheArguments is the rule [queryTracer] exists
// to make unbreakable.
//
// pgx hands the tracer the arguments, and they are the amounts, the balances,
// the player identifiers and the idempotency keys of every operation this
// service performs. A trace is read by more people than the database is, so a
// span carrying them is a financial payload published — which is why this
// tracer is written by hand rather than taken from a library: the available
// ones log arguments, some by default, and the setting that turns it off is one
// deployment away from being turned on while somebody is debugging.
//
// The statement TEXT is recorded and that is safe for the opposite reason:
// every statement in this package is a package-level constant with numbered
// placeholders, so the text carries nothing anybody submitted. A tree that
// started composing SQL from input would have to revisit this.
func TestAStatementSpanNeverCarriesTheArguments(t *testing.T) {
	t.Parallel()

	spans := tracetest.NewSpanRecorder()
	reporting, err := telemetry.New(telemetry.Config{
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
	})
	if err != nil {
		t.Fatalf("build the telemetry: %v", err)
	}
	tracer := queryTracer{telemetry: reporting}

	// Everything a movement's arguments actually are: an amount in minor units,
	// a player, a provider's key, and a wallet.
	secrets := []any{
		int64(2500), "player-9f2c", "idempotency-key-9f2c",
		"0199c0de-0000-7000-8000-000000000002",
	}
	ctx := tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{
		SQL:  moveWallet,
		Args: secrets,
	})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	ended := spans.Ended()
	if len(ended) != 1 {
		t.Fatalf("%d spans finished, wanted one", len(ended))
	}
	rendered := renderSpan(ended[0])
	for _, secret := range []string{"2500", "player-9f2c", "idempotency-key-9f2c",
		"0199c0de-0000-7000-8000-000000000002"} {
		if strings.Contains(rendered, secret) {
			t.Errorf("a statement span carried the argument %q:\n%s", secret, rendered)
		}
	}
	// And the statement itself is there, because a span that said only "a query
	// ran" would be a span nobody could act on.
	if !strings.Contains(rendered, "UPDATE wagering.wallet") {
		t.Errorf("the statement span does not name the statement:\n%s", rendered)
	}
}

// renderSpan is everything one span says, for the assertions about what must
// never appear in it.
func renderSpan(span sdktrace.ReadOnlySpan) string {
	parts := []string{span.Name(), span.Status().Description}
	for _, attr := range span.Attributes() {
		parts = append(parts, string(attr.Key)+"="+attr.Value.String())
	}
	for _, event := range span.Events() {
		parts = append(parts, event.Name)
		for _, attr := range event.Attributes {
			parts = append(parts, string(attr.Key)+"="+attr.Value.String())
		}
	}
	return strings.Join(parts, "\n")
}
