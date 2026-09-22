package telemetry

import (
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TestAFailedSpanCarriesACodeAndNeverAMessage pins the rule this package exists
// to make unbreakable.
//
// app.ErrForeignOperation must never be rendered to anybody. A span is read by
// whoever can read the trace, so a span that carried an error's message would
// be exactly the rendering the sentinel exists to prevent — and it would be a
// rendering nobody reviews, because it never appears in a response or in a test
// of one.
//
// The door takes a code, so there is no call site that COULD render one. This
// test holds the other half: that the span ends up with no event, no
// exception, and nothing that came from an error's own words.
func TestAFailedSpanCarriesACodeAndNeverAMessage(t *testing.T) {
	t.Parallel()
	r := record(t)
	const secret = "no operation 0199c0de-0000-7000-8000-000000000001 for provider acme"

	_, span := r.Start(t.Context(), "read an operation")
	r.Failed(span, "NOT_FOUND", Correlation("thread-1"))
	span.End()

	ended := r.only(t)
	if ended.Status().Code != codes.Error {
		t.Errorf("the span is %v, wanted an error", ended.Status().Code)
	}
	if ended.Status().Description != "NOT_FOUND" {
		t.Errorf("the span says %q, wanted the code", ended.Status().Description)
	}
	if len(ended.Events()) != 0 {
		t.Errorf("the span carries %d events; a recorded error is how a message gets in",
			len(ended.Events()))
	}
	if !hasAttribute(ended, KeyCode, "NOT_FOUND") {
		t.Errorf("the span carries no %s attribute: %v", KeyCode, ended.Attributes())
	}
	if rendered := render(ended); strings.Contains(rendered, secret) {
		t.Errorf("a span carried a refusal's own words:\n%s", rendered)
	}
}

// TestAnEmptyIdentifierIsNotAnAttribute pins what [Some] is for.
//
// Most of these identifiers are unknown when a span opens — a body that will
// not parse names no transaction — and an attribute carrying "" is worse than
// an absent one: it is a value somebody can search for, and it returns every
// failure in the system.
func TestAnEmptyIdentifierIsNotAnAttribute(t *testing.T) {
	t.Parallel()
	r := record(t)

	_, span := r.Start(t.Context(), "consume", trace.WithAttributes(Some(
		Correlation("thread-1"),
		TransactionID(""),
		WalletID(""),
	)...))
	span.End()

	ended := r.only(t)
	for _, attr := range ended.Attributes() {
		if attr.Value.String() == "" {
			t.Errorf("the span carries %s with no value", attr.Key)
		}
	}
	if !hasAttribute(ended, KeyCorrelation, "thread-1") {
		t.Errorf("the correlation was dropped along with the empty ones: %v", ended.Attributes())
	}
}

// TestATraceSurvivesABoundaryAndAMessageNobodyTracedStartsOne pins the
// carriage the whole cross-process trace rests on.
//
// The first half is the outbox and the queue: something injected here has to
// come back out there as the SAME trace, or an operation and its published
// event are two traces correlated by an attribute — which is precisely the
// answer this design rejects.
//
// The second half is a message somebody put on the queue by hand. It names no
// trace, so the work starts one here rather than failing or joining nothing.
func TestATraceSurvivesABoundaryAndAMessageNobodyTracedStartsOne(t *testing.T) {
	t.Parallel()

	t.Run("a trace that was carried", func(t *testing.T) {
		t.Parallel()
		r := record(t)

		ctx, origin := r.Start(t.Context(), "submit an operation")
		carried := r.Inject(ctx)
		origin.End()

		if carried["traceparent"] == "" {
			t.Fatalf("nothing was carried across the boundary: %v", carried)
		}
		_, continued := r.Start(r.Extract(t.Context(), carried), "publish the event")
		continued.End()

		spans := r.ended(t)
		if len(spans) != 2 {
			t.Fatalf("%d spans finished, wanted two", len(spans))
		}
		if spans[0].SpanContext().TraceID() != spans[1].SpanContext().TraceID() {
			t.Fatalf("the published event is in trace %s and the operation in %s; "+
				"they must be one trace", spans[1].SpanContext().TraceID(),
				spans[0].SpanContext().TraceID())
		}
		if spans[1].Parent().SpanID() != spans[0].SpanContext().SpanID() {
			t.Errorf("the published event's parent is %s, wanted the operation's span %s",
				spans[1].Parent().SpanID(), spans[0].SpanContext().SpanID())
		}
	})

	t.Run("a message nobody traced", func(t *testing.T) {
		t.Parallel()
		r := record(t)

		for _, carried := range []map[string]string{nil, {}, {"traceparent": "nonsense"}} {
			_, span := r.Start(r.Extract(t.Context(), carried), "consume")
			span.End()
		}
		for i, span := range r.ended(t) {
			if span.Parent().IsValid() {
				t.Errorf("span %d claims a parent %s it was never given",
					i, span.Parent().SpanID())
			}
			if !span.SpanContext().IsValid() {
				t.Errorf("span %d started no trace of its own", i)
			}
		}
	})
}

// TestALinkedSpanIsAChildOfOneTraceAndReachableFromTheOther pins the publisher's
// shape.
//
// A publish batch sends up to ten events belonging to up to ten different
// traces. Each event's span is a CHILD of the trace its operation was submitted
// under — that is the requirement — and the batch is a LINK, because it cannot
// be the parent of all ten and choosing one would be arbitrary.
func TestALinkedSpanIsAChildOfOneTraceAndReachableFromTheOther(t *testing.T) {
	t.Parallel()
	r := record(t)

	// An operation, somewhere else, minutes ago.
	operationCtx, operation := r.Start(t.Context(), "submit an operation")
	carried := r.Inject(operationCtx)
	operation.End()

	// A publisher turn, here, now.
	batchCtx, batch := r.Start(t.Context(), SpanPublishBatch)
	_, event := r.Start(r.Extract(batchCtx, carried), SpanPublishEvent, Link(batchCtx))
	event.End()
	batch.End()

	spans := r.ended(t)
	if len(spans) != 3 {
		t.Fatalf("%d spans finished, wanted three", len(spans))
	}
	submitted, published, turn := spans[0], spans[1], spans[2]

	if published.SpanContext().TraceID() != submitted.SpanContext().TraceID() {
		t.Fatalf("the published event is not in the operation's trace")
	}
	if published.SpanContext().TraceID() == turn.SpanContext().TraceID() {
		t.Fatalf("the published event took the batch's trace, so the operation's was lost")
	}
	if len(published.Links()) != 1 {
		t.Fatalf("the published event carries %d links, wanted the batch",
			len(published.Links()))
	}
	if published.Links()[0].SpanContext.SpanID() != turn.SpanContext().SpanID() {
		t.Errorf("the link points at %s, wanted the batch turn %s",
			published.Links()[0].SpanContext.SpanID(), turn.SpanContext().SpanID())
	}
}

// hasAttribute reports whether a span carries one string attribute.
func hasAttribute(span sdktrace.ReadOnlySpan, key, value string) bool {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key && attr.Value.String() == value {
			return true
		}
	}
	return false
}

// render is everything a span says, for a failure message.
func render(span sdktrace.ReadOnlySpan) string {
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
