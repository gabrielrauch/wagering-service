package httpapi

import (
	"context"
	"net/http"
	"strings"
	"unicode/utf8"
	"uuid"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// correlationHeader is where a caller supplies the thread tying everything done
// for one request together, and where this service echoes the one it used.
const correlationHeader = "X-Correlation-Id"

// What a correlation may be.
//
// The bounds are wagering.opaque_id's, which is the column every correlation is
// eventually stored in: a value the database would refuse must be refused
// before a transaction has begun, or a wager transaction that was otherwise
// perfectly good fails at the last statement for a reason no provider can see.
//
// The control-character bound does a second job. The value is echoed in a
// response header and written to every log line the request produces, and a
// caller that could put a newline in it would be writing those log lines.
const (
	maxCorrelationBytes = 128
	asciiSpace          = 0x20
	asciiDelete         = 0x7f
)

// correlationKey is the context key the correlation travels under. It is an
// unexported type so that nothing outside this package can set or shadow it.
type correlationKey struct{}

// withCorrelation puts the request's correlation in the context.
func withCorrelation(ctx context.Context, correlation string) context.Context {
	return context.WithValue(ctx, correlationKey{}, correlation)
}

// correlationFrom reads the correlation established for this request.
//
// It answers "" only for a context this package did not build, which is a
// defect rather than a condition: every response written here goes through
// ServeHTTP, and ServeHTTP sets it before anything else runs.
func correlationFrom(ctx context.Context) string {
	correlation, _ := ctx.Value(correlationKey{}).(string)
	return correlation
}

// correlationOf reads the correlation a request carries, or mints one.
//
// A supplied value this service will not use is handled one of two ways, and
// which one depends on what is wrong with it.
//
// A value carrying a control character, a value that is not UTF-8 and a value
// with whitespace around it are REFUSED. The first of those is the reason: this
// value is written into every log line the request produces and echoed in a
// response header, so a caller that could put a newline in it would be writing
// those log lines. Quietly replacing it would repair a log-injection attempt
// and report nothing, which is precisely the case somebody needs to hear about.
// The other two go with it because all three describe a value the caller wrote
// down wrongly and can correct.
//
// A value that is merely too long is REPLACED, and the request goes on. The
// hundred-and-twenty-eight-byte bound is wagering.opaque_id's — the column the
// correlation is eventually stored in — and a caller has no way to know that
// number. Refusing a well-formed identifier against a limit this service never
// published would fail a good request over a storage fact; the replacement is
// logged and the minted value is echoed, so a caller comparing what it sent
// with what came back can see that its thread was not kept.
//
// The correlation returned alongside a refusal is a fresh one, so the refusal
// itself is still traceable.
func correlationOf(r *http.Request) (correlation string, replaced bool, err error) {
	supplied := r.Header.Get(correlationHeader)
	if supplied == "" {
		return newCorrelation(), false, nil
	}
	if len(supplied) > maxCorrelationBytes {
		return newCorrelation(), true, nil
	}
	if err := checkCorrelation(supplied); err != nil {
		return newCorrelation(), false, err
	}
	return supplied, false, nil
}

// newCorrelation mints a correlation for a request that carries none.
//
// It is a package-level call rather than a port, unlike every identifier the
// application layer mints. Nothing compares a correlation, stores it under a
// unique index or derives anything from it — it is carried — so there is
// nothing a test would need to fix it for.
func newCorrelation() string { return uuid.NewV7().String() }

// checkCorrelation refuses a supplied correlation this service must not repeat.
//
// Length is not among the tests, deliberately: it is handled by [correlationOf]
// as a replacement rather than a refusal, for the reason set out there.
func checkCorrelation(correlation string) error {
	refuse := func(why string) error {
		return failure.New(failure.InvalidFieldFormat, "%s", why).WithField(correlationHeader)
	}
	switch {
	case !utf8.ValidString(correlation):
		return refuse("must be valid UTF-8")
	case strings.TrimSpace(correlation) != correlation:
		return refuse("must not be surrounded by whitespace")
	}
	for _, r := range correlation {
		if r < asciiSpace || r == asciiDelete {
			return refuse("must not contain control characters")
		}
	}
	return nil
}
