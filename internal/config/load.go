package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// The defaults. Every one of them is a value this service runs correctly on
// against the local stack in deploy/, so a process told only the four required
// variables starts and works.
//
// The queue bounds are the ones deploy/localstack/01-queues.sh provisions —
// thirty seconds of visibility, a redrive policy of five — because a consumer
// told a different number says the wrong thing about which delivery is the last
// one, and a drain longer than the visibility timeout hands messages back with
// receipt handles that have already expired.
const (
	defaultServiceName        = "wagering"
	defaultLogLevel           = slog.LevelInfo
	defaultLogFormat          = LogJSON
	defaultTelemetryShutdown  = 5 * time.Second
	defaultExportTimeout      = 10 * time.Second
	defaultMetricInterval     = 30 * time.Second
	defaultStartTimeout       = 30 * time.Second
	defaultStopTimeout        = 60 * time.Second
	defaultMaxConns           = 10
	defaultConnectTimeout     = 5 * time.Second
	defaultLockTimeout        = 3 * time.Second
	defaultStatementTimeout   = 15 * time.Second
	defaultDatabaseHealth     = 2 * time.Second
	defaultInboundQueue       = "wager-transactions.fifo"
	defaultOutboundQueue      = "wallet-events.fifo"
	defaultClientTimeout      = 10 * time.Second
	defaultResolveTimeout     = 5 * time.Second
	defaultMaxMessages        = 10
	defaultWaitTime           = 20 * time.Second
	defaultVisibilityTimeout  = 30 * time.Second
	defaultQueueHealth        = 2 * time.Second
	defaultMaxReceiveCount    = 5
	defaultClockSkew          = 30 * time.Second
	defaultFetchTimeout       = 5 * time.Second
	defaultMinRefreshInterval = time.Minute
	defaultOIDCHTTPTimeout    = 5 * time.Second
	defaultAddr               = ":8080"
	defaultReadTimeout        = 10 * time.Second
	defaultReadHeaderTimeout  = 5 * time.Second
	defaultWriteTimeout       = 15 * time.Second
	defaultIdleTimeout        = 60 * time.Second
	defaultHTTPShutdown       = 20 * time.Second
	defaultMaxHeaderBytes     = 1 << 20
	defaultMaxBodyBytes       = 1 << 20
	defaultReadinessTimeout   = 3 * time.Second
	defaultReferenceAttempts  = 20
	defaultReferenceTTL       = 5 * time.Minute
	defaultConsumerName       = "wager-consumer"
	defaultConcurrency        = 4
	defaultDrainTimeout       = 20 * time.Second
	defaultClaimBatch         = 10
	defaultClaimHold          = 30 * time.Second
	defaultPollInterval       = time.Second
	defaultReferenceName      = "reference-worker"
	defaultResumeInterval     = time.Second
)

// The default schedules. One per loop and one for the application layer,
// because they are answering different questions: how soon a message is looked
// at again, how soon an event is sent again, how soon a loop that found nothing
// asks again, and how long a parked operation waits for the operation it refers
// to.
var (
	defaultWageringBackoff  = Backoff{Initial: time.Second, Factor: 2, Max: time.Minute}
	defaultConsumerBackoff  = Backoff{Initial: 2 * time.Second, Factor: 2, Max: time.Minute}
	defaultPublisherBackoff = Backoff{Initial: 2 * time.Second, Factor: 2, Max: 5 * time.Minute}
	defaultReferenceBackoff = Backoff{Initial: 2 * time.Second, Factor: 2, Max: time.Minute}
)

// Lookup is where a configuration value comes from.
//
// A function rather than a direct call to os.Getenv, so that the whole of this
// package is exercised without touching the process's environment: a test that
// set a variable would be a test nothing else could run beside, and the
// composition root's own suite needs to build several configurations at once.
type Lookup func(key string) (value string, present bool)

// Environment reads the process's own environment.
func Environment(key string) (string, bool) { return os.LookupEnv(key) }

// Static reads from a map. It is what a test and a compose-file parser both
// want, and it is the only other source this package ships.
func Static(values map[string]string) Lookup {
	return func(key string) (string, bool) {
		value, present := values[key]
		return value, present
	}
}

// Load reads and checks the whole configuration.
//
// Every problem is reported, not the first: an operator with three variables
// wrong should learn that in one restart. The result is the joined set, and
// errors.Is still finds each one.
func Load(lookup Lookup) (Config, error) {
	if lookup == nil {
		return Config{}, errors.New("config: reading the configuration needs a source")
	}
	r := &reader{lookup: lookup}
	cfg := Config{
		Telemetry: r.telemetry(),
		Lifecycle: r.lifecycle(),
		Postgres:  r.postgres(),
		SQS:       r.sqs(),
		OIDC:      r.oidc(),
		HTTP:      r.http(),
		Wagering:  r.wagering(),
		Consumer:  r.consumer(),
		Publisher: r.publisher(),
		Reference: r.reference(),
	}
	if len(r.errs) > 0 {
		return Config{}, errors.Join(r.errs...)
	}
	return cfg, nil
}

// reader turns the environment into values, collecting what it could not read.
type reader struct {
	lookup Lookup
	errs   []error
}

// refuse records a problem against one variable. The variable is always named
// and the value never is, except where an accessor passes it deliberately —
// see [reader.secret] for the one that must not.
func (r *reader) refuse(key, problem string, args ...any) {
	r.errs = append(r.errs, fmt.Errorf("config: %s: %s", key, fmt.Sprintf(problem, args...)))
}

// value reads one variable, reporting whether it holds anything.
//
// A variable that is set and empty counts as absent, and the surrounding
// whitespace is trimmed. Both are about how these values are actually written:
// `FOO=${BAR}` in a compose file with no BAR is an empty FOO rather than an
// unset one, so the other reading would let a single undefined variable defeat
// every default in this package; and a .env file carries trailing spaces
// nobody typed on purpose. None of the values here is one where an edge space
// means anything — the identifiers that would refuse one are refused by the
// domain, not spelt with it.
func (r *reader) value(key string) (string, bool) {
	raw, present := r.lookup(key)
	if !present {
		return "", false
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	return raw, true
}

// text reads a string, falling back when it is absent.
func (r *reader) text(key, fallback string) string {
	if value, ok := r.value(key); ok {
		return value
	}
	return fallback
}

// optional reads a string that has no default and need not be there.
func (r *reader) optional(key string) string {
	value, _ := r.value(key)
	return value
}

// required reads a string that must be there.
func (r *reader) required(key string) string {
	value, ok := r.value(key)
	if !ok {
		r.refuse(key, "is required and was not set")
	}
	return value
}

// secret reads a required value that must never be rendered.
//
// It is [reader.required], and today the two bodies are identical — because
// nothing in this package renders a required value in the first place. This is
// therefore a MARKER and not a mechanism, and it is worth being plain about
// that rather than leaving somebody to discover it: it makes every secret in
// this configuration greppable, and it is the place a check belongs if one is
// ever needed. What actually keeps the password out of a message is that
// [reader.refuse] is never handed a value on this path and that
// [Postgres.Redacted] is the only rendering of a DSN anywhere — both of which
// TestARefusalNeverQuotesTheDSN and TestRedactedKeepsThePasswordOut pin.
func (r *reader) secret(key string) string { return r.required(key) }

// duration reads a positive duration in Go's own notation.
//
// Positive rather than non-negative because every duration in this
// configuration bounds something, and a bound of zero is either no bound at all
// or a library's default — two different meanings for one spelling, decided by
// whichever constructor happens to read it.
func (r *reader) duration(key string, fallback time.Duration) time.Duration {
	raw, ok := r.value(key)
	if !ok {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		r.refuse(key, "%q is not a duration (try 5s, 250ms, 2m)", raw)
		return fallback
	}
	if parsed <= 0 {
		r.refuse(key, "must be positive, got %s", parsed)
		return fallback
	}
	return parsed
}

// The ceilings, one per kind, because the two have different reasons.
const (
	// maxCount bounds a pool size, a batch and a concurrency. It is
	// math.MaxInt32 because each of them is narrowed to an int32 — pgx's pool
	// limit — or to an int that must not overflow a 32-bit build, and bounding
	// them in one place makes every one of those narrowings provably lossless
	// rather than checked at each conversion.
	maxCount = math.MaxInt32
	// maxSize bounds a request limit in bytes. Two gigabytes, and not because
	// anything is narrowed: net/http takes MaxHeaderBytes as an int, so this is
	// what keeps a 32-bit build honest, and a body limit larger than this is
	// not a limit — the process would be out of memory long before a request
	// reached it.
	maxSize = math.MaxInt32
)

// count reads a positive count.
func (r *reader) count(key string, fallback int) int {
	parsed, ok := r.number(key)
	if !ok {
		return fallback
	}
	return int(parsed)
}

// size reads a positive size in bytes.
func (r *reader) size(key string, fallback int64) int64 {
	parsed, ok := r.bounded(key, maxSize)
	if !ok {
		return fallback
	}
	return parsed
}

// number reads a positive count within [maxCount].
func (r *reader) number(key string) (int64, bool) { return r.bounded(key, maxCount) }

// bounded reads a positive integer no larger than ceiling, reporting whether it
// read one. A value it refused reports false, so the caller keeps its default
// and the joined error is what stops the process.
func (r *reader) bounded(key string, ceiling int64) (int64, bool) {
	raw, ok := r.value(key)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		r.refuse(key, "%q is not a whole number", raw)
		return 0, false
	}
	if parsed < 1 || parsed > ceiling {
		r.refuse(key, "must be between 1 and %d, got %d", ceiling, parsed)
		return 0, false
	}
	return parsed, true
}

// conns reads the pool limit, which pgx takes as an int32.
func (r *reader) conns(key string, fallback int32) int32 {
	parsed, ok := r.number(key)
	if !ok {
		return fallback
	}
	// Lossless: number has already refused anything above maxCount.
	return int32(parsed)
}

// factor reads a backoff multiplier, refusing anything that is not a number.
//
// This is the one rule here that duplicates a check downstream, and it is
// deliberate. strconv.ParseFloat("NaN", 64) succeeds, so NaN is a value the
// environment can hand over without anybody typing anything odd, and NaN is not
// less than anything, so "the factor must be at least one" does not catch it —
// which is how it reached app.BackoffPolicy, where Pow(x, 0) is 1 for any x, so
// the first attempt was scheduled normally and every one after it for now, for
// ever.
//
// Both workers.Backoff and app.BackoffPolicy refuse it now, at the point of
// use, where the invariant belongs. It is refused here too because the two
// refusals say different things: a constructor can report that a worker's
// backoff factor is not a number, and only this can report which variable
// carried it, before anything has been built from it.
//
// How small a factor may be is left to those two, which both refuse anything
// below 1 with a sentence naming the loop.
func (r *reader) factor(key string, fallback float64) float64 {
	raw, ok := r.value(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		r.refuse(key, "%q is not a number", raw)
		return fallback
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		r.refuse(key, "%q is not a finite number, and a backoff multiplied by it "+
			"would never wait", raw)
		return fallback
	}
	return parsed
}

// flag reads a switch, in strconv.ParseBool's spelling.
func (r *reader) flag(key string, fallback bool) bool {
	raw, ok := r.value(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		r.refuse(key, "%q is not true or false", raw)
		return fallback
	}
	return parsed
}

// list reads a comma-separated list, refusing an empty element rather than
// dropping it: a trailing comma is a typo, and a list quietly one shorter than
// it looks is the kind of allow-list that is wrong for a year.
func (r *reader) list(key string) []string {
	raw, ok := r.value(key)
	if !ok {
		return nil
	}
	parts := strings.Split(raw, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			r.refuse(key, "%q has an empty entry", raw)
			return nil
		}
		items = append(items, item)
	}
	return items
}

// endpoint reads an optional URL naming something this process will talk to.
//
// It is checked here rather than left to the exporter, and the reason is what
// the exporter does with a bad one: an OTLP exporter is built lazily and
// connects in the background, so a typo in the scheme is a process that starts,
// reports itself healthy and exports nothing, for ever, with one line about it
// somewhere in the start-up log. Refused here it is a start-up failure with the
// variable named, which is what every other wrong value in this package gets.
//
// The scheme is what decides whether the export is encrypted — that is the
// OTLP specification's rule, not this package's — so a scheme that is neither
// http nor https is refused rather than defaulted. Defaulting it either way
// would be this package deciding whether telemetry crosses a network in
// plaintext.
func (r *reader) endpoint(key string) string {
	raw, ok := r.value(key)
	if !ok {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		r.refuse(key, "%q is not a URL", raw)
		return ""
	}
	switch {
	case parsed.Scheme != "http" && parsed.Scheme != "https":
		r.refuse(key, "%q must name a scheme, http or https, because the scheme is what "+
			"decides whether the export is encrypted", raw)
		return ""
	case parsed.Host == "":
		r.refuse(key, "%q names no host", raw)
		return ""
	}
	return raw
}

// level reads a log level in slog's own spelling, which admits the four names
// and an offset such as INFO+2.
func (r *reader) level(key string, fallback slog.Level) slog.Level {
	raw, ok := r.value(key)
	if !ok {
		return fallback
	}
	var parsed slog.Level
	if err := parsed.UnmarshalText([]byte(raw)); err != nil {
		r.refuse(key, "%q is not a level (debug, info, warn, error)", raw)
		return fallback
	}
	return parsed
}

// format reads the log format.
func (r *reader) format(key string, fallback LogFormat) LogFormat {
	raw, ok := r.value(key)
	if !ok {
		return fallback
	}
	switch LogFormat(strings.ToLower(raw)) {
	case LogJSON:
		return LogJSON
	case LogText:
		return LogText
	default:
		r.refuse(key, "%q is not a log format (%s or %s)", raw, LogJSON, LogText)
		return fallback
	}
}

// backoff reads one schedule from three variables sharing a prefix.
func (r *reader) backoff(prefix string, fallback Backoff) Backoff {
	return Backoff{
		Initial: r.duration(prefix+"_BACKOFF_INITIAL", fallback.Initial),
		Factor:  r.factor(prefix+"_BACKOFF_FACTOR", fallback.Factor),
		Max:     r.duration(prefix+"_BACKOFF_MAX", fallback.Max),
	}
}

func (r *reader) telemetry() Telemetry {
	return Telemetry{
		ServiceName:     r.text("SERVICE_NAME", defaultServiceName),
		LogLevel:        r.level("LOG_LEVEL", defaultLogLevel),
		LogFormat:       r.format("LOG_FORMAT", defaultLogFormat),
		ShutdownTimeout: r.duration("TELEMETRY_SHUTDOWN_TIMEOUT", defaultTelemetryShutdown),
		Disabled:        r.flag("OTEL_SDK_DISABLED", false),
		OTLPEndpoint:    r.endpoint("OTEL_EXPORTER_OTLP_ENDPOINT"),
		ExportTimeout:   r.duration("TELEMETRY_EXPORT_TIMEOUT", defaultExportTimeout),
		MetricInterval:  r.duration("TELEMETRY_METRIC_INTERVAL", defaultMetricInterval),
	}
}

func (r *reader) lifecycle() Lifecycle {
	return Lifecycle{
		StartTimeout: r.duration("START_TIMEOUT", defaultStartTimeout),
		StopTimeout:  r.duration("STOP_TIMEOUT", defaultStopTimeout),
	}
}

func (r *reader) postgres() Postgres {
	return Postgres{
		DSN:              r.secret("DATABASE_URL"),
		MaxConns:         r.conns("DATABASE_MAX_CONNS", defaultMaxConns),
		ConnectTimeout:   r.duration("DATABASE_CONNECT_TIMEOUT", defaultConnectTimeout),
		LockTimeout:      r.duration("DATABASE_LOCK_TIMEOUT", defaultLockTimeout),
		StatementTimeout: r.duration("DATABASE_STATEMENT_TIMEOUT", defaultStatementTimeout),
		HealthTimeout:    r.duration("DATABASE_HEALTH_TIMEOUT", defaultDatabaseHealth),
	}
}

func (r *reader) sqs() SQS {
	return SQS{
		Region:            r.required("AWS_REGION"),
		Endpoint:          r.optional("AWS_ENDPOINT_URL"),
		InboundQueue:      r.text("SQS_INBOUND_QUEUE", defaultInboundQueue),
		OutboundQueue:     r.text("SQS_OUTBOUND_QUEUE", defaultOutboundQueue),
		ClientTimeout:     r.duration("SQS_CLIENT_TIMEOUT", defaultClientTimeout),
		ResolveTimeout:    r.duration("SQS_RESOLVE_TIMEOUT", defaultResolveTimeout),
		MaxMessages:       r.count("SQS_MAX_MESSAGES", defaultMaxMessages),
		WaitTime:          r.duration("SQS_WAIT_TIME", defaultWaitTime),
		VisibilityTimeout: r.duration("SQS_VISIBILITY_TIMEOUT", defaultVisibilityTimeout),
		HealthTimeout:     r.duration("SQS_HEALTH_TIMEOUT", defaultQueueHealth),
		MaxReceiveCount:   r.count("SQS_MAX_RECEIVE_COUNT", defaultMaxReceiveCount),
	}
}

func (r *reader) oidc() OIDC {
	return OIDC{
		Issuer:             r.required("OIDC_ISSUER"),
		Audience:           r.required("OIDC_AUDIENCE"),
		JWKSURI:            r.optional("OIDC_JWKS_URI"),
		Algorithms:         r.list("OIDC_ALGORITHMS"),
		ClockSkew:          r.duration("OIDC_CLOCK_SKEW", defaultClockSkew),
		FetchTimeout:       r.duration("OIDC_FETCH_TIMEOUT", defaultFetchTimeout),
		MinRefreshInterval: r.duration("OIDC_MIN_REFRESH_INTERVAL", defaultMinRefreshInterval),
		HTTPTimeout:        r.duration("OIDC_HTTP_TIMEOUT", defaultOIDCHTTPTimeout),
	}
}

func (r *reader) http() HTTP {
	return HTTP{
		Addr:              r.text("HTTP_ADDR", defaultAddr),
		ReadTimeout:       r.duration("HTTP_READ_TIMEOUT", defaultReadTimeout),
		ReadHeaderTimeout: r.duration("HTTP_READ_HEADER_TIMEOUT", defaultReadHeaderTimeout),
		WriteTimeout:      r.duration("HTTP_WRITE_TIMEOUT", defaultWriteTimeout),
		IdleTimeout:       r.duration("HTTP_IDLE_TIMEOUT", defaultIdleTimeout),
		ShutdownTimeout:   r.duration("HTTP_SHUTDOWN_TIMEOUT", defaultHTTPShutdown),
		MaxHeaderBytes:    r.count("HTTP_MAX_HEADER_BYTES", defaultMaxHeaderBytes),
		MaxBodyBytes:      r.size("HTTP_MAX_BODY_BYTES", defaultMaxBodyBytes),
		ReadinessTimeout:  r.duration("HTTP_READINESS_TIMEOUT", defaultReadinessTimeout),
	}
}

func (r *reader) wagering() Wagering {
	return Wagering{
		Backoff:           r.backoff("WAGERING", defaultWageringBackoff),
		ReferenceAttempts: r.count("REFERENCE_MAX_ATTEMPTS", defaultReferenceAttempts),
		ReferenceTTL:      r.duration("REFERENCE_TTL", defaultReferenceTTL),
	}
}

func (r *reader) consumer() Consumer {
	return Consumer{
		Enabled:      r.flag("CONSUMER_ENABLED", true),
		Name:         r.text("CONSUMER_NAME", defaultConsumerName),
		Concurrency:  r.count("CONSUMER_CONCURRENCY", defaultConcurrency),
		Backoff:      r.backoff("CONSUMER", defaultConsumerBackoff),
		DrainTimeout: r.duration("CONSUMER_DRAIN_TIMEOUT", defaultDrainTimeout),
	}
}

func (r *reader) publisher() Publisher {
	return Publisher{
		Enabled:      r.flag("PUBLISHER_ENABLED", true),
		Name:         r.publisherName(),
		Batch:        r.count("PUBLISHER_BATCH", defaultClaimBatch),
		Hold:         r.duration("PUBLISHER_HOLD", defaultClaimHold),
		Interval:     r.duration("PUBLISHER_INTERVAL", defaultPollInterval),
		Backoff:      r.backoff("PUBLISHER", defaultPublisherBackoff),
		DrainTimeout: r.duration("PUBLISHER_DRAIN_TIMEOUT", defaultDrainTimeout),
	}
}

// publisherName is the one value with no default, and the reason is the only
// place in this configuration where two processes agreeing would be a fault.
//
// It lands in outbox.claimed_by, and a reschedule is scoped to the publisher
// that holds the row — so two publishers sharing a name move each other's
// claims, and the head-of-line event of some wallet is put back by a process
// that was not sending it. A fixed default would guarantee that on the day a
// second replica is deployed, silently, so there is none: PUBLISHER_NAME names
// it, HOSTNAME stands in because a container runtime sets it to something
// distinct per replica, and a process with neither does not start.
func (r *reader) publisherName() string {
	if name, ok := r.value("PUBLISHER_NAME"); ok {
		return name
	}
	if host, ok := r.value("HOSTNAME"); ok {
		return host
	}
	r.refuse("PUBLISHER_NAME", "is required and was not set, and HOSTNAME was not set "+
		"either; a publisher's name must be distinct per process, because two sharing "+
		"one reschedule each other's outbox claims")
	return ""
}

func (r *reader) reference() Reference {
	return Reference{
		Enabled:      r.flag("REFERENCE_WORKER_ENABLED", true),
		Name:         r.text("REFERENCE_WORKER_NAME", defaultReferenceName),
		Interval:     r.duration("REFERENCE_WORKER_INTERVAL", defaultResumeInterval),
		Backoff:      r.backoff("REFERENCE_WORKER", defaultReferenceBackoff),
		DrainTimeout: r.duration("REFERENCE_WORKER_DRAIN_TIMEOUT", defaultDrainTimeout),
	}
}
