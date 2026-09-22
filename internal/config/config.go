package config

import (
	"log/slog"
	"net/url"
	"time"
)

// Config is everything this service is tuned with.
//
// One value for both binaries rather than one per binary. The API and the
// worker share the database, the queues, the clock and the schedule an
// operation is retried on, and two configuration types would be two places for
// those to drift — a worker that parked operations on a different budget from
// the API that accepted them would settle them at a time the API never
// promised. Which parts a binary actually reads is the composition root's
// decision, and it is made by which modules that binary includes.
type Config struct {
	// Telemetry is this process's own observability.
	Telemetry Telemetry
	// Lifecycle bounds starting and stopping the process as a whole.
	Lifecycle Lifecycle
	// Postgres is the database this service's state lives in.
	Postgres Postgres
	// SQS is the two queues it receives on and sends to.
	SQS SQS
	// OIDC is the identity provider whose tokens it accepts.
	OIDC OIDC
	// HTTP is the server the API binary carries its routes over.
	HTTP HTTP
	// Wagering is the application layer's and the domain's own numbers.
	Wagering Wagering
	// Consumer, Publisher and Reference are the three loops the worker binary
	// runs, each of which can be switched off on its own.
	Consumer  Consumer
	Publisher Publisher
	Reference Reference
}

// LogFormat is how a log line is written.
type LogFormat string

// The two formats. JSON is the default because the lines are read by a log
// aggregator far more often than by a person, and text is there for the person.
const (
	LogJSON LogFormat = "json"
	LogText LogFormat = "text"
)

// Telemetry is where this process reports what it is doing.
type Telemetry struct {
	// ServiceName names this service in every log line and in the resource
	// attributes of every span and every measurement.
	ServiceName string
	// LogLevel is the lowest level that is written.
	LogLevel slog.Level
	// LogFormat is JSON or text.
	LogFormat LogFormat
	// ShutdownTimeout bounds the last thing the process does. It is the budget
	// the telemetry flush runs under, and a flush that is not bounded holds a
	// deployment open on an exporter that is not answering.
	ShutdownTimeout time.Duration

	// Disabled switches the OpenTelemetry SDK off, whatever else is set.
	//
	// It is OTEL_SDK_DISABLED, which is the specification's own name for this,
	// so that an operator who knows OpenTelemetry does not have to learn a
	// second spelling for the one variable they are most likely to reach for.
	Disabled bool
	// OTLPEndpoint is the collector spans and measurements are exported to, as
	// a URL — http://host:4317 for plaintext gRPC, https:// for TLS.
	//
	// Empty means nothing is exported, and that is a deliberate departure from
	// the specification, which defaults it to http://localhost:4317. A default
	// of "somewhere" makes a laptop, a unit test and a CI runner all retry a
	// connection to a collector nobody started, for ever, in the background of
	// every run. Empty is the honest statement that there is nowhere to send
	// telemetry — it is reported once at start-up — and a deployment that wants
	// it says where.
	OTLPEndpoint string
	// ExportTimeout bounds one attempt at exporting a batch. An exporter that
	// is not answering must not become back pressure on the thing being
	// measured.
	ExportTimeout time.Duration
	// MetricInterval is how often measurements are exported. It is also how
	// often the outbox lag is asked for, since that one is observed on
	// collection rather than recorded on a loop.
	MetricInterval time.Duration
}

// Exporting reports whether this process has anywhere to send telemetry.
//
// Two conditions, one answer, because a caller asking "do I build the SDK"
// must not have to remember both — and because the two are different
// sentences for an operator: switched off deliberately, or never told where.
func (t Telemetry) Exporting() bool { return !t.Disabled && t.OTLPEndpoint != "" }

// Lifecycle bounds the whole of start-up and the whole of shutdown.
//
// These are the outer bounds, not the only ones: every hook that talks to
// anything bounds itself as well, so that a process failing to start says which
// dependency it could not reach rather than only that it took too long.
type Lifecycle struct {
	// StartTimeout bounds every OnStart hook together.
	StartTimeout time.Duration
	// StopTimeout bounds every OnStop hook together. It must be generous
	// enough for the drains beneath it — the HTTP server's, then each worker's
	// — because it is the budget they are all spent from.
	StopTimeout time.Duration
}

// Postgres is the connection to the database and the bounds it is used under.
type Postgres struct {
	// DSN names the database and the role. It carries a password and is the
	// only secret in this configuration; see [Postgres.Redacted].
	DSN string
	// MaxConns bounds the pool.
	MaxConns int32
	// ConnectTimeout bounds opening one connection.
	ConnectTimeout time.Duration
	// LockTimeout bounds waiting for a row lock.
	LockTimeout time.Duration
	// StatementTimeout bounds a statement that has started.
	StatementTimeout time.Duration
	// HealthTimeout bounds the readiness probe.
	HealthTimeout time.Duration
}

// Redacted is the DSN with its password replaced, which is the only form of it
// that may be written down.
//
// A DSN that will not parse renders as a fixed sentence rather than as itself,
// because the reason it will not parse may be that the password contains
// something that is not a URL — and a value printed to explain a parse failure
// is a value printed.
func (p Postgres) Redacted() string {
	parsed, err := url.Parse(p.DSN)
	if err != nil {
		return "<unparseable dsn>"
	}
	if _, set := parsed.User.Password(); set {
		parsed.User = url.UserPassword(parsed.User.Username(), "xxxxx")
	}
	return parsed.String()
}

// SQS is the two queues and the bounds every call against them is made under.
type SQS struct {
	// Region is the AWS region the queues live in.
	Region string
	// Endpoint overrides where requests are sent, for LocalStack and for
	// nothing else. Empty is the real regional endpoint.
	Endpoint string
	// InboundQueue is the FIFO queue wager operations arrive on.
	InboundQueue string
	// OutboundQueue is the FIFO queue wallet events leave on.
	OutboundQueue string
	// ClientTimeout bounds building the SQS client, which resolves credentials
	// from the SDK's default chain. That chain reaches the instance metadata
	// service on an EC2 host, so an unbounded build is a process that hangs at
	// start-up on a host where that address is black-holed.
	ClientTimeout time.Duration
	// ResolveTimeout bounds turning a queue name into its URL at start-up.
	ResolveTimeout time.Duration
	// MaxMessages is how many messages one receive may return, 1 to 10.
	MaxMessages int
	// WaitTime is how long a receive waits before answering empty. Whole
	// seconds, at most twenty.
	WaitTime time.Duration
	// VisibilityTimeout is how long a received message is hidden. Whole
	// seconds. It must exceed the consumer's drain timeout, or a message given
	// back at shutdown is given back with a receipt handle that has expired.
	VisibilityTimeout time.Duration
	// HealthTimeout bounds the readiness probe.
	HealthTimeout time.Duration
	// MaxReceiveCount is the queues' redrive policy, so that the consumer can
	// say when a delivery is the last one before the dead-letter queue. It is
	// configuration rather than something read back from the queue, because
	// reading it would be a call per message.
	MaxReceiveCount int
}

// OIDC is the identity provider this service accepts tokens from.
type OIDC struct {
	// Issuer is the value a token's "iss" claim must carry, byte for byte.
	// For Keycloak that is <base>/realms/<realm>, and it must not end in a
	// slash: the verifier compares exactly and does not normalise.
	Issuer string
	// Audience is the value that must appear in a token's "aud" claim.
	Audience string
	// JWKSURI is where the signing keys are published. Empty means discovery
	// through the issuer's OpenID configuration document.
	JWKSURI string
	// Algorithms is the signature algorithm allow-list, by JOSE name. Empty
	// means the adapter's default, which is every asymmetric family it knows.
	Algorithms []string
	// ClockSkew is how far this process's clock may be from the issuer's.
	ClockSkew time.Duration
	// FetchTimeout bounds one attempt at obtaining the key set, including the
	// start-up fetch that decides whether this process starts at all.
	FetchTimeout time.Duration
	// MinRefreshInterval is the shortest gap between two fetches, which is what
	// keeps a stream of invented key identifiers from becoming an amplifier
	// pointed at the identity provider.
	MinRefreshInterval time.Duration
	// HTTPTimeout bounds one request made by the client that fetches keys. The
	// adapter requires a client rather than defaulting to http.DefaultClient,
	// so this is the timeout that client is built with.
	HTTPTimeout time.Duration
}

// HTTP is the server the API binary listens on.
type HTTP struct {
	// Addr is the address to listen on, in net.Listen's form.
	Addr string
	// ReadTimeout bounds reading a whole request.
	ReadTimeout time.Duration
	// ReadHeaderTimeout bounds reading the headers alone.
	ReadHeaderTimeout time.Duration
	// WriteTimeout bounds writing a response.
	WriteTimeout time.Duration
	// IdleTimeout bounds how long a kept-alive connection may sit unused.
	IdleTimeout time.Duration
	// ShutdownTimeout is how long the server waits for requests in flight.
	ShutdownTimeout time.Duration
	// MaxHeaderBytes bounds the headers of one request.
	MaxHeaderBytes int
	// MaxBodyBytes bounds a request body.
	MaxBodyBytes int64
	// ReadinessTimeout bounds the whole readiness probe, not each check in it.
	ReadinessTimeout time.Duration
}

// Wagering is the application layer's schedule and the domain's wait budget.
type Wagering struct {
	// Backoff is when a parked operation is looked at again. It becomes
	// app.BackoffPolicy.
	Backoff Backoff
	// ReferenceAttempts is how many times an operation may be parked waiting
	// for the operation it refers to.
	ReferenceAttempts int
	// ReferenceTTL is how long after the first wait it stops waiting.
	//
	// Whichever of the two runs out first settles the operation, rejected for
	// the reference it waited for. They are the domain's budget and are
	// deliberately not the same numbers as [Reference], which is only how often
	// the loop looks.
	ReferenceTTL time.Duration
}

// Backoff is an exponential schedule, in the shape app.BackoffPolicy and
// workers.Backoff both take.
//
// It is restated here rather than imported from either, so that this package
// depends on nothing in this tree and cannot acquire an opinion about which of
// them a triple is for. The composition root converts.
type Backoff struct {
	// Initial is the wait after the first failed attempt.
	Initial time.Duration
	// Factor multiplies the wait after each further attempt, and is at least 1.
	Factor float64
	// Max caps it, and is at least Initial.
	Max time.Duration
}

// Consumer is the loop that takes wager operations off the inbound queue.
type Consumer struct {
	// Enabled is whether the worker binary runs this loop.
	Enabled bool
	// Name identifies the consumer in the inbox and MUST be identical across
	// every replica.
	//
	// The inbox holds one row per consumer per message, and that row is what
	// turns a redelivery into a replay. A name derived from a hostname or a
	// process id would give each replica its own rows, so the same message
	// redelivered to a different replica would be applied a second time — the
	// exact outcome the inbox exists to prevent. It is therefore a plain
	// configured string with a stable default, and nothing here derives it.
	Name string
	// Concurrency is how many messages this instance handles at once.
	Concurrency int
	// Backoff is how long a message waits after a transient failure.
	Backoff Backoff
	// DrainTimeout is how long a stop waits for work in hand. It should be
	// shorter than [SQS.VisibilityTimeout], so that a message given back at
	// shutdown is given back with a receipt handle that is still worth
	// something.
	DrainTimeout time.Duration
}

// Publisher is the loop that moves outbox events onto the outbound queue.
type Publisher struct {
	// Enabled is whether the worker binary runs this loop.
	Enabled bool
	// Name identifies this publisher in the claims it takes and MUST be
	// distinct per process — the opposite of [Consumer.Name], for the opposite
	// reason.
	//
	// It lands in outbox.claimed_by and is what scopes a reschedule to the
	// publisher holding the row. Two publishers sharing a name reschedule each
	// other's claims, which is the one thing the scoping exists to prevent. It
	// therefore has no fixed default: it is PUBLISHER_NAME, or HOSTNAME, which
	// a container runtime sets to something distinct per replica, and a process
	// with neither does not start.
	Name string
	// Batch is how many events one turn claims, at most ten.
	Batch int
	// Hold is how long a claim stands.
	Hold time.Duration
	// Interval is how long an idle publisher waits before claiming again.
	Interval time.Duration
	// Backoff is how long an event waits after a send it did not survive.
	Backoff Backoff
	// DrainTimeout is how long a stop waits for the turn in progress.
	DrainTimeout time.Duration
}

// Reference is the loop that carries parked operations forward.
type Reference struct {
	// Enabled is whether the worker binary runs this loop.
	Enabled bool
	// Name is the subject the service principal acts under, kept for the audit
	// trail and never for a decision. Stable rather than per-process, so that a
	// row's audit trail names the job and not the pod that happened to run it.
	Name string
	// Interval is how long the worker waits when nothing was due.
	Interval time.Duration
	// Backoff is how long it waits after a turn that failed.
	Backoff Backoff
	// DrainTimeout is how long a stop waits for the turn in progress.
	DrainTimeout time.Duration
}
