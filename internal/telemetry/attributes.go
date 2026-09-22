package telemetry

import "go.opentelemetry.io/otel/attribute"

// The attribute keys every span in this service is described by.
//
// They are the names the log lines already use, so that one identifier is
// spelled one way wherever it is written down — see the package documentation
// for why that is preferred to OpenTelemetry's dotted convention.
//
// There is deliberately no key here for an amount, a balance, a player or a
// body. A trace is read by more people than a database is, and a financial
// payload in one is a financial payload published.
const (
	// KeyCorrelation is the thread tying everything done for one request or one
	// message together. It is the attribute the documented Tempo search is on.
	KeyCorrelation = "correlationId"
	// KeyMessage is the envelope's own message id, which is also the inbox's
	// key — not the queue's delivery id, which names a delivery rather than a
	// message.
	KeyMessage = "messageId"
	// KeyTransaction is this system's identifier for one wager transaction.
	KeyTransaction = "transactionId"
	// KeyWallet is the wallet an operation moved, and the FIFO group an event
	// is ordered within.
	KeyWallet = "walletId"
	// KeyProvider is the provider an operation was submitted as.
	KeyProvider = "providerId"
	// KeyEvent is one published event's identity, stable across republication.
	KeyEvent = "eventId"

	// KeyKind is the kind of operation: BET, WIN, LOSS or ROLLBACK.
	KeyKind = "kind"
	// KeyStatus is what the operation came to: PROCESSED, REJECTED or
	// PENDING_REFERENCE.
	KeyStatus = "status"
	// KeyFailureCode is the catalogued reason a rejected operation was
	// rejected, and is absent when there is none.
	KeyFailureCode = "failureCode"
	// KeyReplay reports that a result was read rather than produced.
	KeyReplay = "idempotentReplay"
	// KeySource names where work entered this service: [SourceHTTP],
	// [SourceQueue] or [SourceReference].
	KeySource = "source"
	// KeyConsumer names the consumer an inbox row belongs to.
	KeyConsumer = "consumer"
	// KeyPublisher names the publisher that holds an outbox claim.
	KeyPublisher = "publisher"
	// KeyOutcome is what one attempt came to, in the vocabulary of whatever was
	// attempted.
	KeyOutcome = "outcome"
	// KeyClass is an app.Class: the answer to "may this be tried again".
	KeyClass = "class"
	// KeyCode is the stable reason something failed. It is a failure.Code, an
	// app.Class or one of this package's own words, and never a rendered error.
	KeyCode = "code"
	// KeyEvents is how many events one batch carried.
	KeyEvents = "events"
	// KeyTransactionKind distinguishes the two transaction shapes the database
	// is asked for: a movement and a snapshot.
	KeyTransactionKind = "transaction"
	// KeyRPCService and KeyRPCMethod name one call against a remote service, in
	// OpenTelemetry's own spelling, because these two are about the wire rather
	// than about this domain.
	KeyRPCService = "rpc.service"
	KeyRPCMethod  = "rpc.method"
	// KeyRPCSystem names the protocol family, likewise.
	KeyRPCSystem = "rpc.system"
)

// The values [KeySource] takes. Three, because there are three doors into this
// service's write path and an operator asking "where did this come from" has
// exactly those three answers.
const (
	// SourceHTTP is a provider's own request.
	SourceHTTP = "http"
	// SourceQueue is a message on the inbound queue.
	SourceQueue = "sqs"
	// SourceReference is the reference worker carrying a parked operation
	// forward.
	SourceReference = "reference"
)

// The words this package uses where nothing else has one.
const (
	// NoFailureCode is what [KeyFailureCode] carries for an operation that was
	// not rejected.
	//
	// A value rather than an absent attribute, because a counter's attribute
	// set decides its time series: a metric that sometimes carries the key and
	// sometimes does not is two series that no dashboard adds up, and
	// "processed operations" would silently exclude nothing while looking like
	// it excluded something.
	NoFailureCode = "NONE"
	// OutcomePublished and OutcomeRefused are what one outbox publish attempt
	// came to. Refused covers every way an event did not reach the queue,
	// because the publisher puts all of them back on the same terms.
	OutcomePublished = "published"
	OutcomeRefused   = "refused"
	// Movement and Snapshot are the two transaction shapes, as [KeyTransactionKind].
	Movement = "movement"
	Snapshot = "snapshot"
	// RPCSystemAWS is what [KeyRPCSystem] carries for a call against AWS.
	RPCSystemAWS = "aws-api"
)

// Correlation names the thread one span belongs to.
func Correlation(id string) attribute.KeyValue { return attribute.String(KeyCorrelation, id) }

// MessageID names the envelope this span is about.
func MessageID(id string) attribute.KeyValue { return attribute.String(KeyMessage, id) }

// TransactionID names the wager transaction this span is about.
func TransactionID(id string) attribute.KeyValue { return attribute.String(KeyTransaction, id) }

// WalletID names the wallet this span is about.
func WalletID(id string) attribute.KeyValue { return attribute.String(KeyWallet, id) }

// ProviderID names the provider this span acts as.
func ProviderID(id string) attribute.KeyValue { return attribute.String(KeyProvider, id) }

// EventID names the published event this span is about.
func EventID(id string) attribute.KeyValue { return attribute.String(KeyEvent, id) }

// Kind names what an operation is: BET, WIN, LOSS or ROLLBACK.
func Kind(kind string) attribute.KeyValue { return attribute.String(KeyKind, kind) }

// Status names what an operation came to.
func Status(status string) attribute.KeyValue { return attribute.String(KeyStatus, status) }

// FailureCode names the catalogued reason a rejected operation was rejected,
// and renders an empty code as [NoFailureCode] for the reason that constant
// gives.
func FailureCode(code string) attribute.KeyValue {
	if code == "" {
		code = NoFailureCode
	}
	return attribute.String(KeyFailureCode, code)
}

// Replay reports that a result was read rather than produced.
func Replay(replay bool) attribute.KeyValue { return attribute.Bool(KeyReplay, replay) }

// Source names the door work entered by.
func Source(source string) attribute.KeyValue { return attribute.String(KeySource, source) }

// Class carries an app.Class, as the string it already is.
func Class(class string) attribute.KeyValue { return attribute.String(KeyClass, class) }

// Some names an identifier only when there is one to name.
//
// It exists because most of these identifiers are optional at the moment a
// span is opened — a message whose body would not parse names no transaction,
// a submission that was refused names no wallet — and an attribute carrying ""
// is worse than an absent one: it is a value somebody can search for and find
// nothing but the failures.
// It allocates rather than filtering in place. In-place is the idiom and would
// be correct at every call site here — they all pass literals — but it writes
// through the caller's backing array, and the one day somebody passes a slice
// they still need is the day a span quietly loses an attribute it had.
func Some(attrs ...attribute.KeyValue) []attribute.KeyValue {
	kept := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		if attr.Value.Type() == attribute.STRING && attr.Value.AsString() == "" {
			continue
		}
		kept = append(kept, attr)
	}
	return kept
}
