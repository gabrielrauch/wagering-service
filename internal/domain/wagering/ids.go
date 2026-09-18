package wagering

import (
	"strings"
	"unicode/utf8"
	"uuid"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// maxOpaqueLength bounds a provider-supplied identifier. It is generous enough
// for a UUID, a ULID or a composite key, and small enough that an identifier
// cannot be used to smuggle a payload.
const maxOpaqueLength = 128

// The ASCII control-character boundary. Provider-supplied identifiers refuse
// every rune below asciiSpace and asciiDelete itself; the canonical-payload
// encoder in idempotency.go escapes the same range, and the two must agree.
const (
	asciiSpace  = 0x20
	asciiDelete = 0x7f
)

// Identifiers minted by this system. Each is a distinct type over [uuid.UUID],
// so a transaction id cannot be passed where a wallet id belongs, and each has
// a zero value that names nothing and is refused everywhere.
type (
	// WalletID identifies a wallet.
	WalletID uuid.UUID
	// TransactionID identifies a wager transaction within this system, as
	// opposed to the provider's own identifier for it.
	TransactionID uuid.UUID
	// LedgerEntryID identifies a wallet ledger entry.
	LedgerEntryID uuid.UUID
)

// Identifiers owned by a provider. They are opaque: validated for shape,
// compared byte for byte, and never parsed or normalised, so that the value
// hashed is the value submitted.
type (
	// Provider names the game operator that submitted an operation.
	Provider string
	// PlayerID is the provider's identifier for the player.
	PlayerID string
	// ExternalTransactionID is the provider's identifier for an operation.
	ExternalTransactionID string
	// RoundID identifies one play of a game.
	RoundID string
	// GameID identifies the game a round belongs to.
	GameID string
	// IdempotencyKey binds repeated submissions to a single wager transaction.
	IdempotencyKey string
)

// NewWalletID mints a time-ordered wallet identifier.
func NewWalletID() WalletID { return WalletID(uuid.NewV7()) }

// NewTransactionID mints a time-ordered transaction identifier.
func NewTransactionID() TransactionID { return TransactionID(uuid.NewV7()) }

// NewLedgerEntryID mints a time-ordered ledger entry identifier.
func NewLedgerEntryID() LedgerEntryID { return LedgerEntryID(uuid.NewV7()) }

// IsZero reports whether the identifier names nothing.
func (id WalletID) IsZero() bool { return id == WalletID{} }

// IsZero reports whether the identifier names nothing.
func (id TransactionID) IsZero() bool { return id == TransactionID{} }

// IsZero reports whether the identifier names nothing.
func (id LedgerEntryID) IsZero() bool { return id == LedgerEntryID{} }

// String returns the canonical UUID form.
func (id WalletID) String() string { return uuid.UUID(id).String() }

// String returns the canonical UUID form.
func (id TransactionID) String() string { return uuid.UUID(id).String() }

// String returns the canonical UUID form.
func (id LedgerEntryID) String() string { return uuid.UUID(id).String() }

// ParseWalletID reads a wallet identifier from its canonical UUID form.
func ParseWalletID(s string) (WalletID, error) { return parseUUID[WalletID](s, "walletId") }

// ParseTransactionID reads a transaction identifier from its canonical UUID form.
func ParseTransactionID(s string) (TransactionID, error) {
	return parseUUID[TransactionID](s, "transactionId")
}

// ParseLedgerEntryID reads a ledger entry identifier from its canonical UUID form.
func ParseLedgerEntryID(s string) (LedgerEntryID, error) {
	return parseUUID[LedgerEntryID](s, "ledgerEntryId")
}

// NewProvider validates a provider name.
func NewProvider(s string) (Provider, error) { return parseOpaque[Provider](s, "provider") }

// NewPlayerID validates a player identifier.
func NewPlayerID(s string) (PlayerID, error) { return parseOpaque[PlayerID](s, "playerId") }

// NewExternalTransactionID validates a provider's transaction identifier.
func NewExternalTransactionID(s string) (ExternalTransactionID, error) {
	return parseOpaque[ExternalTransactionID](s, "externalTransactionId")
}

// NewRoundID validates a round identifier.
func NewRoundID(s string) (RoundID, error) { return parseOpaque[RoundID](s, "roundId") }

// NewGameID validates a game identifier.
func NewGameID(s string) (GameID, error) { return parseOpaque[GameID](s, "gameId") }

// NewIdempotencyKey validates an idempotency key.
func NewIdempotencyKey(s string) (IdempotencyKey, error) {
	return parseOpaque[IdempotencyKey](s, "idempotencyKey")
}

// String returns the identifier unchanged.
func (p Provider) String() string { return string(p) }

// String returns the identifier unchanged.
func (p PlayerID) String() string { return string(p) }

// String returns the identifier unchanged.
func (e ExternalTransactionID) String() string { return string(e) }

// String returns the identifier unchanged.
func (r RoundID) String() string { return string(r) }

// String returns the identifier unchanged.
func (g GameID) String() string { return string(g) }

// String returns the identifier unchanged.
func (k IdempotencyKey) String() string { return string(k) }

// parseUUID reads any UUID-backed identifier, refusing the nil UUID so that a
// well-formed but empty identifier cannot slip through.
func parseUUID[T ~[16]byte](s, field string) (T, error) {
	var zero T
	if s == "" {
		return zero, missing(field)
	}
	parsed, err := uuid.Parse(s)
	if err != nil {
		return zero, failure.Wrap(err, failure.InvalidFieldFormat, "%q is not a UUID", s).WithField(field)
	}
	if parsed == uuid.Nil() {
		return zero, failure.New(failure.MissingRequiredField, "must not be the nil UUID").WithField(field)
	}
	return T(parsed), nil
}

// parseOpaque validates a provider-supplied identifier.
//
// Surrounding whitespace is refused rather than trimmed. Trimming would be a
// normalisation, and a normalisation can merge two identifiers a provider meant
// to keep distinct — which, for values that feed the idempotency hash, would
// silently collapse two operations into one.
func parseOpaque[T ~string](s, field string) (T, error) {
	var zero T
	if s == "" {
		return zero, missing(field)
	}
	if len(s) > maxOpaqueLength {
		return zero, failure.New(failure.InvalidFieldFormat,
			"must be at most %d bytes, got %d", maxOpaqueLength, len(s)).WithField(field)
	}
	if !utf8.ValidString(s) {
		return zero, failure.New(failure.InvalidFieldFormat, "must be valid UTF-8").WithField(field)
	}
	if strings.TrimSpace(s) != s {
		return zero, failure.New(failure.InvalidFieldFormat,
			"must not be surrounded by whitespace").WithField(field)
	}
	for _, r := range s {
		if r < asciiSpace || r == asciiDelete {
			return zero, failure.New(failure.InvalidFieldFormat,
				"must not contain control characters").WithField(field)
		}
	}
	return T(s), nil
}

// validateProviderFields checks the shape of every identifier a provider owns.
//
// A submission is held to it by [Command.Validate] and a stored row by
// providerFields is the set of identifiers a provider owns. Naming them keeps
// the four ~string-based fields from being transposed at a call site, which the
// compiler cannot catch when they are passed positionally.
type providerFields struct {
	Provider              Provider
	ExternalTransactionID ExternalTransactionID
	IdempotencyKey        IdempotencyKey
	PlayerID              PlayerID
	RoundID               RoundID
	GameID                GameID
}

// [TransactionSnapshot.validateOrigin]: a stored identifier that a submission
// would have been refused for is corruption, so the two must ask the same
// question. The order is the order a provider reads them in.
func validateProviderFields(f providerFields) error {
	if _, err := NewProvider(string(f.Provider)); err != nil {
		return err
	}
	if _, err := NewExternalTransactionID(string(f.ExternalTransactionID)); err != nil {
		return err
	}
	if _, err := NewIdempotencyKey(string(f.IdempotencyKey)); err != nil {
		return err
	}
	if _, err := NewPlayerID(string(f.PlayerID)); err != nil {
		return err
	}
	if _, err := NewRoundID(string(f.RoundID)); err != nil {
		return err
	}
	if _, err := NewGameID(string(f.GameID)); err != nil {
		return err
	}
	return nil
}
