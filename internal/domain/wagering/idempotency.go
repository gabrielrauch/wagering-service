package wagering

import (
	"crypto/sha256"
	"encoding/hex"
)

// PayloadHash is the fingerprint of an operation's business fields: the
// lowercase hex SHA-256 of its canonical JSON form.
type PayloadHash string

// String returns the hash as lowercase hex.
func (h PayloadHash) String() string { return string(h) }

// CanonicalPayload returns the exact bytes the idempotency hash is taken over.
//
// # Algorithm
//
// The payload is a single JSON object containing the operation's business
// fields and nothing else, with its keys in ASCII order and no insignificant
// whitespace:
//
//	{"externalTransactionId":"…","gameId":"…","kind":"BET",
//	 "money":{"amount":"25.00","currency":"BRL"},
//	 "playerId":"…","provider":"…",
//	 "referenceExternalTransactionId":"…","roundId":"…"}
//
// SHA-256 of those bytes, hex-encoded lowercase, is the [PayloadHash].
//
// # What is included
//
// Only what identifies the operation as a piece of business: provider,
// externalTransactionId, kind, money, playerId, roundId, gameId and the
// reference when there is one.
//
// # What is excluded
//
// The idempotency key itself, because the hash exists to decide whether one key
// has been reused for two different operations — including it would make every
// submission trivially unique. The wallet id, because it is derived from the
// player and currency rather than submitted. And all transport metadata:
// timestamps, correlation and causation ids, queue message ids, headers,
// delivery counts and retry attempts, none of which say anything about what the
// provider asked for.
//
// # Normalisation
//
// None is applied. Every input has exactly one valid spelling by the time it
// gets here: an amount must carry exactly two decimal places, a currency must
// be uppercase, and identifiers are refused rather than trimmed. The canonical
// form is therefore the submitted form, and no transformation stands between
// what a provider sent and what is hashed.
//
// The reference key is omitted entirely when the operation names none, rather
// than emitted as null, so that "no reference" has one representation.
//
// Strings are escaped minimally, per RFC 8259: quote, backslash and the control
// characters, with no HTML escaping and no \u encoding of anything else. Every
// other byte is copied through exactly as it arrived, including one that is not
// valid UTF-8 — the encoder never substitutes a replacement character for what a
// provider sent, because a hash that quietly repaired its input would be a hash
// of something nobody submitted. That is why the encoding is written out here
// rather than delegated: it must be reproducible by any transport, byte for
// byte, without depending on a JSON library's defaults.
func (c Command) CanonicalPayload() string {
	var buf [canonicalPayloadReserve]byte
	return string(c.appendCanonicalPayload(buf[:0]))
}

// canonicalPayloadReserve is the buffer the encoder starts from. It is a
// reserve rather than a bound: a payload that outgrows it moves to the heap of
// its own accord, and one that fits — which every payload built from
// identifiers of ordinary length does — is encoded, and hashed, without
// allocating at all.
//
// It is generous enough for eight values of UUID length together with the keys
// around them, and small enough to sit on a stack without thought.
const canonicalPayloadReserve = 512

// PayloadHash returns the fingerprint of the command's business fields.
//
// The command is validated first, so a hash is only ever taken over an
// operation that could actually be recorded. Computing it here, rather than
// accepting one from the caller, is what guarantees that two transports cannot
// disagree about whether a submission is a retry.
func (c Command) PayloadHash() (PayloadHash, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	return c.payloadHash(), nil
}

// payloadHash hashes an already-validated command.
//
// The payload is hashed as the bytes the encoder produced rather than through
// [Command.CanonicalPayload], whose string would be copied once to be made and
// once more to be handed back to the hasher — two copies of a value that is
// read once and discarded.
func (c Command) payloadHash() PayloadHash {
	var buf [canonicalPayloadReserve]byte
	sum := sha256.Sum256(c.appendCanonicalPayload(buf[:0]))

	// The digest is a fixed thirty-two bytes and its hex form a fixed
	// sixty-four, so both are rendered in arrays the compiler sizes and the
	// stack holds. Only the hash that is returned reaches the heap.
	var digits [payloadHashLength]byte
	hex.Encode(digits[:], sum[:])
	return PayloadHash(digits[:])
}

// payloadHashLength is the length of a hex-encoded SHA-256 digest.
const payloadHashLength = 2 * sha256.Size

// appendCanonicalPayload appends the canonical payload to dst and returns the
// extended buffer.
//
// It is the one statement of the encoding; everything that needs the payload —
// as a string to read, or as bytes to hash — asks here, so the two cannot
// produce different bytes.
func (c Command) appendCanonicalPayload(dst []byte) []byte {
	dst = append(dst, '{')

	dst = appendFirstJSONField(dst, "externalTransactionId", string(c.ExternalTransactionID))
	dst = appendJSONField(dst, "gameId", string(c.GameID))
	dst = appendJSONField(dst, "kind", string(c.Kind))

	// The amount is appended rather than escaped, because it is digits, a point
	// and possibly a sign — a set of characters with nothing in it that JSON
	// represents any other way.
	dst = append(dst, `,"money":{"amount":"`...)
	dst = c.Money.AppendAmount(dst)
	dst = append(dst, '"')
	dst = appendJSONField(dst, "currency", c.Money.Currency().String())
	dst = append(dst, '}')

	dst = appendJSONField(dst, "playerId", string(c.PlayerID))
	dst = appendJSONField(dst, "provider", string(c.Provider))
	if c.ReferenceExternalTransactionID != "" {
		dst = appendJSONField(dst, "referenceExternalTransactionId", string(c.ReferenceExternalTransactionID))
	}
	dst = appendJSONField(dst, "roundId", string(c.RoundID))

	return append(dst, '}')
}

// appendFirstJSONField appends "key":"value" with no leading comma, for the
// field that opens an object.
func appendFirstJSONField(dst []byte, key, value string) []byte {
	dst = appendJSONString(dst, key)
	dst = append(dst, ':')
	return appendJSONString(dst, value)
}

// appendJSONField appends ,"key":"value" — every field but the first.
func appendJSONField(dst []byte, key, value string) []byte {
	return appendFirstJSONField(append(dst, ','), key, value)
}

// hexDigits spells the \u escape's four digits, which are lowercase.
const hexDigits = "0123456789abcdef"

// appendJSONString appends a minimally escaped JSON string.
//
// It walks bytes rather than runes. Only ASCII is ever escaped, and every byte
// of a multi-byte rune is above the escaped range, so decoding one only to
// re-encode it unchanged would be work done for nothing — and, for a string
// that is not valid UTF-8, work that would silently substitute the replacement
// character for what a provider actually sent. Copying the bytes through
// leaves the payload exactly as submitted, which is the whole point of it.
//
// Runs that need no escaping are copied whole rather than a byte at a time,
// which for an identifier — where nothing needs escaping at all — makes the
// body of the loop a single copy.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := range len(s) {
		ch := s[i]
		if ch >= asciiSpace && ch != '"' && ch != '\\' {
			continue
		}
		dst = append(dst, s[start:i]...)
		switch ch {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			// Unreachable for validated identifiers, which refuse control
			// characters outright, but kept so the encoder is total.
			dst = append(dst, '\\', 'u', '0', '0', hexDigits[ch>>4], hexDigits[ch&0xf])
		}
		start = i + 1
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
