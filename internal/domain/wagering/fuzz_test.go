package wagering

import (
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// FuzzParseOpaque holds the rule the identifier constructors exist for: an
// identifier that is accepted is kept exactly as it arrived.
//
// Every one of these values feeds the idempotency hash, so any normalisation —
// trimming a space, repairing a bad byte — would merge two operations a provider
// meant to keep apart. TestIdentifierValidation names the classes that are
// refused; this holds the no-normalisation rule against bytes nobody chose.
func FuzzParseOpaque(f *testing.F) {
	seeds := []string{
		// Accepted.
		"provider-1", "player-42", "ext-abc", "round-7", "game-9", "key-1",
		"ação", "日本", "a b", "-", strings.Repeat("a", maxOpaqueLength),

		// Refused, one per rule.
		"",                                       // missing
		strings.Repeat("a", maxOpaqueLength+1),   // too long
		"\xff\xfe",                               // not UTF-8
		" leading", "trailing ", "\ttab", "nl\n", // surrounded by whitespace
		"a\x00b", "a\x1fb", "a\x7fb", // control characters
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		id, err := parseOpaque[PlayerID](s, "playerId")
		if err != nil {
			if _, ok := failure.CodeOf(err); !ok {
				t.Fatalf("parseOpaque(%q) refused with a code-less error: %v", s, err)
			}
			// A refusal yields nothing a caller could mistake for a value.
			if id != "" {
				t.Fatalf("parseOpaque(%q) refused but returned %q", s, id)
			}
			return
		}

		if string(id) != s {
			t.Fatalf("parseOpaque(%q) = %q, want the bytes back unchanged", s, id)
		}
		// Acceptance is a decision about bytes alone, so feeding the result back
		// has to reach the same decision.
		if _, err := parseOpaque[PlayerID](string(id), "playerId"); err != nil {
			t.Fatalf("parseOpaque refused what it had just accepted: %q: %v", id, err)
		}
	})
}
