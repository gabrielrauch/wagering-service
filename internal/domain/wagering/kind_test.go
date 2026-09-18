package wagering

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// TestKindProperties is the per-kind rule table, asserted directly rather than
// only through the processor.
func TestKindProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind             Kind
		internal         bool
		reversal         bool
		movesMoney       bool
		requiresRef      bool
		allowsRef        bool
		requiresPositive bool
		requiresZero     bool
	}{
		{Opening, true, false, true, false, false, false, false},
		{Bet, false, false, true, false, false, true, false},
		{Win, false, false, true, false, true, true, false},
		{Loss, false, false, false, false, false, false, true},
		{Refund, false, true, true, true, true, true, false},
		{Rollback, false, true, true, true, true, true, false},
	}
	for _, tc := range tests {
		t.Run(string(tc.kind), func(t *testing.T) {
			t.Parallel()
			checks := map[string][2]bool{
				"IsInternal":             {tc.kind.IsInternal(), tc.internal},
				"IsExternal":             {tc.kind.IsExternal(), !tc.internal},
				"IsReversal":             {tc.kind.IsReversal(), tc.reversal},
				"MovesMoney":             {tc.kind.MovesMoney(), tc.movesMoney},
				"RequiresReference":      {tc.kind.RequiresReference(), tc.requiresRef},
				"AllowsReference":        {tc.kind.AllowsReference(), tc.allowsRef},
				"RequiresPositiveAmount": {tc.kind.RequiresPositiveAmount(), tc.requiresPositive},
				"RequiresZeroAmount":     {tc.kind.RequiresZeroAmount(), tc.requiresZero},
			}
			for name, pair := range checks {
				if pair[0] != pair[1] {
					t.Errorf("%s.%s() = %v, want %v", tc.kind, name, pair[0], pair[1])
				}
			}
			// A kind that requires a reference must also allow one.
			if tc.kind.RequiresReference() && !tc.kind.AllowsReference() {
				t.Errorf("%s requires a reference it is not allowed to carry", tc.kind)
			}
			// No kind can demand a positive and a zero amount at once.
			if tc.kind.RequiresPositiveAmount() && tc.kind.RequiresZeroAmount() {
				t.Errorf("%s demands both a positive and a zero amount", tc.kind)
			}
		})
	}
}

// TestCanReverse is the matrix of what each reversal may act on.
func TestCanReverse(t *testing.T) {
	t.Parallel()

	allowed := map[Kind]map[Kind]bool{
		Refund:   {Bet: true},
		Rollback: {Bet: true, Win: true, Refund: true},
	}
	for _, reversal := range Kinds() {
		for _, target := range Kinds() {
			want := allowed[reversal][target]
			if got := reversal.CanReverse(target); got != want {
				t.Errorf("%s.CanReverse(%s) = %v, want %v", reversal, target, got, want)
			}
		}
	}

	// A rollback can never itself be undone, which is what makes one applied
	// straight to a bet permanent.
	if Rollback.CanReverse(Rollback) {
		t.Error("a rollback can be rolled back")
	}
	// Only a reversal reverses anything.
	for _, kind := range []Kind{Opening, Bet, Win, Loss} {
		for _, target := range Kinds() {
			if kind.CanReverse(target) {
				t.Errorf("%s claims it can reverse %s", kind, target)
			}
		}
	}
}

func TestKindDirection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind Kind
		want Direction
		ok   bool
	}{
		{Opening, Credit, true},
		{Bet, Debit, true},
		{Win, Credit, true},
		{Refund, Credit, true},
		// A loss moves nothing, and a rollback takes the opposite of whatever it
		// undoes, so neither has a direction of its own.
		{Loss, "", false},
		{Rollback, "", false},
	}
	for _, tc := range tests {
		t.Run(string(tc.kind), func(t *testing.T) {
			t.Parallel()
			got, ok := tc.kind.Direction()
			if ok != tc.ok || got != tc.want {
				t.Errorf("%s.Direction() = %s, %v, want %s, %v", tc.kind, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestReversalDirection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target Kind
		want   Direction
		ok     bool
	}{
		{Bet, Credit, true},
		{Win, Debit, true},
		{Refund, Debit, true},
		{Loss, "", false},
		{Rollback, "", false},
	}
	for _, tc := range tests {
		t.Run(string(tc.target), func(t *testing.T) {
			t.Parallel()
			got, ok := reversalDirection(tc.target)
			if ok != tc.ok || got != tc.want {
				t.Errorf("reversalDirection(%s) = %s, %v, want %s, %v", tc.target, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestParsing(t *testing.T) {
	t.Parallel()

	t.Run("kinds", func(t *testing.T) {
		t.Parallel()
		for _, want := range Kinds() {
			got, err := ParseKind(string(want))
			if err != nil || got != want {
				t.Errorf("ParseKind(%q) = %s, %v", want, got, err)
			}
		}
		for _, bad := range []string{"", "bet", "Bet", "TRANSFER", " BET"} {
			if _, err := ParseKind(bad); !failure.Is(err, failure.InvalidFieldFormat) {
				t.Errorf("ParseKind(%q) = %v, want %v", bad, err, failure.InvalidFieldFormat)
			}
		}
	})

	t.Run("statuses", func(t *testing.T) {
		t.Parallel()
		for _, want := range Statuses() {
			got, err := ParseStatus(string(want))
			if err != nil || got != want {
				t.Errorf("ParseStatus(%q) = %s, %v", want, got, err)
			}
		}
		for _, bad := range []string{"", "pending", "SETTLING"} {
			if _, err := ParseStatus(bad); !failure.Is(err, failure.InvalidFieldFormat) {
				t.Errorf("ParseStatus(%q) = %v, want %v", bad, err, failure.InvalidFieldFormat)
			}
		}
	})
}

func TestIdentifierValidation(t *testing.T) {
	t.Parallel()

	t.Run("opaque identifiers", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name  string
			input string
			want  failure.Code
		}{
			{"empty", "", failure.MissingRequiredField},
			{"leading space", " player", failure.InvalidFieldFormat},
			{"trailing space", "player ", failure.InvalidFieldFormat},
			{"newline", "player\n", failure.InvalidFieldFormat},
			{"control character", "play\x00er", failure.InvalidFieldFormat},
			{"invalid UTF-8", "play\xffer", failure.InvalidFieldFormat},
			{"too long", string(make([]byte, maxOpaqueLength+1)), failure.InvalidFieldFormat},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				if _, err := NewPlayerID(tc.input); !failure.Is(err, tc.want) {
					t.Errorf("NewPlayerID(%q) = %v, want %v", tc.input, err, tc.want)
				}
			})
		}

		// Inner spaces and non-ASCII are fine: an identifier is opaque and must
		// survive byte for byte into the hash.
		for _, good := range []string{"player 1", "jogador-ç", "player_1", "PLAYER"} {
			if _, err := NewPlayerID(good); err != nil {
				t.Errorf("NewPlayerID(%q) = %v, want no error", good, err)
			}
		}
	})

	t.Run("UUID identifiers", func(t *testing.T) {
		t.Parallel()
		minted := NewWalletID()
		parsed, err := ParseWalletID(minted.String())
		if err != nil {
			t.Fatalf("ParseWalletID: %v", err)
		}
		if parsed != minted {
			t.Error("a minted identifier did not survive a round trip")
		}

		if _, err := ParseWalletID(""); !failure.Is(err, failure.MissingRequiredField) {
			t.Error("an empty UUID was accepted")
		}
		if _, err := ParseWalletID("not-a-uuid"); !failure.Is(err, failure.InvalidFieldFormat) {
			t.Error("a malformed UUID was accepted")
		}
		// The nil UUID is well-formed but names nothing.
		if _, err := ParseWalletID("00000000-0000-0000-0000-000000000000"); !failure.Is(err, failure.MissingRequiredField) {
			t.Error("the nil UUID was accepted")
		}
	})

	t.Run("minted identifiers are distinct and never zero", func(t *testing.T) {
		t.Parallel()
		seen := make(map[WalletID]bool, 100)
		for range 100 {
			id := NewWalletID()
			if id.IsZero() {
				t.Fatal("a minted identifier is the zero value")
			}
			if seen[id] {
				t.Fatal("a minted identifier repeated")
			}
			seen[id] = true
		}
	})
}
