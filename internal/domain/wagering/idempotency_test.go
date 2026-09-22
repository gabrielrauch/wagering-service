package wagering

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

// fixedWalletID is the wallet fixedCommand addresses, written out because the
// canonical payload carries it and is asserted byte for byte.
const fixedWalletID = "0192f291-27dd-7d3f-8071-5f8685deef37"

// fixedCommand is a submission with every value written out, so the canonical
// payload can be asserted byte for byte.
func fixedCommand(t *testing.T) Command {
	t.Helper()
	wallet, err := ParseWalletID(fixedWalletID)
	if err != nil {
		t.Fatalf("ParseWalletID(%q): %v", fixedWalletID, err)
	}
	return Command{
		TransactionID:         NewTransactionID(),
		LedgerEntryID:         NewLedgerEntryID(),
		WalletID:              wallet,
		Provider:              "acme-games",
		ExternalTransactionID: "ext-1",
		IdempotencyKey:        "key-1",
		PlayerID:              "player-1",
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  Bet,
		Money:                 brl(t, "25.00"),
	}
}

// TestCanonicalPayload pins the exact bytes the hash is taken over. Two
// transports must agree on them, so they are asserted literally rather than
// round-tripped.
func TestCanonicalPayload(t *testing.T) {
	t.Parallel()

	t.Run("without a reference", func(t *testing.T) {
		t.Parallel()
		const want = `{"externalTransactionId":"ext-1","gameId":"game-1","kind":"BET",` +
			`"money":{"amount":"25.00","currency":"BRL"},` +
			`"playerId":"player-1","providerId":"acme-games","roundId":"round-1",` +
			`"walletId":"` + fixedWalletID + `"}`

		got := fixedCommand(t).CanonicalPayload()
		if got != want {
			t.Errorf("CanonicalPayload()\n got %s\nwant %s", got, want)
		}
		if !json.Valid([]byte(got)) {
			t.Error("the canonical payload is not valid JSON")
		}
	})

	t.Run("with a reference", func(t *testing.T) {
		t.Parallel()
		cmd := fixedCommand(t)
		cmd.Kind = Refund
		cmd.ReferenceExternalTransactionID = "ext-0"

		const want = `{"externalTransactionId":"ext-1","gameId":"game-1","kind":"REFUND",` +
			`"money":{"amount":"25.00","currency":"BRL"},` +
			`"playerId":"player-1","providerId":"acme-games",` +
			`"referenceExternalTransactionId":"ext-0","roundId":"round-1",` +
			`"walletId":"` + fixedWalletID + `"}`

		if got := cmd.CanonicalPayload(); got != want {
			t.Errorf("CanonicalPayload()\n got %s\nwant %s", got, want)
		}
	})
}

// TestCanonicalPayloadKeysAreSorted reads the keys back in the order they were
// written, so the ordering rule is checked rather than merely implied by the
// literal above.
func TestCanonicalPayloadKeysAreSorted(t *testing.T) {
	t.Parallel()

	cmd := fixedCommand(t)
	cmd.Kind = Rollback
	cmd.ReferenceExternalTransactionID = "ext-0"

	keys := topLevelKeys(t, cmd.CanonicalPayload())
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			t.Errorf("keys are not in ascending order: %q then %q, in %v", keys[i-1], keys[i], keys)
		}
	}
	if len(keys) != 9 {
		t.Errorf("payload has %d keys (%v), want 9", len(keys), keys)
	}
}

// topLevelKeys returns the keys of a JSON object in the order they appear.
func topLevelKeys(t *testing.T, document string) []string {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(document))

	if _, err := dec.Token(); err != nil { // opening brace
		t.Fatalf("reading the opening brace: %v", err)
	}
	var keys []string
	depth := 0
	for dec.More() || depth > 0 {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("reading a token: %v", err)
		}
		switch v := tok.(type) {
		case json.Delim:
			switch v {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		case string:
			if depth == 0 {
				keys = append(keys, v)
				// Skip this key's value, which may itself be an object.
				var discard json.RawMessage
				if err := dec.Decode(&discard); err != nil {
					t.Fatalf("skipping a value: %v", err)
				}
			}
		}
	}
	return keys
}

func TestPayloadHashIsSha256OfTheCanonicalPayload(t *testing.T) {
	t.Parallel()

	cmd := fixedCommand(t)
	got, err := cmd.PayloadHash()
	if err != nil {
		t.Fatalf("PayloadHash: %v", err)
	}

	sum := sha256.Sum256([]byte(cmd.CanonicalPayload()))
	want := PayloadHash(hex.EncodeToString(sum[:]))
	if got != want {
		t.Errorf("PayloadHash() = %s, want %s", got, want)
	}
	if len(string(got)) != 64 {
		t.Errorf("hash is %d characters, want 64", len(string(got)))
	}
}

// TestHashIgnoresWhatIsNotBusiness covers the exclusions: the idempotency key
// itself, and the identifiers this system minted rather than the provider.
func TestHashIgnoresWhatIsNotBusiness(t *testing.T) {
	t.Parallel()

	base := fixedCommand(t)
	original, err := base.PayloadHash()
	if err != nil {
		t.Fatalf("PayloadHash: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Command)
	}{
		// Including the key would make every submission unique and the hash
		// useless for spotting a key reused for a different operation.
		{"idempotency key", func(c *Command) { c.IdempotencyKey = "a-completely-different-key" }},
		{"transaction id", func(c *Command) { c.TransactionID = NewTransactionID() }},
		{"ledger entry id", func(c *Command) { c.LedgerEntryID = NewLedgerEntryID() }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := base
			tc.mutate(&cmd)
			got, err := cmd.PayloadHash()
			if err != nil {
				t.Fatalf("PayloadHash: %v", err)
			}
			if got != original {
				t.Errorf("changing the %s changed the hash", tc.name)
			}
		})
	}
}

// TestHashCoversEveryBusinessField is the other half: anything that describes
// what the provider asked for must change the fingerprint.
func TestHashCoversEveryBusinessField(t *testing.T) {
	t.Parallel()

	base := fixedCommand(t)
	base.Kind = Refund
	base.ReferenceExternalTransactionID = "ext-0"
	original, err := base.PayloadHash()
	if err != nil {
		t.Fatalf("PayloadHash: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Command)
	}{
		{"provider", func(c *Command) { c.Provider = "other-games" }},
		{"external transaction id", func(c *Command) { c.ExternalTransactionID = "ext-2" }},
		{"player", func(c *Command) { c.PlayerID = "player-2" }},
		{"wallet", func(c *Command) { c.WalletID = NewWalletID() }},
		{"round", func(c *Command) { c.RoundID = "round-2" }},
		{"game", func(c *Command) { c.GameID = "game-2" }},
		{"kind", func(c *Command) { c.Kind = Rollback }},
		{"amount", func(c *Command) { c.Money = brl(t, "25.01") }},
		{"currency", func(c *Command) { c.Money = usd(t, "25.00") }},
		{"reference", func(c *Command) { c.ReferenceExternalTransactionID = "ext-9" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := base
			tc.mutate(&cmd)
			got, err := cmd.PayloadHash()
			if err != nil {
				t.Fatalf("PayloadHash: %v", err)
			}
			if got == original {
				t.Errorf("changing the %s left the hash unchanged", tc.name)
			}
		})
	}
}

// TestAbsentReferenceIsOmitted pins that "no reference" has one representation
// rather than competing with an explicit null.
func TestAbsentReferenceIsOmitted(t *testing.T) {
	t.Parallel()

	cmd := fixedCommand(t)
	payload := cmd.CanonicalPayload()

	if strings.Contains(payload, "referenceExternalTransactionId") {
		t.Errorf("an absent reference still appears in %s", payload)
	}
	if strings.Contains(payload, "null") {
		t.Errorf("an absent field was emitted as null in %s", payload)
	}
}

func TestHashIsStableAcrossRepeatedCalls(t *testing.T) {
	t.Parallel()

	cmd := fixedCommand(t)
	first, err := cmd.PayloadHash()
	if err != nil {
		t.Fatalf("PayloadHash: %v", err)
	}
	for range 10 {
		again, err := cmd.PayloadHash()
		if err != nil {
			t.Fatalf("PayloadHash: %v", err)
		}
		if again != first {
			t.Fatalf("hash changed between calls: %s then %s", first, again)
		}
	}
}

// TestHashRefusesAnInvalidCommand keeps a fingerprint from ever being taken
// over an operation that could not be recorded.
func TestHashRefusesAnInvalidCommand(t *testing.T) {
	t.Parallel()

	cmd := fixedCommand(t)
	cmd.RoundID = ""

	if got, err := cmd.PayloadHash(); err == nil {
		t.Errorf("PayloadHash of an invalid command = %s, want an error", got)
	}
}

func TestCanonicalPayloadEscapesStrings(t *testing.T) {
	t.Parallel()

	cmd := fixedCommand(t)
	cmd.GameID = `slots "deluxe" \ edition`

	payload := cmd.CanonicalPayload()
	if !json.Valid([]byte(payload)) {
		t.Fatalf("escaping produced invalid JSON: %s", payload)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got := decoded["gameId"]; got != string(cmd.GameID) {
		t.Errorf("gameId round-tripped as %q, want %q", got, cmd.GameID)
	}
}
