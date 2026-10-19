package wager_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

const (
	readmeVector        = "629836932b79106b99523d06a1e7fa80689b0ea1e1c47aa3f0a5a2c87d0c4344"
	readmeCanonicalJSON = `{"externalTransactionId":"transaction-123","gameId":"fortune-chimp","kind":"BET",` +
		`"money":{"amount":"25.00","currency":"BRL"},` +
		`"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","providerId":"provider-a",` +
		`"roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}`
)

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func newCommand(t *testing.T, in wager.CommandInput) wager.Command {
	t.Helper()
	cmd, err := wager.NewCommand(in)
	if err != nil {
		t.Fatalf("NewCommand() error = %v", err)
	}
	return cmd
}

func TestPayloadHash_Vector(t *testing.T) {
	if got := sha256Hex(readmeCanonicalJSON); got != readmeVector {
		t.Fatalf("sha256 of the canonical JSON = %s, want %s: the test fixture is wrong", got, readmeVector)
	}
	if got := newCommand(t, betInput()).PayloadHash(); got != readmeVector {
		t.Errorf("PayloadHash() = %s, want %s", got, readmeVector)
	}
}

func TestPayloadHash_ExcludesIdempotencyKey(t *testing.T) {
	first := newCommand(t, betInput())
	second := newCommand(t, edited(func(in *wager.CommandInput) { in.IdempotencyKey = "another-key" }))
	if first.PayloadHash() != second.PayloadHash() {
		t.Errorf("hash changed with the idempotency key: %s vs %s", first.PayloadHash(), second.PayloadHash())
	}
}

func TestPayloadHash_ChangesWithAnyBusinessField(t *testing.T) {
	withReference := func(in *wager.CommandInput) {
		in.Kind, in.Amount, in.ReferenceExternalTransactionID = "WIN", "25.00", "transaction-122"
	}
	variants := []struct {
		name string
		edit func(*wager.CommandInput)
	}{
		{"providerId", func(in *wager.CommandInput) { in.ProviderID = "provider-b" }},
		{"externalTransactionId", func(in *wager.CommandInput) { in.ExternalTransactionID = "transaction-124" }},
		{"playerId", func(in *wager.CommandInput) { in.PlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a2" }},
		{"walletId", func(in *wager.CommandInput) { in.WalletID = "0192f291-27dd-7d3f-8071-5f8685deef38" }},
		{"roundId", func(in *wager.CommandInput) { in.RoundID = "round-988" }},
		{"gameId", func(in *wager.CommandInput) { in.GameID = "fortune-tiger" }},
		{"kind", func(in *wager.CommandInput) { in.Kind = "WIN" }},
		{"amount", func(in *wager.CommandInput) { in.Amount = "25.01" }},
		{"currency", func(in *wager.CommandInput) { in.Currency = "USD" }},
		{"reference", withReference},
		{"another reference", func(in *wager.CommandInput) {
			withReference(in)
			in.ReferenceExternalTransactionID = "transaction-121"
		}},
	}

	original := newCommand(t, betInput()).PayloadHash()
	seen := map[string]string{original: "original"}
	for _, v := range variants {
		got := newCommand(t, edited(v.edit)).PayloadHash()
		if other, dup := seen[got]; dup {
			t.Errorf("changing %s gave the same hash as %s (%s)", v.name, other, got)
		}
		seen[got] = v.name
	}
}

func TestPayloadHash_OmitsAbsentReference(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{
			"externalTransactionId": "transaction-123",
			"gameId":                "fortune-chimp",
			"kind":                  "WIN",
			"money":                 map[string]any{"amount": "25.00", "currency": "BRL"},
			"playerId":              playerUUID,
			"providerId":            "provider-a",
			"roundId":               "round-987",
			"walletId":              walletUUID,
		}
	}
	mustHash := func(t *testing.T, v map[string]any) string {
		t.Helper()
		h, err := wager.CanonicalHash(v)
		if err != nil {
			t.Fatalf("CanonicalHash() error = %v", err)
		}
		return h
	}
	win := func(reference string) wager.Command {
		return newCommand(t, edited(func(in *wager.CommandInput) {
			in.Kind, in.ReferenceExternalTransactionID = "WIN", reference
		}))
	}

	t.Run("absent", func(t *testing.T) {
		if got, want := win("").PayloadHash(), mustHash(t, base()); got != want {
			t.Errorf("PayloadHash() = %s, want the hash without the key, %s", got, want)
		}
		withEmpty := base()
		withEmpty["referenceExternalTransactionId"] = ""
		if got := win("").PayloadHash(); got == mustHash(t, withEmpty) {
			t.Error("PayloadHash() equals the hash with an empty reference key, want the key left out")
		}
	})
	t.Run("present", func(t *testing.T) {
		with := base()
		with["referenceExternalTransactionId"] = "transaction-122"
		if got, want := win("transaction-122").PayloadHash(), mustHash(t, with); got != want {
			t.Errorf("PayloadHash() = %s, want the hash with the key, %s", got, want)
		}
	})
}

func TestPayloadHash_IsLowercaseHex(t *testing.T) {
	got := newCommand(t, betInput()).PayloadHash()
	if len(got) != 64 || strings.Trim(got, "0123456789abcdef") != "" {
		t.Errorf("PayloadHash() = %q, want 64 lowercase hex digits", got)
	}
}

func TestCanonicalHash(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]any
		json string
	}{
		{
			name: "HTML characters stay literal",
			in:   map[string]any{"k": "a<b>&c"},
			json: `{"k":"a<b>&c"}`,
		},
		{
			name: "non-ASCII text stays UTF-8",
			in:   map[string]any{"k": "ação"},
			json: `{"k":"ação"}`,
		},
		{
			name: "keys are sorted at every level",
			in:   map[string]any{"b": 1, "a": map[string]any{"d": true, "c": nil}},
			json: `{"a":{"c":null,"d":true},"b":1}`,
		},
		{
			name: "no trailing newline and no whitespace",
			in:   map[string]any{"a": []string{"x", "y"}},
			json: `{"a":["x","y"]}`,
		},
		{
			name: "empty object",
			in:   map[string]any{},
			json: `{}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := wager.CanonicalHash(tc.in)
			if err != nil {
				t.Fatalf("CanonicalHash() error = %v", err)
			}
			if want := sha256Hex(tc.json); got != want {
				t.Errorf("CanonicalHash() = %s, want the SHA-256 of %s, %s", got, tc.json, want)
			}
		})
	}
}

func TestCanonicalHash_NoHTMLEscape(t *testing.T) {
	got, err := wager.CanonicalHash(map[string]any{"k": "a<b"})
	if err != nil {
		t.Fatalf("CanonicalHash() error = %v", err)
	}
	if want := sha256Hex(`{"k":"a<b"}`); got != want {
		t.Errorf("CanonicalHash() = %s, want %s", got, want)
	}
	escaped, err := json.Marshal(map[string]any{"k": "a<b"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if got == sha256Hex(string(escaped)) {
		t.Errorf("CanonicalHash() hashed the HTML-escaped form %s", escaped)
	}
}

func TestCanonicalHash_UnencodableValue(t *testing.T) {
	got, err := wager.CanonicalHash(map[string]any{"k": make(chan int)})
	var unsupported *json.UnsupportedTypeError
	if !errors.As(err, &unsupported) {
		t.Fatalf("CanonicalHash() error = %v, want a *json.UnsupportedTypeError", err)
	}
	if got != "" {
		t.Errorf("CanonicalHash() = %q with an error, want empty", got)
	}
}
