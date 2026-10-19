package sqsin_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

const exampleBody = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`

const exampleVector = "c0c6bae37f6ceee9f633907ef9c19bdbf43fbf3570e3bf0a66a09dda29ef9437"

func TestParseEnvelope_Example(t *testing.T) {
	env, in, err := sqsin.ParseEnvelope([]byte(exampleBody))
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}

	if env.MessageID != "msg-123" || env.Type != sqsin.MessageType {
		t.Errorf("envelope = %+v, want messageId msg-123 and type %s", env, sqsin.MessageType)
	}
	want := wager.CommandInput{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "provider-a:transaction-123",
		PlayerID:              "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		WalletID:              "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Amount:                "25.00",
		Currency:              "BRL",
	}
	if in != want {
		t.Errorf("CommandInput =\n%+v\nwant\n%+v", in, want)
	}
}

func TestParseEnvelope_Reference(t *testing.T) {
	body := strings.Replace(exampleBody, `"kind": "BET"`, `"kind": "REFUND", "referenceExternalTransactionId": "transaction-100"`, 1)

	_, in, err := sqsin.ParseEnvelope([]byte(body))
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}

	if in.Kind != "REFUND" || in.ReferenceExternalTransactionID != "transaction-100" {
		t.Errorf("kind = %q, reference = %q, want REFUND and transaction-100", in.Kind, in.ReferenceExternalTransactionID)
	}
}

func TestParseEnvelope_Invalid(t *testing.T) {
	replace := func(old, repl string) string {
		if !strings.Contains(exampleBody, old) {
			t.Fatalf("example has no %q", old)
		}
		return strings.Replace(exampleBody, old, repl, 1)
	}

	tests := []struct {
		name       string
		body       string
		wantReason string
	}{
		{"not JSON", `{"messageId":`, "not a JSON object"},
		{"JSON array", `[]`, "not a JSON object"},
		{"trailing data", exampleBody + ` {}`, "not a JSON object"},
		{"trailing brace", exampleBody + `}`, "not a JSON object"},
		{"missing messageId", replace(`"messageId": "msg-123",`, ""), "messageId is required"},
		{"empty messageId", replace(`"messageId": "msg-123"`, `"messageId": ""`), "messageId"},
		{"messageId as number", replace(`"messageId": "msg-123"`, `"messageId": 123`), "messageId must be a string"},
		{"missing type", replace(`"type": "WagerTransactionRequested",`, ""), "type is required"},
		{"wrong type", replace(`"WagerTransactionRequested"`, `"WalletOpened"`), "type"},
		{"missing occurredAt", replace(`"occurredAt": "2026-09-08T12:00:00.000Z",`, ""), "occurredAt is required"},
		{"occurredAt without milliseconds", replace(`2026-09-08T12:00:00.000Z`, `2026-09-08T12:00:00Z`), "occurredAt"},
		{"occurredAt with offset", replace(`2026-09-08T12:00:00.000Z`, `2026-09-08T09:00:00.000-03:00`), "occurredAt"},
		{"occurredAt with comma", replace(`2026-09-08T12:00:00.000Z`, `2026-09-08T12:00:00,000Z`), "occurredAt"},
		{"unknown top-level field", replace(`"messageId": "msg-123",`, `"messageId": "msg-123", "traceId": "t",`), `unknown field "traceId"`},
		{"missing data", `{"messageId":"m","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z"}`, "data is required"},
		{"data not object", `{"messageId":"m","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":[]}`, "data must be an object"},
		{"data null", `{"messageId":"m","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":null}`, "data must be an object"},
		{"unknown data field", replace(`"gameId": "fortune-chimp",`, `"gameId": "fortune-chimp", "currency": "BRL",`), `unknown field "data.currency"`},
		{"amount as number", replace(`"amount": "25.00"`, `"amount": 25.00`), "data.money.amount must be a string"},
		{"kind as null", replace(`"kind": "BET"`, `"kind": null`), "data.kind must be a string"},
		{"money not object", replace(`{ "amount": "25.00", "currency": "BRL" }`, `"25.00 BRL"`), "data.money must be an object"},
		{"unknown money field", replace(`"currency": "BRL"`, `"currency": "BRL", "scale": "2"`), `unknown field "data.money.scale"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := sqsin.ParseEnvelope([]byte(tt.body))

			var invalid *sqsin.InvalidMessageError
			if !errors.As(err, &invalid) {
				t.Fatalf("err = %v, want *InvalidMessageError", err)
			}
			if !strings.Contains(invalid.Reason, tt.wantReason) {
				t.Errorf("Reason = %q, want it to contain %q", invalid.Reason, tt.wantReason)
			}
		})
	}
}

func TestParseEnvelope_ReasonIsSafe(t *testing.T) {
	key := `\u0000evil\u001b[31m` + strings.Repeat("k", 10_000)
	body := strings.Replace(exampleBody, `"messageId": "msg-123",`, `"messageId": "msg-123", "`+key+`": "x",`, 1)

	_, _, err := sqsin.ParseEnvelope([]byte(body))

	var invalid *sqsin.InvalidMessageError
	if !errors.As(err, &invalid) {
		t.Fatalf("err = %v, want *InvalidMessageError", err)
	}
	if len(invalid.Reason) > 300 {
		t.Errorf("len(Reason) = %d, want at most 300", len(invalid.Reason))
	}
	for _, r := range invalid.Reason {
		if r < 0x20 || r == 0x7f {
			t.Fatalf("Reason %q contains control character %U", invalid.Reason, r)
		}
	}
	if !strings.HasPrefix(invalid.Reason, "unknown field ") {
		t.Errorf("Reason = %q, want it to name an unknown field", invalid.Reason)
	}
}

func TestInboxHash_Vector(t *testing.T) {
	env, _, err := sqsin.ParseEnvelope([]byte(exampleBody))
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}

	got, err := sqsin.InboxHash(env)
	if err != nil {
		t.Fatalf("InboxHash: %v", err)
	}

	if got != exampleVector {
		t.Errorf("InboxHash = %s, want %s", got, exampleVector)
	}
}

func TestInboxHash_IgnoresOccurredAtAndMessageID(t *testing.T) {
	resent := strings.NewReplacer(
		`"msg-123"`, `"msg-456"`,
		`2026-09-08T12:00:00.000Z`, `2026-09-08T12:05:00.000Z`,
		`{ "amount": "25.00", "currency": "BRL" }`, `{"currency":"BRL","amount":"25.00"}`,
	).Replace(exampleBody)

	if got := hashOf(t, resent); got != exampleVector {
		t.Errorf("InboxHash of the resent message = %s, want %s", got, exampleVector)
	}
}

func TestInboxHash_CoversEveryDataField(t *testing.T) {
	tests := []struct{ name, old, repl string }{
		{"idempotency key", `"provider-a:transaction-123"`, `"provider-a:transaction-124"`},
		{"amount", `"25.00"`, `"25.01"`},
		{"reference added", `"kind": "BET"`, `"kind": "BET", "referenceExternalTransactionId": "t-1"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hashOf(t, strings.Replace(exampleBody, tt.old, tt.repl, 1)); got == exampleVector {
				t.Errorf("InboxHash did not change when the %s changed", tt.name)
			}
		})
	}
}

func hashOf(t *testing.T, body string) string {
	t.Helper()
	env, _, err := sqsin.ParseEnvelope([]byte(body))
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}
	h, err := sqsin.InboxHash(env)
	if err != nil {
		t.Fatalf("InboxHash: %v", err)
	}
	return h
}

func TestHashEquivalence_HTTPvsSQS(t *testing.T) {
	fromHTTP := wager.CommandInput{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "provider-a:transaction-123",
		PlayerID:              "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		WalletID:              "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Amount:                "25.00",
		Currency:              "BRL",
	}
	_, fromSQS, err := sqsin.ParseEnvelope([]byte(exampleBody))
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}

	httpCmd, err := wager.NewCommand(fromHTTP)
	if err != nil {
		t.Fatalf("NewCommand from HTTP: %v", err)
	}
	sqsCmd, err := wager.NewCommand(fromSQS)
	if err != nil {
		t.Fatalf("NewCommand from SQS: %v", err)
	}

	if httpCmd.PayloadHash() != sqsCmd.PayloadHash() {
		t.Errorf("PayloadHash HTTP = %s, SQS = %s, want equal", httpCmd.PayloadHash(), sqsCmd.PayloadHash())
	}
}
