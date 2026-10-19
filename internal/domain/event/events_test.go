package event_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
)

var (
	eventID  = uuid.MustParse("0192f2a0-1b7e-7c11-9a3e-2f0c1d4e5a6b")
	walletID = uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37")

	txID     = "0192f298-345e-7e38-af88-e43f851a819d"
	playerID = "0192f291-27dd-7d3f-8071-5f8685deef01"

	occurredAt = time.Date(2026, 9, 8, 12, 0, 0, 123_000_000, time.UTC)
)

func validMeta() event.Meta {
	return event.Meta{
		EventID:       eventID,
		WalletID:      walletID,
		CorrelationID: "c-0192f298-345e",
		CausationID:   txID,
		OccurredAt:    occurredAt,
	}
}

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL())
	if err != nil {
		t.Fatalf("FromMinor(%d, BRL) error = %v", minor, err)
	}
	return m
}

func balanceChanged(t *testing.T) event.WalletBalanceChangedData {
	t.Helper()
	return event.WalletBalanceChangedData{
		WalletID:      walletID.String(),
		TransactionID: txID,
		Direction:     "DEBIT",
		Money:         brl(t, 2500),
		BalanceBefore: brl(t, 100000),
		BalanceAfter:  brl(t, 97500),
		WalletVersion: 2,
	}
}

func processed(t *testing.T) event.WagerTransactionProcessedData {
	t.Helper()
	return event.WagerTransactionProcessedData{
		TransactionID:         txID,
		WalletID:              walletID.String(),
		PlayerID:              playerID,
		Kind:                  "BET",
		Money:                 brl(t, 2500),
		BalanceAfter:          brl(t, 97500),
		WalletVersion:         2,
		ProviderID:            "provider-a",
		ExternalTransactionID: "ext-1",
		RoundID:               "round-1",
		GameID:                "game-1",
	}
}

func rejected(t *testing.T) event.WagerTransactionRejectedData {
	t.Helper()
	return event.WagerTransactionRejectedData{
		TransactionID:         txID,
		ProviderID:            "provider-a",
		ExternalTransactionID: "ext-1",
		WalletID:              walletID.String(),
		Kind:                  "BET",
		Money:                 brl(t, 2500),
		FailureCode:           "INSUFFICIENT_FUNDS",
	}
}

func pendingReference() event.WagerTransactionPendingReferenceData {
	return event.WagerTransactionPendingReferenceData{
		TransactionID:                  txID,
		ProviderID:                     "provider-a",
		ExternalTransactionID:          "ext-2",
		WalletID:                       walletID.String(),
		Kind:                           "REFUND",
		ReferenceExternalTransactionID: "ext-1",
		NextAttemptAt:                  event.Timestamp(occurredAt.Add(time.Second)),
		ExpiresAt:                      event.Timestamp(occurredAt.Add(time.Minute)),
	}
}

func constructors(t *testing.T) []struct {
	name      string
	eventType string
	build     func(event.Meta) (event.Envelope, error)
} {
	t.Helper()
	return []struct {
		name      string
		eventType string
		build     func(event.Meta) (event.Envelope, error)
	}{
		{
			name:      "WagerTransactionProcessed",
			eventType: event.TypeWagerTransactionProcessed,
			build: func(m event.Meta) (event.Envelope, error) {
				return event.NewWagerTransactionProcessed(m, processed(t))
			},
		},
		{
			name:      "WagerTransactionRejected",
			eventType: event.TypeWagerTransactionRejected,
			build: func(m event.Meta) (event.Envelope, error) {
				return event.NewWagerTransactionRejected(m, rejected(t))
			},
		},
		{
			name:      "WalletBalanceChanged",
			eventType: event.TypeWalletBalanceChanged,
			build: func(m event.Meta) (event.Envelope, error) {
				return event.NewWalletBalanceChanged(m, balanceChanged(t))
			},
		},
		{
			name:      "WagerTransactionPendingReference",
			eventType: event.TypeWagerTransactionPendingReference,
			build: func(m event.Meta) (event.Envelope, error) {
				return event.NewWagerTransactionPendingReference(m, pendingReference())
			},
		},
	}
}

func marshalObject(t *testing.T, v any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("Unmarshal(%s) error = %v", raw, err)
	}
	return obj
}

func TestWalletBalanceChanged_Golden(t *testing.T) {
	t.Parallel()

	env, err := event.NewWalletBalanceChanged(validMeta(), balanceChanged(t))
	if err != nil {
		t.Fatalf("NewWalletBalanceChanged() error = %v", err)
	}
	got, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	golden, err := os.ReadFile("testdata/wallet_balance_changed.golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var want bytes.Buffer
	if err := json.Compact(&want, golden); err != nil {
		t.Fatalf("compact golden: %v", err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Errorf("event JSON differs from the golden file\n got: %s\nwant: %s", got, want.Bytes())
	}
}

func TestConstructors_SetTypeAndVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range constructors(t) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, err := tc.build(validMeta())
			if err != nil {
				t.Fatalf("constructor error = %v", err)
			}
			if env.EventType != tc.eventType {
				t.Errorf("EventType = %q, want %q", env.EventType, tc.eventType)
			}
			if env.Version != 1 {
				t.Errorf("Version = %d, want 1", env.Version)
			}
			if env.EventID != eventID.String() {
				t.Errorf("EventID = %q, want %q", env.EventID, eventID)
			}
			if env.AggregateID != walletID.String() {
				t.Errorf("AggregateID = %q, want the wallet id %q", env.AggregateID, walletID)
			}
			if env.CorrelationID != "c-0192f298-345e" {
				t.Errorf("CorrelationID = %q", env.CorrelationID)
			}
			if env.CausationID != txID {
				t.Errorf("CausationID = %q, want %q", env.CausationID, txID)
			}
			if got := time.Time(env.OccurredAt); !got.Equal(occurredAt) {
				t.Errorf("OccurredAt = %v, want %v", got, occurredAt)
			}
			if env.Data == nil {
				t.Error("Data is nil")
			}
		})
	}
}

func TestConstructors_NormalizeOccurredAtToUTC(t *testing.T) {
	t.Parallel()

	m := validMeta()
	m.OccurredAt = occurredAt.In(time.FixedZone("BRT", -3*60*60))

	for _, tc := range constructors(t) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, err := tc.build(m)
			if err != nil {
				t.Fatalf("constructor error = %v", err)
			}
			got := time.Time(env.OccurredAt)
			if got.Location() != time.UTC {
				t.Errorf("OccurredAt location = %v, want UTC", got.Location())
			}
			if !got.Equal(occurredAt) {
				t.Errorf("OccurredAt = %v, want the same instant as %v", got, occurredAt)
			}
		})
	}
}

func TestConstructors_RejectInvalidMeta(t *testing.T) {
	t.Parallel()

	invalid := []struct {
		name   string
		mutate func(*event.Meta)
	}{
		{"nil event id", func(m *event.Meta) { m.EventID = uuid.Nil }},
		{"nil wallet id", func(m *event.Meta) { m.WalletID = uuid.Nil }},
		{"empty correlation id", func(m *event.Meta) { m.CorrelationID = "" }},
		{"zero occurred at", func(m *event.Meta) { m.OccurredAt = time.Time{} }},
	}

	for _, ctor := range constructors(t) {
		for _, tc := range invalid {
			t.Run(ctor.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				m := validMeta()
				tc.mutate(&m)

				env, err := ctor.build(m)
				if !errors.Is(err, event.ErrInvalidMeta) {
					t.Fatalf("error = %v, want ErrInvalidMeta", err)
				}
				if env != (event.Envelope{}) {
					t.Errorf("envelope = %+v, want the zero Envelope on error", env)
				}
			})
		}
	}
}

func TestConstructors_AcceptMissingCausationID(t *testing.T) {
	t.Parallel()

	m := validMeta()
	m.CausationID = ""

	for _, tc := range constructors(t) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, err := tc.build(m)
			if err != nil {
				t.Fatalf("constructor error = %v", err)
			}
			obj := marshalObject(t, env)
			if _, ok := obj["causationId"]; ok {
				t.Errorf("causationId present in %v, want it omitted", obj)
			}
		})
	}
}

func TestTimestamp_UTCMillis(t *testing.T) {
	t.Parallel()

	brt := time.FixedZone("BRT", -3*60*60)

	tests := []struct {
		name string
		at   time.Time
		want string
	}{
		{
			name: "offset time is rendered in UTC",
			at:   time.Date(2026, 9, 8, 9, 0, 0, 123_000_000, brt),
			want: `"2026-09-08T12:00:00.123Z"`,
		},
		{
			name: "whole second keeps three decimals",
			at:   time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
			want: `"2026-09-08T12:00:00.000Z"`,
		},
		{
			name: "sub-millisecond digits are truncated",
			at:   time.Date(2026, 9, 8, 12, 0, 0, 123_999_999, time.UTC),
			want: `"2026-09-08T12:00:00.123Z"`,
		},
		{
			name: "offset can cross the day boundary",
			at:   time.Date(2026, 9, 8, 22, 30, 0, 5_000_000, brt),
			want: `"2026-09-09T01:30:00.005Z"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := json.Marshal(event.Timestamp(tc.at))
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("Marshal() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestTimestamp_RejectsUnrepresentableTimes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		at   time.Time
	}{
		{"zero", time.Time{}},
		{"before year 0", time.Date(-1, 12, 31, 23, 59, 59, 0, time.UTC)},
		{"after year 9999", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := json.Marshal(event.Timestamp(tc.at))
			if !errors.Is(err, event.ErrInvalidTimestamp) {
				t.Errorf("Marshal() = %s, %v; want ErrInvalidTimestamp", got, err)
			}
		})
	}
}

func TestProcessedData_OmitsExternalFieldsForOpening(t *testing.T) {
	t.Parallel()

	opening := event.WagerTransactionProcessedData{
		TransactionID: txID,
		WalletID:      walletID.String(),
		PlayerID:      playerID,
		Kind:          "OPENING",
		Money:         brl(t, 100000),
		BalanceAfter:  brl(t, 100000),
		WalletVersion: 1,
	}

	obj := marshalObject(t, opening)

	for _, key := range []string{"providerId", "externalTransactionId", "roundId", "gameId", "referenceTransactionId"} {
		if _, ok := obj[key]; ok {
			t.Errorf("key %q present in %v, want it omitted", key, obj)
		}
	}
	for _, key := range []string{"transactionId", "walletId", "playerId", "kind", "money", "balanceAfter", "walletVersion"} {
		if _, ok := obj[key]; !ok {
			t.Errorf("key %q missing from %v", key, obj)
		}
	}
}

func TestProcessedData_IncludesExternalFieldsWhenSet(t *testing.T) {
	t.Parallel()

	data := processed(t)
	data.Kind = "REFUND"
	data.ReferenceTransactionID = "0192f298-345e-7e38-af88-e43f851a8100"

	obj := marshalObject(t, data)

	want := map[string]string{
		"providerId":             "provider-a",
		"externalTransactionId":  "ext-1",
		"roundId":                "round-1",
		"gameId":                 "game-1",
		"referenceTransactionId": "0192f298-345e-7e38-af88-e43f851a8100",
	}
	for key, value := range want {
		if got := obj[key]; got != value {
			t.Errorf("%s = %v, want %q", key, got, value)
		}
	}
}

func TestData_ZeroMoneyIsNotOmitted(t *testing.T) {
	t.Parallel()

	data := processed(t)
	data.Kind = "LOSS"
	data.Money = brl(t, 0)
	data.BalanceAfter = brl(t, 0)

	obj := marshalObject(t, data)

	for _, key := range []string{"money", "balanceAfter"} {
		got, ok := obj[key].(map[string]any)
		if !ok {
			t.Fatalf("%s = %v, want an object", key, obj[key])
		}
		if got["amount"] != "0.00" || got["currency"] != "BRL" {
			t.Errorf("%s = %v, want 0.00 BRL", key, got)
		}
	}
}

func TestRejectedData_JSONFieldNames(t *testing.T) {
	t.Parallel()

	env, err := event.NewWagerTransactionRejected(validMeta(), rejected(t))
	if err != nil {
		t.Fatalf("NewWagerTransactionRejected() error = %v", err)
	}

	data, ok := marshalObject(t, env)["data"].(map[string]any)
	if !ok {
		t.Fatal("data is not an object")
	}
	for _, key := range []string{"transactionId", "providerId", "externalTransactionId", "walletId", "kind", "money", "failureCode"} {
		if _, ok := data[key]; !ok {
			t.Errorf("key %q missing from %v", key, data)
		}
	}
	if len(data) != 7 {
		t.Errorf("data has %d keys, want 7: %v", len(data), data)
	}
}

func TestPendingReferenceData_JSONFieldNames(t *testing.T) {
	t.Parallel()

	env, err := event.NewWagerTransactionPendingReference(validMeta(), pendingReference())
	if err != nil {
		t.Fatalf("NewWagerTransactionPendingReference() error = %v", err)
	}

	data, ok := marshalObject(t, env)["data"].(map[string]any)
	if !ok {
		t.Fatal("data is not an object")
	}
	want := map[string]any{
		"transactionId":                  txID,
		"providerId":                     "provider-a",
		"externalTransactionId":          "ext-2",
		"walletId":                       walletID.String(),
		"kind":                           "REFUND",
		"referenceExternalTransactionId": "ext-1",
		"nextAttemptAt":                  "2026-09-08T12:00:01.123Z",
		"expiresAt":                      "2026-09-08T12:01:00.123Z",
	}
	for key, value := range want {
		if data[key] != value {
			t.Errorf("%s = %v, want %v", key, data[key], value)
		}
	}
	if len(data) != len(want) {
		t.Errorf("data has %d keys, want %d: %v", len(data), len(want), data)
	}
}
