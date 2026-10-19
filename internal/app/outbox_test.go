package app_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
)

var (
	eventID  = uuid.MustParse("0192f2a0-1b7e-7c11-9a3e-2f0c1d4e5a6b")
	walletID = uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37")
	txID     = "0192f298-345e-7e38-af88-e43f851a819d"

	occurredAt = time.Date(2026, 9, 8, 9, 0, 0, 123_456_000, time.FixedZone("BRT", -3*60*60))
)

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL())
	if err != nil {
		t.Fatalf("FromMinor(%d, BRL) error = %v", minor, err)
	}
	return m
}

func balanceChanged(t *testing.T) event.Envelope {
	t.Helper()
	e, err := event.NewWalletBalanceChanged(
		event.Meta{
			EventID:       eventID,
			WalletID:      walletID,
			CorrelationID: "c-0192f298-345e",
			CausationID:   txID,
			OccurredAt:    occurredAt,
		},
		event.WalletBalanceChangedData{
			WalletID:      walletID.String(),
			TransactionID: txID,
			Direction:     "DEBIT",
			Money:         brl(t, 2500),
			BalanceBefore: brl(t, 100000),
			BalanceAfter:  brl(t, 97500),
			WalletVersion: 2,
		})
	if err != nil {
		t.Fatalf("NewWalletBalanceChanged() error = %v", err)
	}
	return e
}

func TestNewOutboxRecord_SnapshotMatchesEnvelope(t *testing.T) {
	env := balanceChanged(t)

	rec, err := app.NewOutboxRecord(env)
	if err != nil {
		t.Fatalf("NewOutboxRecord() error = %v", err)
	}

	want, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal(envelope) error = %v", err)
	}
	var gotDoc, wantDoc map[string]any
	if err := json.Unmarshal(rec.Payload, &gotDoc); err != nil {
		t.Fatalf("payload is not a JSON object: %v", err)
	}
	if err := json.Unmarshal(want, &wantDoc); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if !reflect.DeepEqual(gotDoc, wantDoc) {
		t.Errorf("payload = %s\nwant the envelope as %s", rec.Payload, want)
	}
	if !bytes.Equal(rec.Payload, want) {
		t.Errorf("payload bytes = %s, want %s", rec.Payload, want)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"EventID", rec.EventID, eventID},
		{"AggregateType", rec.AggregateType, event.AggregateTypeWallet},
		{"AggregateID", rec.AggregateID, walletID.String()},
		{"EventType", rec.EventType, event.TypeWalletBalanceChanged},
		{"EventVersion", rec.EventVersion, env.Version},
		{"CorrelationID", rec.CorrelationID, "c-0192f298-345e"},
		{"CausationID", rec.CausationID, txID},
		{"Attempts", rec.Attempts, 0},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if rec.OccurredAt.Location() != time.UTC {
		t.Errorf("OccurredAt zone = %v, want UTC", rec.OccurredAt.Location())
	}
}

func TestNewOutboxRecord_OccurredAtIsTheInstantOfThePayload(t *testing.T) {
	if occurredAt.Truncate(time.Millisecond).Equal(occurredAt) {
		t.Fatalf("fixture %v has no sub-millisecond digits", occurredAt)
	}
	rec, err := app.NewOutboxRecord(balanceChanged(t))
	if err != nil {
		t.Fatalf("NewOutboxRecord() error = %v", err)
	}

	var doc struct {
		OccurredAt string `json:"occurredAt"`
	}
	if err := json.Unmarshal(rec.Payload, &doc); err != nil {
		t.Fatalf("payload is not a JSON object: %v", err)
	}
	stated, err := time.Parse(time.RFC3339Nano, doc.OccurredAt)
	if err != nil {
		t.Fatalf("payload occurredAt %q does not parse: %v", doc.OccurredAt, err)
	}

	want := time.Date(2026, 9, 8, 12, 0, 0, 123_000_000, time.UTC)
	if !stated.Equal(want) {
		t.Errorf("payload occurredAt = %v, want %v", stated, want)
	}
	if !rec.OccurredAt.Equal(want) {
		t.Errorf("OccurredAt = %v, want %v, the instant the payload states", rec.OccurredAt, want)
	}
}

func TestNewOutboxRecord_EmptyCausationStaysEmpty(t *testing.T) {
	env := balanceChanged(t)
	env.CausationID = ""

	rec, err := app.NewOutboxRecord(env)
	if err != nil {
		t.Fatalf("NewOutboxRecord() error = %v", err)
	}
	if rec.CausationID != "" {
		t.Errorf("CausationID = %q, want empty", rec.CausationID)
	}
}

func TestNewOutboxRecord_RejectsInvalid(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*event.Envelope)
		cause  error
	}{
		{name: "event id is not a uuid", mutate: func(e *event.Envelope) { e.EventID = "not-a-uuid" }},
		{name: "event id is empty", mutate: func(e *event.Envelope) { e.EventID = "" }},
		{name: "event id is nil", mutate: func(e *event.Envelope) { e.EventID = uuid.Nil.String() }},
		{name: "event id is not canonical", mutate: func(e *event.Envelope) { e.EventID = "{" + eventID.String() + "}" }},
		{name: "event id is uppercase", mutate: func(e *event.Envelope) { e.EventID = strings.ToUpper(eventID.String()) }},
		{name: "event type is empty", mutate: func(e *event.Envelope) { e.EventType = "" }},
		{name: "aggregate id is empty", mutate: func(e *event.Envelope) { e.AggregateID = "" }},
		{name: "aggregate id is not a uuid", mutate: func(e *event.Envelope) { e.AggregateID = "wallet-1" }},
		{name: "aggregate id is nil", mutate: func(e *event.Envelope) { e.AggregateID = uuid.Nil.String() }},
		{name: "aggregate id is not canonical", mutate: func(e *event.Envelope) { e.AggregateID = "urn:uuid:" + walletID.String() }},
		{name: "aggregate id is uppercase", mutate: func(e *event.Envelope) { e.AggregateID = strings.ToUpper(walletID.String()) }},
		{name: "correlation id is empty", mutate: func(e *event.Envelope) { e.CorrelationID = "" }},
		{name: "version is zero", mutate: func(e *event.Envelope) { e.Version = 0 }},
		{
			name:   "occurrence time is unset",
			mutate: func(e *event.Envelope) { e.OccurredAt = event.Timestamp{} },
			cause:  event.ErrInvalidTimestamp,
		},
		{name: "data cannot be marshaled", mutate: func(e *event.Envelope) { e.Data = make(chan int) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := balanceChanged(t)
			tt.mutate(&env)

			rec, err := app.NewOutboxRecord(env)
			if !errors.Is(err, app.ErrPermanent) {
				t.Fatalf("NewOutboxRecord() error = %v, want it to wrap app.ErrPermanent", err)
			}
			if tt.cause != nil && !errors.Is(err, tt.cause) {
				t.Errorf("NewOutboxRecord() error = %v, want it to wrap %v", err, tt.cause)
			}
			if !reflect.DeepEqual(rec, app.OutboxRecord{}) {
				t.Errorf("NewOutboxRecord() record = %+v, want the zero record on error", rec)
			}
		})
	}
}
