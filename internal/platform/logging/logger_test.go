package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"testing/slogtest"
	"time"

	"go.opentelemetry.io/otel/trace"
)

var allFields = Fields{
	CorrelationID: "corr-1",
	MessageID:     "msg-1",
	TransactionID: "tx-1",
	WalletID:      "wallet-1",
	ProviderID:    "provider-1",
}

var fieldKeys = []string{"correlationId", "messageId", "transactionId", "walletId", "providerId"}

func newBufferLogger(level slog.Level) (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return New(&buf, level, "replica-1"), &buf
}

func decodeLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log output %q is not a single JSON object: %v", buf.String(), err)
	}
	return line
}

func TestLogger_AddsContextFields(t *testing.T) {
	logger, buf := newBufferLogger(slog.LevelInfo)
	ctx := With(context.Background(), allFields)

	logger.InfoContext(ctx, "wager accepted")

	line := decodeLine(t, buf)
	want := map[string]any{
		"msg":           "wager accepted",
		"level":         "INFO",
		"instance":      "replica-1",
		"correlationId": "corr-1",
		"messageId":     "msg-1",
		"transactionId": "tx-1",
		"walletId":      "wallet-1",
		"providerId":    "provider-1",
	}
	for key, w := range want {
		if got := line[key]; got != w {
			t.Errorf("%s = %v, want %v", key, got, w)
		}
	}
}

func TestLogger_OmitsEmptyFields(t *testing.T) {
	tests := []struct {
		name string
		log  func(*slog.Logger)
		keep string
	}{
		{
			name: "only wallet id in context",
			log: func(l *slog.Logger) {
				l.InfoContext(With(context.Background(), Fields{WalletID: "wallet-1"}), "hello")
			},
			keep: "walletId",
		},
		{
			name: "context without fields",
			log:  func(l *slog.Logger) { l.InfoContext(context.Background(), "hello") },
		},
		{
			name: "call without context",
			log:  func(l *slog.Logger) { l.Info("hello") },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, buf := newBufferLogger(slog.LevelInfo)

			tt.log(logger)

			line := decodeLine(t, buf)
			for _, key := range fieldKeys {
				_, present := line[key]
				if want := key == tt.keep; present != want {
					t.Errorf("key %s present = %t, want %t", key, present, want)
				}
			}
		})
	}
}

func TestLogger_RespectsLevel(t *testing.T) {
	logger, buf := newBufferLogger(slog.LevelWarn)

	logger.Info("filtered out")
	if buf.Len() != 0 {
		t.Fatalf("info record was written at warn level: %s", buf)
	}

	logger.Warn("kept")
	if line := decodeLine(t, buf); line["msg"] != "kept" {
		t.Errorf("msg = %v, want kept", line["msg"])
	}
}

func TestLogger_KeepsContextFieldsThroughWith(t *testing.T) {
	logger, buf := newBufferLogger(slog.LevelInfo)
	ctx := With(context.Background(), Fields{CorrelationID: "corr-1"})

	logger.With("component", "consumer").InfoContext(ctx, "hello")

	line := decodeLine(t, buf)
	if line["correlationId"] != "corr-1" {
		t.Errorf("correlationId = %v, want corr-1", line["correlationId"])
	}
	if line["component"] != "consumer" {
		t.Errorf("component = %v, want consumer", line["component"])
	}
	if line["instance"] != "replica-1" {
		t.Errorf("instance = %v, want replica-1", line["instance"])
	}
}

func TestLogger_KeepsContextFieldsThroughWithGroup(t *testing.T) {
	logger, buf := newBufferLogger(slog.LevelInfo)
	ctx := With(context.Background(), Fields{CorrelationID: "corr-1"})

	logger.WithGroup("req").InfoContext(ctx, "hello", "path", "/wagers")

	line := decodeLine(t, buf)
	group, ok := line["req"].(map[string]any)
	if !ok {
		t.Fatalf("req = %v, want a JSON object", line["req"])
	}
	if group["correlationId"] != "corr-1" {
		t.Errorf("req.correlationId = %v, want corr-1", group["correlationId"])
	}
	if group["path"] != "/wagers" {
		t.Errorf("req.path = %v, want /wagers", group["path"])
	}
	if line["instance"] != "replica-1" {
		t.Errorf("instance = %v, want it to stay at the top level", line["instance"])
	}
}

func TestLogger_WritesDurationsAsText(t *testing.T) {
	logger, buf := newBufferLogger(slog.LevelInfo)

	logger.With("interval", 500*time.Millisecond).
		WithGroup("retry").
		Info("hello", "retryIn", 2*time.Second, slog.Duration("lease", 90*time.Second))

	line := decodeLine(t, buf)
	if got := line["interval"]; got != "500ms" {
		t.Errorf("interval = %v, want 500ms", got)
	}
	group, ok := line["retry"].(map[string]any)
	if !ok {
		t.Fatalf("retry = %v, want a JSON object", line["retry"])
	}
	for key, want := range map[string]string{"retryIn": "2s", "lease": "1m30s"} {
		if got := group[key]; got != want {
			t.Errorf("retry.%s = %v, want %s", key, got, want)
		}
	}
}

func TestLogger_KeepsOtherNumbers(t *testing.T) {
	logger, buf := newBufferLogger(slog.LevelInfo)

	logger.Info("hello", slog.Int64("durationMs", 7))

	if got := decodeLine(t, buf)["durationMs"]; got != float64(7) {
		t.Errorf("durationMs = %v (%T), want the number 7", got, got)
	}
}

func TestWith_MergesWithoutClearing(t *testing.T) {
	first := With(context.Background(), Fields{CorrelationID: "corr-1", WalletID: "wallet-1"})

	second := With(first, Fields{TransactionID: "tx-1"})

	want := Fields{CorrelationID: "corr-1", WalletID: "wallet-1", TransactionID: "tx-1"}
	if got := FieldsFrom(second); got != want {
		t.Errorf("FieldsFrom(second) = %+v, want %+v", got, want)
	}
}

func TestWith_NonEmptyFieldsReplaceExisting(t *testing.T) {
	first := With(context.Background(), Fields{CorrelationID: "corr-1", WalletID: "wallet-1"})

	second := With(first, Fields{WalletID: "wallet-2"})

	want := Fields{CorrelationID: "corr-1", WalletID: "wallet-2"}
	if got := FieldsFrom(second); got != want {
		t.Errorf("FieldsFrom(second) = %+v, want %+v", got, want)
	}
}

func TestWith_DoesNotChangeParentContext(t *testing.T) {
	parent := With(context.Background(), Fields{CorrelationID: "corr-1"})

	_ = With(parent, Fields{CorrelationID: "corr-2", TransactionID: "tx-1"})

	want := Fields{CorrelationID: "corr-1"}
	if got := FieldsFrom(parent); got != want {
		t.Errorf("FieldsFrom(parent) = %+v, want %+v", got, want)
	}
}

func TestFieldsFrom_EmptyContext(t *testing.T) {
	if got := FieldsFrom(context.Background()); got != (Fields{}) {
		t.Errorf("FieldsFrom(empty) = %+v, want zero Fields", got)
	}
}

func TestLogger_AddsTheActiveSpan(t *testing.T) {
	logger, buf := newBufferLogger(slog.LevelInfo)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80, 0x31, 0x9c},
		SpanID:     trace.SpanID{0xb7, 0xad, 0x6b, 0x71, 0x69, 0x20, 0x33, 0x31},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(With(context.Background(), Fields{CorrelationID: "corr-1"}), sc)

	logger.InfoContext(ctx, "wager accepted")

	line := decodeLine(t, buf)
	want := map[string]any{
		"traceId":       "0af7651916cd43dd8448eb211c80319c",
		"spanId":        "b7ad6b7169203331",
		"correlationId": "corr-1",
	}
	for key, w := range want {
		if got := line[key]; got != w {
			t.Errorf("%s = %v, want %v", key, got, w)
		}
	}
}

func TestLogger_NoSpanNoTraceFields(t *testing.T) {
	logger, buf := newBufferLogger(slog.LevelInfo)

	logger.InfoContext(context.Background(), "hello")

	line := decodeLine(t, buf)
	for _, key := range []string{"traceId", "spanId"} {
		if _, present := line[key]; present {
			t.Errorf("key %s present without a span", key)
		}
	}
}

func TestContextHandler_Conformance(t *testing.T) {
	var buf bytes.Buffer
	newHandler := func(*testing.T) slog.Handler {
		buf.Reset()
		return contextHandler{inner: slog.NewJSONHandler(&buf, nil)}
	}
	result := func(t *testing.T) map[string]any {
		t.Helper()
		return decodeLine(t, &buf)
	}

	slogtest.Run(t, newHandler, result)
}
