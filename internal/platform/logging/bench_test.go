package logging

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

type valueKey int

func BenchmarkTracingOffLogLine(b *testing.B) {
	log := New(io.Discard, slog.LevelInfo, "app-1")
	ctx := b.Context()
	for key := range valueKey(5) {
		ctx = context.WithValue(ctx, key, "value")
	}
	ctx = With(ctx, Fields{CorrelationID: "corr-1", WalletID: "0192f291-27dd-7c4e-9f3a-5b1d2c3e4f50"})
	b.ReportAllocs()
	for b.Loop() {
		log.LogAttrs(ctx, slog.LevelInfo, "http request",
			slog.String("method", "POST"), slog.String("route", "POST /wagering/transactions"), slog.Int("status", 201))
	}
}
