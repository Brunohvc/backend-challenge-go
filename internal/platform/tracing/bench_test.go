package tracing_test

import (
	"context"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

type valueKey int

func requestContext(b *testing.B) context.Context {
	b.Helper()
	ctx := b.Context()
	for key := range valueKey(5) {
		ctx = context.WithValue(ctx, key, "value")
	}
	return ctx
}

func BenchmarkTracingOffChild(b *testing.B) {
	var tr tracing.Tracer
	ctx := requestContext(b)
	b.ReportAllocs()
	for b.Loop() {
		_, span := tr.Child(ctx, "uow.do")
		span.End()
	}
}

func BenchmarkTracingOffStart(b *testing.B) {
	var tr tracing.Tracer
	ctx := requestContext(b)
	b.ReportAllocs()
	for b.Loop() {
		_, span := tr.Start(ctx, "outbox.relay")
		span.End()
	}
}

func BenchmarkTracingOffTraceParent(b *testing.B) {
	ctx := requestContext(b)
	b.ReportAllocs()
	for b.Loop() {
		tracing.TraceParent(ctx)
	}
}
