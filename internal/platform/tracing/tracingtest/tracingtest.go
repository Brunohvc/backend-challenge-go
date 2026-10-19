// Package tracingtest records in memory the spans a test produces.
package tracingtest

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

// New returns a Tracer that records every span and the recorder that keeps the spans once they end.
func New(t testing.TB) (tracing.Tracer, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tracing.NewTracer(tp), rec
}

// Root starts a recording span on tr for code that only adds children to one.
func Root(ctx context.Context, tr tracing.Tracer) (context.Context, trace.Span) {
	return tr.Start(ctx, "test")
}

// Named returns the ended spans called name, in the order they ended.
func Named(rec *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	var spans []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == name {
			spans = append(spans, s)
		}
	}
	return spans
}

// One returns the single ended span called name, failing t when there is not exactly one.
func One(t testing.TB, rec *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	spans := Named(rec, name)
	if len(spans) != 1 {
		t.Fatalf("%d spans named %q, want 1; ended: %v", len(spans), name, Names(rec))
	}
	return spans[0]
}

// Names returns the names of the ended spans, in the order they ended.
func Names(rec *tracetest.SpanRecorder) []string {
	ended := rec.Ended()
	names := make([]string, len(ended))
	for i, s := range ended {
		names[i] = s.Name()
	}
	return names
}

// Attributes returns the attributes of s, each value as text.
func Attributes(s sdktrace.ReadOnlySpan) map[string]string {
	attrs := make(map[string]string, len(s.Attributes()))
	for _, kv := range s.Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	return attrs
}

// ChildOf reports whether child was started under parent.
func ChildOf(child, parent sdktrace.ReadOnlySpan) bool {
	return child.Parent().SpanID() == parent.SpanContext().SpanID() &&
		child.SpanContext().TraceID() == parent.SpanContext().TraceID()
}
