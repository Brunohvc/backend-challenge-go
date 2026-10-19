// Package tracing starts and propagates the spans of the service using only the OpenTelemetry API.
package tracing

import (
	"context"
	"encoding/hex"
	"strings"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

const scope = "github.com/Brunohvc/backend-challenge-go"

const (
	traceParentHeader  = "traceparent"
	traceParentVersion = "00"
)

var (
	w3c      = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	off      = propagation.NewCompositeTextMapPropagator()
	noopSpan = trace.SpanFromContext(context.Background())
)

// Tracer starts the spans of the process; its zero value is off and starts none.
type Tracer struct {
	provider trace.TracerProvider
	tracer   trace.Tracer
}

// NewTracer returns a Tracer that starts its spans on provider.
func NewTracer(provider trace.TracerProvider) Tracer {
	return Tracer{provider: provider, tracer: provider.Tracer(scope)}
}

// Enabled reports whether t starts spans.
func (t Tracer) Enabled() bool { return t.tracer != nil }

// Provider returns the tracer provider of t, nil when t is off.
func (t Tracer) Provider() trace.TracerProvider { return t.provider }

// Propagator returns the W3C trace context and baggage propagator, or one that carries nothing when t is off.
func (t Tracer) Propagator() propagation.TextMapPropagator {
	if !t.Enabled() {
		return off
	}
	return w3c
}

// Start starts a span in ctx, or returns ctx and a span that records nothing when t is off.
func (t Tracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !t.Enabled() {
		return ctx, noopSpan
	}
	return t.tracer.Start(ctx, name, opts...)
}

// Extract returns ctx with the remote span context carrier holds, or ctx itself when t is off.
func (t Tracer) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return t.Propagator().Extract(ctx, carrier)
}

// Child starts a span on t under the span of ctx when that one records, and otherwise starts nothing.
func (t Tracer) Child(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !t.Enabled() || !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, noopSpan
	}
	return t.tracer.Start(ctx, name, opts...)
}

// Fail marks span as failed with errorType, a value of a closed set, as its status and error.type.
func Fail(span trace.Span, errorType string) {
	span.SetStatus(codes.Error, errorType)
	span.SetAttributes(semconv.ErrorTypeKey.String(errorType))
}

// TraceParent returns the W3C traceparent of the recording span of ctx, or "" when none records.
func TraceParent(ctx context.Context) string {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier[traceParentHeader]
}

// SpanContextFromTraceParent returns the remote span context traceparent names, empty when it is not valid.
func SpanContextFromTraceParent(traceparent string) trace.SpanContext {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || parts[0] != traceParentVersion || len(parts[3]) != 2 {
		return trace.SpanContext{}
	}
	traceID, err := trace.TraceIDFromHex(parts[1])
	if err != nil {
		return trace.SpanContext{}
	}
	spanID, err := trace.SpanIDFromHex(parts[2])
	if err != nil {
		return trace.SpanContext{}
	}
	flags, err := hex.DecodeString(parts[3])
	if err != nil {
		return trace.SpanContext{}
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.TraceFlags(flags[0]) & trace.FlagsSampled,
		Remote:     true,
	})
}
