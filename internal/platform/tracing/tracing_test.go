package tracing_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const (
	sampledParent   = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	unsampledParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"
)

func TestTracer_ZeroValueIsOff(t *testing.T) {
	var tr tracing.Tracer

	ctx, span := tr.Start(t.Context(), "request")
	defer span.End()

	if tr.Enabled() || span.IsRecording() || trace.SpanContextFromContext(ctx).IsValid() {
		t.Errorf("zero Tracer: enabled = %t, recording = %t, want neither", tr.Enabled(), span.IsRecording())
	}
	if fields := tr.Propagator().Fields(); len(fields) != 0 {
		t.Errorf("Propagator().Fields() = %v, want none", fields)
	}
}

func TestTracer_PropagatesTheW3CContextAndBaggage(t *testing.T) {
	tr, _ := tracingtest.New(t)

	fields := tr.Propagator().Fields()

	slices.Sort(fields)
	if strings.Join(fields, ",") != "baggage,traceparent,tracestate" {
		t.Errorf("Propagator().Fields() = %v, want traceparent, tracestate and baggage", fields)
	}
}

func TestTracer_ContinuesTheRemoteCaller(t *testing.T) {
	tr, rec := tracingtest.New(t)

	ctx := tr.Extract(t.Context(), propagation.MapCarrier{"traceparent": sampledParent})
	_, span := tr.Start(ctx, "request")
	span.End()

	got := tracingtest.One(t, rec, "request")
	if got.SpanContext().TraceID().String() != "0af7651916cd43dd8448eb211c80319c" ||
		got.Parent().SpanID().String() != "b7ad6b7169203331" || !got.Parent().IsRemote() {
		t.Errorf("span trace %s parent %s (remote %t), want the caller's", got.SpanContext().TraceID(),
			got.Parent().SpanID(), got.Parent().IsRemote())
	}
}

func TestChild_OnlyUnderARecordingSpan(t *testing.T) {
	tr, rec := tracingtest.New(t)

	if _, orphan := tr.Child(t.Context(), "orphan"); orphan.IsRecording() || orphan.SpanContext().IsValid() {
		t.Error("Child() without a parent started a span")
	}
	ctx, root := tracingtest.Root(t.Context(), tr)
	_, child := tr.Child(ctx, "child", trace.WithSpanKind(trace.SpanKindClient))
	child.End()
	root.End()

	got := tracingtest.One(t, rec, "child")
	if !tracingtest.ChildOf(got, tracingtest.One(t, rec, "test")) {
		t.Error("child span is not under the root")
	}
	if got.SpanKind() != trace.SpanKindClient {
		t.Errorf("SpanKind() = %v, want client", got.SpanKind())
	}
	if names := tracingtest.Names(rec); len(names) != 2 {
		t.Errorf("ended spans = %v, want only the child and the root", names)
	}
}

func TestChild_NotUnderASpanThatIsNotRecording(t *testing.T) {
	tr, rec := tracingtest.New(t)
	ctx := tr.Extract(t.Context(), propagation.MapCarrier{"traceparent": unsampledParent})
	ctx, parent := tr.Start(ctx, "request")
	defer parent.End()

	_, child := tr.Child(ctx, "child")
	child.End()

	if names := tracingtest.Names(rec); len(names) != 0 {
		t.Errorf("ended spans = %v, want none", names)
	}
}

func TestChild_NotOnATracerThatIsOff(t *testing.T) {
	tr, rec := tracingtest.New(t)
	ctx, root := tracingtest.Root(t.Context(), tr)
	defer root.End()
	var off tracing.Tracer

	childCtx, child := off.Child(ctx, "child")
	child.End()

	if child.IsRecording() || childCtx != ctx {
		t.Error("Child() on a Tracer that is off started a span")
	}
	if names := tracingtest.Names(rec); len(names) != 0 {
		t.Errorf("ended spans = %v, want none", names)
	}
}

type countingProvider struct {
	noop.TracerProvider
	lookups int
}

func (p *countingProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	p.lookups++
	return recordingTracer{provider: p}
}

type recordingTracer struct {
	noop.Tracer
	provider *countingProvider
}

func (t recordingTracer) Start(ctx context.Context, _ string, _ ...trace.SpanStartOption) (context.Context, trace.Span) {
	span := recordingSpan{provider: t.provider}
	return trace.ContextWithSpan(ctx, span), span
}

type recordingSpan struct {
	noop.Span
	provider *countingProvider
}

func (recordingSpan) IsRecording() bool { return true }

func (s recordingSpan) TracerProvider() trace.TracerProvider { return s.provider }

func TestTracer_LooksItsTracerUpOnce(t *testing.T) {
	provider := &countingProvider{}
	tr := tracing.NewTracer(provider)

	ctx, root := tr.Start(t.Context(), "request")
	for range 3 {
		_, child := tr.Child(ctx, "statement")
		child.End()
	}
	root.End()

	if provider.lookups != 1 {
		t.Errorf("the provider was asked for a tracer %d times, want once", provider.lookups)
	}
}

func TestTraceParent_OfTheRecordingSpan(t *testing.T) {
	tr, _ := tracingtest.New(t)
	ctx, span := tracingtest.Root(t.Context(), tr)
	defer span.End()

	got := tracing.TraceParent(ctx)

	sc := span.SpanContext()
	want := "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-01"
	if got != want {
		t.Errorf("TraceParent() = %q, want %q", got, want)
	}
	if back := tracing.SpanContextFromTraceParent(got); !back.Equal(sc.WithRemote(true)) {
		t.Errorf("SpanContextFromTraceParent() = %+v, want %+v", back, sc.WithRemote(true))
	}
}

func TestTraceParent_EmptyWithoutARecordingSpan(t *testing.T) {
	tr, _ := tracingtest.New(t)
	remote := tr.Extract(t.Context(), propagation.MapCarrier{"traceparent": sampledParent})

	for name, ctx := range map[string]context.Context{"no span": t.Context(), "only a remote caller": remote} {
		if got := tracing.TraceParent(ctx); got != "" {
			t.Errorf("TraceParent() with %s = %q, want empty", name, got)
		}
	}
}

func TestSpanContextFromTraceParent_InvalidIsEmpty(t *testing.T) {
	for _, raw := range []string{
		"",
		"00-zz-b7ad6b7169203331-01",
		"00-00000000000000000000000000000000-b7ad6b7169203331-01",
		"00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
		"00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
		"01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-1",
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-zz",
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
	} {
		if sc := tracing.SpanContextFromTraceParent(raw); sc.IsValid() {
			t.Errorf("SpanContextFromTraceParent(%q) is valid, want an empty span context", raw)
		}
	}
}

func TestSpanContextFromTraceParent_KeepsOnlyTheSampledFlag(t *testing.T) {
	tests := map[string]bool{
		sampledParent:   true,
		unsampledParent: false,
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-03": true,
	}
	for raw, sampled := range tests {
		sc := tracing.SpanContextFromTraceParent(raw)
		if !sc.IsValid() || !sc.IsRemote() || sc.IsSampled() != sampled || sc.TraceFlags() & ^trace.FlagsSampled != 0 {
			t.Errorf("SpanContextFromTraceParent(%q) = %+v, want a valid remote context sampled = %t", raw, sc, sampled)
		}
	}
}

func TestFail_SetsTheBoundedErrorType(t *testing.T) {
	tr, rec := tracingtest.New(t)
	_, span := tracingtest.Root(t.Context(), tr)

	tracing.Fail(span, "unavailable")
	span.End()

	got := tracingtest.One(t, rec, "test")
	if got.Status().Code != codes.Error || got.Status().Description != "unavailable" {
		t.Errorf("Status() = %+v, want error unavailable", got.Status())
	}
	if attrs := tracingtest.Attributes(got); attrs["error.type"] != "unavailable" {
		t.Errorf("error.type = %q, want unavailable", attrs["error.type"])
	}
}
