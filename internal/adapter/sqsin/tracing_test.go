package sqsin_test

import (
	"fmt"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
)

const (
	producerTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	processSpan         = "process wager-transactions.fifo"
)

func tracedHarness(t *testing.T) (*harness, *tracetest.SpanRecorder) {
	t.Helper()
	h := newHarness(testConfig())
	tr, rec := tracingtest.New(t)
	h.tracer = tr
	return h, rec
}

func handled(t *testing.T, h *harness, rec *tracetest.SpanRecorder, n int) []sdktrace.ReadOnlySpan {
	t.Helper()
	waitFor(t, fmt.Sprintf("%d process spans", n), func() bool { return len(tracingtest.Named(rec, processSpan)) >= n })
	h.stop(t)
	return tracingtest.Named(rec, processSpan)
}

func TestConsumer_ContinuesTheProducersTrace(t *testing.T) {
	h, rec := tracedHarness(t)
	h.start(t)

	h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil), withAttribute("traceparent", producerTraceParent)))

	span := handled(t, h, rec, 1)[0]
	if span.SpanContext().TraceID().String() != "0af7651916cd43dd8448eb211c80319c" ||
		span.Parent().SpanID().String() != "b7ad6b7169203331" || !span.Parent().IsRemote() {
		t.Errorf("trace %s under %s, want the producer's", span.SpanContext().TraceID(), span.Parent().SpanID())
	}
	if span.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("SpanKind() = %v, want consumer", span.SpanKind())
	}
	want := map[string]string{
		"messaging.system":              "aws_sqs",
		"messaging.operation.type":      "process",
		"messaging.operation.name":      "process",
		"messaging.destination.name":    "wager-transactions.fifo",
		"messaging.consumer.group.name": consumerName,
		"messaging.message.id":          "m1",
		"sqsin.result":                  "processed",
	}
	attrs := tracingtest.Attributes(span)
	for key, value := range want {
		if attrs[key] != value {
			t.Errorf("%s = %q, want %q", key, attrs[key], value)
		}
	}
	for key, value := range attrs {
		if value == tokenA || value == "25.00" {
			t.Errorf("%s = %q carries the token or the amount", key, value)
		}
	}
	if subs := h.sub.all(); len(subs) != 1 || !subs[0].span.Equal(span.SpanContext()) {
		t.Errorf("the message was submitted under %v, want its process span", subs)
	}
}

func TestConsumer_StartsATraceForAnUntracedMessage(t *testing.T) {
	h, rec := tracedHarness(t)
	h.start(t)

	h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil)))

	if span := handled(t, h, rec, 1)[0]; span.Parent().IsValid() {
		t.Errorf("the span is under %s, want a new trace", span.Parent().SpanID())
	}
}

func TestConsumer_ProcessSpanRecordsTheOutcome(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		result    string
		errorType string
		reason    string
	}{
		{"moved to the dead-letter queue", fmt.Errorf("wagering: %w: broken invariant", app.ErrPermanent),
			"dlq", "PROCESSING_FAILED", "PROCESSING_FAILED"},
		{"tried again", fmt.Errorf("%w: connection refused", app.ErrUnavailable), "retry", "unavailable", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, rec := tracedHarness(t)
			h.sub.fn = submitErr(tt.err)
			h.start(t)

			h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil)))

			span := handled(t, h, rec, 1)[0]
			attrs := tracingtest.Attributes(span)
			if attrs["sqsin.result"] != tt.result || attrs["sqsin.dlq_reason"] != tt.reason {
				t.Errorf("result %q, reason %q; want %q, %q", attrs["sqsin.result"], attrs["sqsin.dlq_reason"], tt.result, tt.reason)
			}
			if span.Status().Code != codes.Error || attrs["error.type"] != tt.errorType {
				t.Errorf("status %+v, error.type %q; want an error of type %q", span.Status(), attrs["error.type"], tt.errorType)
			}
		})
	}
}

func TestConsumer_UntracedIgnoresTheProducersTrace(t *testing.T) {
	h := newHarness(testConfig())
	h.start(t)

	h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil), withAttribute("traceparent", producerTraceParent)))

	waitFor(t, "the message handled", func() bool { return slices.Contains(h.m.results(), "processed") })
	h.stop(t)
	if subs := h.sub.all(); len(subs) != 1 || subs[0].span.IsValid() {
		t.Errorf("the message was submitted under %v, want no span context with tracing off", subs)
	}
}

func TestConsumer_UntracedLeavesTheCallersSpanAlone(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		result string
	}{
		{"moved to the dead-letter queue", fmt.Errorf("wagering: %w: broken invariant", app.ErrPermanent), "dlq"},
		{"tried again", fmt.Errorf("%w: connection refused", app.ErrUnavailable), "retry"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, rec := tracingtest.New(t)
			ctx, caller := tracingtest.Root(t.Context(), tr)
			h := newHarness(testConfig())
			h.sub.fn = submitErr(tt.err)
			h.startIn(ctx, t)

			h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil)))

			waitFor(t, "the message "+tt.result, func() bool { return slices.Contains(h.m.results(), tt.result) })
			h.stop(t)
			caller.End()
			span := tracingtest.One(t, rec, "test")
			if attrs := tracingtest.Attributes(span); len(attrs) != 0 || span.Status().Code != codes.Unset {
				t.Errorf("the caller's span got attributes %v and status %+v, want neither with tracing off", attrs, span.Status())
			}
		})
	}
}
