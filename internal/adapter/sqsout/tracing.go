package sqsout

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const operationSend = "send"

const (
	attrEventID   = attribute.Key("outbox.event_id")
	attrEventType = attribute.Key("outbox.event_type")
)

func (p *Publisher) startSends(ctx context.Context, batch []app.OutboxRecord) []trace.Span {
	if !p.tracer.Enabled() {
		return nil
	}
	spans := make([]trace.Span, len(batch))
	for i, rec := range batch {
		spans[i] = p.startSend(ctx, rec)
	}
	return spans
}

func (p *Publisher) startSend(ctx context.Context, rec app.OutboxRecord) trace.Span {
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemAWSSQS,
			semconv.MessagingOperationTypeSend,
			semconv.MessagingOperationName(operationSend),
			semconv.MessagingDestinationName(p.destination),
			attrEventID.String(rec.EventID.String()),
			attrEventType.String(rec.EventType),
		),
	}
	parent := ctx
	if sc := tracing.SpanContextFromTraceParent(rec.TraceParent); sc.IsValid() {
		parent = trace.ContextWithRemoteSpanContext(ctx, sc)
		if trace.SpanContextFromContext(ctx).IsValid() {
			opts = append(opts, trace.WithLinks(trace.LinkFromContext(ctx)))
		}
	}
	_, span := p.tracer.Start(parent, operationSend+" "+p.destination, opts...)
	return span
}

func traceParentOf(ctx context.Context, spans []trace.Span, i int) string {
	if spans == nil {
		return ""
	}
	return tracing.TraceParent(trace.ContextWithSpan(ctx, spans[i]))
}

func endSends(spans []trace.Span, batch []app.OutboxRecord, results map[uuid.UUID]error, messageIDs map[string]string) {
	for i, span := range spans {
		id := batch[i].EventID
		if err := results[id]; err != nil {
			tracing.Fail(span, sendErrorType(err))
		} else if messageID := messageIDs[id.String()]; messageID != "" {
			span.SetAttributes(semconv.MessagingMessageID(messageID))
		}
		span.End()
	}
}

func sendErrorType(err error) string {
	var r *refusedError
	switch {
	case errors.As(err, &r) && r.code != "":
		return r.code
	case errors.Is(err, errNoResult):
		return "no_result"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	return "send_failed"
}
