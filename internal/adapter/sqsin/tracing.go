package sqsin

import (
	"context"
	"maps"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const operationProcess = "process"

const (
	attrResult    = attribute.Key("sqsin.result")
	attrDLQReason = attribute.Key("sqsin.dlq_reason")
)

func (c *Consumer) startProcess(ctx context.Context, msg types.Message) (context.Context, trace.Span) {
	if !c.tracer.Enabled() {
		return c.tracer.Start(ctx, operationProcess)
	}
	ctx = c.tracer.Extract(ctx, messageCarrier(msg.MessageAttributes))
	return c.tracer.Start(ctx, operationProcess+" "+c.destination, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(
		semconv.MessagingSystemAWSSQS,
		semconv.MessagingOperationTypeProcess,
		semconv.MessagingOperationName(operationProcess),
		semconv.MessagingDestinationName(c.destination),
		semconv.MessagingConsumerGroupName(c.cfg.Name),
		semconv.MessagingMessageID(aws.ToString(msg.MessageId)),
	))
}

func (c *Consumer) record(ctx context.Context, result string) {
	c.m.Message(result)
	c.noteResult(ctx, result)
}

func (c *Consumer) noteResult(ctx context.Context, result string) {
	if !c.tracer.Enabled() {
		return
	}
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attrResult.String(result))
	}
}

func (c *Consumer) noteDeadLetter(ctx context.Context, reason string) {
	if !c.tracer.Enabled() {
		return
	}
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attrDLQReason.String(reason))
		tracing.Fail(span, reason)
	}
}

func (c *Consumer) noteRetry(ctx context.Context, cause error) {
	if !c.tracer.Enabled() {
		return
	}
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		tracing.Fail(span, app.ErrorType(cause))
	}
}

type messageCarrier map[string]types.MessageAttributeValue

func (m messageCarrier) Get(key string) string {
	a, ok := m[key]
	if !ok || aws.ToString(a.DataType) != "String" {
		return ""
	}
	return aws.ToString(a.StringValue)
}

func (m messageCarrier) Set(key, value string) {
	m[key] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(value)}
}

func (m messageCarrier) Keys() []string {
	return slices.Collect(maps.Keys(m))
}
