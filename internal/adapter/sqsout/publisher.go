// Package sqsout publishes the events of the outbox to the events FIFO queue.
package sqsout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/awsclient"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const maxBatchEntries = 10

// The message attributes every event carries.
const (
	EventTypeAttribute     = "eventType"
	EventVersionAttribute  = "eventVersion"
	CorrelationIDAttribute = "correlationId"
)

// TraceParentAttribute carries the W3C trace context an event was published under, when traced.
const TraceParentAttribute = "traceparent"

var errNoResult = errors.New("sqsout: SQS did not report the outcome of the event")

// Client is the part of the SQS client the publisher uses.
type Client interface {
	SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

// Publisher sends outbox records to the events queue.
type Publisher struct {
	client      Client
	queueURL    string
	destination string
	tracer      tracing.Tracer
	log         *slog.Logger
}

// NewPublisher returns a publisher to the events queue of q that traces each send on tr.
func NewPublisher(c Client, q awsclient.Queues, tr tracing.Tracer, log *slog.Logger) *Publisher {
	if log == nil {
		log = slog.Default()
	}
	return &Publisher{client: c, queueURL: q.Events, destination: path.Base(q.Events), tracer: tr, log: log}
}

// Publish sends recs to the events queue and reports the outcome of each by its EventID.
func (p *Publisher) Publish(ctx context.Context, recs []app.OutboxRecord) map[uuid.UUID]error {
	results := make(map[uuid.UUID]error, len(recs))
	for start := 0; start < len(recs); start += maxBatchEntries {
		if err := ctx.Err(); err != nil {
			rest := recs[start:]
			err = fmt.Errorf("sqsout: not sent: %w", err)
			p.log.WarnContext(ctx, "events not sent", "events", len(rest), "error", err)
			fail(results, rest, err)
			break
		}
		p.send(ctx, recs[start:min(start+maxBatchEntries, len(recs))], results)
	}
	return results
}

func (p *Publisher) send(ctx context.Context, batch []app.OutboxRecord, results map[uuid.UUID]error) {
	spans := p.startSends(ctx, batch)
	messageIDs := p.sendBatch(ctx, batch, spans, results)
	endSends(spans, batch, results, messageIDs)
}

func (p *Publisher) sendBatch(ctx context.Context, batch []app.OutboxRecord, spans []trace.Span, results map[uuid.UUID]error) map[string]string {
	entries := make([]types.SendMessageBatchRequestEntry, len(batch))
	for i, rec := range batch {
		entries[i] = entry(rec, traceParentOf(ctx, spans, i))
	}
	out, err := p.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(p.queueURL),
		Entries:  entries,
	})
	if err != nil {
		err = fmt.Errorf("sqsout: send a batch of %d events: %w", len(batch), err)
		p.log.WarnContext(ctx, "event batch not sent", "events", len(batch), "error", err)
		fail(results, batch, err)
		return nil
	}

	outcome := make(map[string]error, len(batch))
	messageIDs := make(map[string]string, len(out.Successful))
	for _, s := range out.Successful {
		outcome[aws.ToString(s.Id)] = nil
		messageIDs[aws.ToString(s.Id)] = aws.ToString(s.MessageId)
	}
	for _, f := range out.Failed {
		outcome[aws.ToString(f.Id)] = refused(f)
	}
	var failed []error
	for _, rec := range batch {
		err, ok := outcome[rec.EventID.String()]
		if !ok {
			err = errNoResult
		}
		results[rec.EventID] = err
		if err != nil {
			failed = append(failed, fmt.Errorf("event %s: %w", rec.EventID, err))
		}
	}
	if len(failed) > 0 {
		p.log.WarnContext(ctx, "events of a batch not accepted",
			"events", len(batch), "failed", len(failed), "error", errors.Join(failed...))
	}
	return messageIDs
}

func entry(rec app.OutboxRecord, traceparent string) types.SendMessageBatchRequestEntry {
	id := rec.EventID.String()
	attrs := map[string]types.MessageAttributeValue{
		EventTypeAttribute:    stringAttribute(rec.EventType),
		EventVersionAttribute: {DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(rec.EventVersion))},
	}
	if rec.CorrelationID != "" {
		attrs[CorrelationIDAttribute] = stringAttribute(rec.CorrelationID)
	}
	if traceparent != "" {
		attrs[TraceParentAttribute] = stringAttribute(traceparent)
	}
	return types.SendMessageBatchRequestEntry{
		Id:                     aws.String(id),
		MessageBody:            aws.String(string(rec.Payload)),
		MessageGroupId:         aws.String(rec.AggregateID),
		MessageDeduplicationId: aws.String(id),
		MessageAttributes:      attrs,
	}
}

func stringAttribute(v string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
}

type refusedError struct {
	code, message string
	senderFault   bool
}

func (e *refusedError) Error() string {
	side := "SQS"
	if e.senderFault {
		side = "sender"
	}
	return fmt.Sprintf("sqsout: SQS refused the event (%s fault): %s: %s", side, e.code, e.message)
}

func refused(f types.BatchResultErrorEntry) error {
	return &refusedError{code: aws.ToString(f.Code), message: aws.ToString(f.Message), senderFault: f.SenderFault}
}

func fail(results map[uuid.UUID]error, batch []app.OutboxRecord, err error) {
	for _, rec := range batch {
		results[rec.EventID] = err
	}
}
