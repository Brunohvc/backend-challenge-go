package sqsout_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/awsclient"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsout"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
)

const eventsURL = "http://sqs.local/000000000000/wallet-events.fifo"

var queues = awsclient.Queues{Input: "input-url", InputDLQ: "dlq-url", Events: eventsURL}

const secret = "payload-secret-7f3a"

var discardLog = slog.New(slog.DiscardHandler)

type logLines struct{ buf bytes.Buffer }

func (l *logLines) logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(&l.buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (l *logLines) decode(t *testing.T) []map[string]any {
	t.Helper()
	var lines []map[string]any
	dec := json.NewDecoder(bytes.NewReader(l.buf.Bytes()))
	for dec.More() {
		var line map[string]any
		if err := dec.Decode(&line); err != nil {
			t.Fatalf("log line: %v", err)
		}
		lines = append(lines, line)
	}
	return lines
}

func requireWarnings(t *testing.T, lines []map[string]any, want []warning) {
	t.Helper()
	if len(lines) != len(want) {
		t.Fatalf("logged %d lines, want %d warnings: %v", len(lines), len(want), lines)
	}
	for i, w := range want {
		line := lines[i]
		if line[slog.LevelKey] != slog.LevelWarn.String() || line[slog.MessageKey] != w.msg {
			t.Errorf("line %d = %s %q, want %s %q", i, line[slog.LevelKey], line[slog.MessageKey], slog.LevelWarn, w.msg)
		}
		for key, n := range w.counts {
			if got, ok := line[key].(float64); !ok || int(got) != n {
				t.Errorf("line %d %s = %v, want %d", i, key, line[key], n)
			}
		}
		msg, _ := line["error"].(string)
		for _, part := range w.says {
			if !strings.Contains(msg, part) {
				t.Errorf("line %d error %q does not say %q", i, msg, part)
			}
		}
		if strings.Contains(msg, secret) {
			t.Errorf("line %d error %q carries the payload", i, msg)
		}
	}
}

type warning struct {
	msg    string
	counts map[string]int
	says   []string
}

type fakeSQS struct {
	calls   []*sqs.SendMessageBatchInput
	respond func(call int, in *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error)
}

func (f *fakeSQS) SendMessageBatch(_ context.Context, in *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	f.calls = append(f.calls, in)
	if f.respond != nil {
		return f.respond(len(f.calls)-1, in)
	}
	return succeedAll(in), nil
}

func succeedAll(in *sqs.SendMessageBatchInput) *sqs.SendMessageBatchOutput {
	out := &sqs.SendMessageBatchOutput{}
	for _, e := range in.Entries {
		out.Successful = append(out.Successful, types.SendMessageBatchResultEntry{Id: e.Id, MessageId: aws.String("m-" + aws.ToString(e.Id))})
	}
	return out
}

func records(n int) []app.OutboxRecord {
	aggregates := []string{idgen.V7{}.New().String(), idgen.V7{}.New().String()}
	recs := make([]app.OutboxRecord, n)
	for i := range recs {
		id := idgen.V7{}.New()
		recs[i] = app.OutboxRecord{
			EventID:       id,
			AggregateType: "Wallet",
			AggregateID:   aggregates[i%len(aggregates)],
			EventType:     "WalletBalanceChanged",
			EventVersion:  1 + i%3,
			Payload:       fmt.Appendf(nil, `{"eventId": %q, "secret": %q}`, id, secret),
			CorrelationID: fmt.Sprintf("corr-%d", i),
			OccurredAt:    time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC),
			Attempts:      1,
		}
	}
	return recs
}

func requireAllSucceeded(t *testing.T, recs []app.OutboxRecord, results map[uuid.UUID]error) {
	t.Helper()
	if len(results) != len(recs) {
		t.Fatalf("got %d results, want one per record (%d)", len(results), len(recs))
	}
	for _, r := range recs {
		err, ok := results[r.EventID]
		if !ok || err != nil {
			t.Errorf("result of %s = %v (present %t), want nil", r.EventID, err, ok)
		}
	}
}

func requireNoPayload(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), secret) {
		t.Errorf("error %q carries the payload", err)
	}
}

func TestPublish_SendsBatchesOfTenInOrder(t *testing.T) {
	fake := &fakeSQS{}
	recs := records(23)

	results := sqsout.NewPublisher(fake, queues, tracing.Tracer{}, discardLog).Publish(t.Context(), recs)

	requireAllSucceeded(t, recs, results)
	if len(fake.calls) != 3 {
		t.Fatalf("got %d SendMessageBatch calls, want 3 (10 + 10 + 3)", len(fake.calls))
	}
	next := 0
	for c, call := range fake.calls {
		if want := min(10, len(recs)-next); len(call.Entries) != want {
			t.Errorf("call %d has %d entries, want %d", c, len(call.Entries), want)
		}
		if got := aws.ToString(call.QueueUrl); got != eventsURL {
			t.Errorf("call %d QueueUrl = %q, want the events queue", c, got)
		}
		ids := make(map[string]bool)
		for _, e := range call.Entries {
			rec := recs[next]
			next++
			if ids[aws.ToString(e.Id)] {
				t.Errorf("call %d repeats entry id %q", c, aws.ToString(e.Id))
			}
			ids[aws.ToString(e.Id)] = true
			requireEntry(t, e, rec)
		}
	}
	if next != len(recs) {
		t.Errorf("sent %d entries, want %d", next, len(recs))
	}
}

func requireEntry(t *testing.T, e types.SendMessageBatchRequestEntry, rec app.OutboxRecord) {
	t.Helper()
	if got := aws.ToString(e.MessageBody); got != string(rec.Payload) {
		t.Errorf("body of %s = %q, want the payload as stored", rec.EventID, got)
	}
	if got := aws.ToString(e.MessageGroupId); got != rec.AggregateID {
		t.Errorf("group of %s = %q, want the aggregate %q", rec.EventID, got, rec.AggregateID)
	}
	if got := aws.ToString(e.MessageDeduplicationId); got != rec.EventID.String() {
		t.Errorf("deduplication id of %s = %q, want the event id", rec.EventID, got)
	}
	if e.DelaySeconds != 0 {
		t.Errorf("delay of %s = %d, want 0", rec.EventID, e.DelaySeconds)
	}
	want := map[string]types.MessageAttributeValue{
		"eventType":     {DataType: aws.String("String"), StringValue: aws.String(rec.EventType)},
		"eventVersion":  {DataType: aws.String("Number"), StringValue: aws.String(fmt.Sprint(rec.EventVersion))},
		"correlationId": {DataType: aws.String("String"), StringValue: aws.String(rec.CorrelationID)},
	}
	if len(e.MessageAttributes) != len(want) {
		t.Errorf("attributes of %s = %d, want %d", rec.EventID, len(e.MessageAttributes), len(want))
	}
	for name, w := range want {
		g, ok := e.MessageAttributes[name]
		if !ok || aws.ToString(g.DataType) != aws.ToString(w.DataType) || aws.ToString(g.StringValue) != aws.ToString(w.StringValue) {
			t.Errorf("attribute %s of %s = %s %q, want %s %q", name, rec.EventID,
				aws.ToString(g.DataType), aws.ToString(g.StringValue), aws.ToString(w.DataType), aws.ToString(w.StringValue))
		}
	}
}

func TestPublish_OmitsAnEmptyCorrelationID(t *testing.T) {
	fake := &fakeSQS{}
	recs := records(1)
	recs[0].CorrelationID = ""

	results := sqsout.NewPublisher(fake, queues, tracing.Tracer{}, discardLog).Publish(t.Context(), recs)

	requireAllSucceeded(t, recs, results)
	if len(fake.calls) != 1 || len(fake.calls[0].Entries) != 1 {
		t.Fatalf("got %d calls, want one with the one event", len(fake.calls))
	}
	attrs := fake.calls[0].Entries[0].MessageAttributes
	if a, ok := attrs[sqsout.CorrelationIDAttribute]; ok {
		t.Errorf("%s attribute = %s %q, want it left out", sqsout.CorrelationIDAttribute, aws.ToString(a.DataType), aws.ToString(a.StringValue))
	}
	for _, name := range []string{sqsout.EventTypeAttribute, sqsout.EventVersionAttribute} {
		if _, ok := attrs[name]; !ok {
			t.Errorf("attribute %s is missing, want it sent", name)
		}
	}
}

func storedTrace(t *testing.T, tr tracing.Tracer) (string, trace.SpanContext) {
	t.Helper()
	ctx, request := tr.Start(t.Context(), "request")
	defer request.End()
	return tracing.TraceParent(ctx), request.SpanContext()
}

func sentTraceParent(e types.SendMessageBatchRequestEntry) (string, bool) {
	a, ok := e.MessageAttributes[sqsout.TraceParentAttribute]
	if !ok || aws.ToString(a.DataType) != "String" {
		return "", false
	}
	return aws.ToString(a.StringValue), true
}

func TestPublish_TracesEachSendUnderTheChangeThatWroteIt(t *testing.T) {
	tr, rec := tracingtest.New(t)
	stored, request := storedTrace(t, tr)
	fake := &fakeSQS{}
	recs := records(2)
	recs[0].TraceParent = stored
	ctx, pass := tr.Start(t.Context(), "pass")

	results := sqsout.NewPublisher(fake, queues, tr, discardLog).Publish(ctx, recs)
	pass.End()

	requireAllSucceeded(t, recs, results)
	sends := tracingtest.Named(rec, "send wallet-events.fifo")
	if len(sends) != 2 {
		t.Fatalf("%d send spans, want one per event; ended: %v", len(sends), tracingtest.Names(rec))
	}
	continued, fresh := sends[0], sends[1]
	if continued.Parent().SpanID() != request.SpanID() || continued.SpanContext().TraceID() != request.TraceID() {
		t.Errorf("send of the traced event is under %s, want the change that wrote it (%s)",
			continued.Parent().SpanID(), request.SpanID())
	}
	if links := continued.Links(); len(links) != 1 || links[0].SpanContext.SpanID() != pass.SpanContext().SpanID() {
		t.Errorf("links of the traced send = %v, want one to the relay pass", links)
	}
	if !tracingtest.ChildOf(fresh, tracingtest.One(t, rec, "pass")) || len(fresh.Links()) != 0 {
		t.Errorf("send of the untraced event: parent %s, %d links; want under the relay pass, unlinked",
			fresh.Parent().SpanID(), len(fresh.Links()))
	}
	for i, s := range sends {
		if s.SpanKind() != trace.SpanKindProducer {
			t.Errorf("send %d kind = %v, want producer", i, s.SpanKind())
		}
		want := map[string]string{
			"messaging.system":           "aws_sqs",
			"messaging.operation.type":   "send",
			"messaging.operation.name":   "send",
			"messaging.destination.name": "wallet-events.fifo",
			"messaging.message.id":       "m-" + recs[i].EventID.String(),
			"outbox.event_id":            recs[i].EventID.String(),
			"outbox.event_type":          recs[i].EventType,
		}
		attrs := tracingtest.Attributes(s)
		for key, value := range want {
			if attrs[key] != value {
				t.Errorf("send %d %s = %q, want %q", i, key, attrs[key], value)
			}
		}
		sc := s.SpanContext()
		got, ok := sentTraceParent(fake.calls[0].Entries[i])
		if want := "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-01"; !ok || got != want {
			t.Errorf("entry %d %s = %q, want the send span %q", i, sqsout.TraceParentAttribute, got, want)
		}
		if len(fake.calls[0].Entries[i].MessageAttributes) != 4 {
			t.Errorf("entry %d has %d attributes, want the three of every event and %s",
				i, len(fake.calls[0].Entries[i].MessageAttributes), sqsout.TraceParentAttribute)
		}
	}
}

func TestPublish_UntracedSendsNoTraceParent(t *testing.T) {
	tr, _ := tracingtest.New(t)
	stored, _ := storedTrace(t, tr)
	fake := &fakeSQS{}
	recs := records(1)
	recs[0].TraceParent = stored

	sqsout.NewPublisher(fake, queues, tracing.Tracer{}, discardLog).Publish(t.Context(), recs)

	if got, ok := sentTraceParent(fake.calls[0].Entries[0]); ok {
		t.Errorf("%s = %q with tracing off, want it left out", sqsout.TraceParentAttribute, got)
	}
}

func TestPublish_TracedSendRecordsTheFailure(t *testing.T) {
	tests := []struct {
		name    string
		respond func(int, *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error)
		want    string
	}{
		{
			name: "an entry SQS refused, by its code",
			respond: func(_ int, in *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
				return &sqs.SendMessageBatchOutput{Failed: []types.BatchResultErrorEntry{{
					Id: in.Entries[0].Id, Code: aws.String("InvalidParameterValue"), Message: aws.String("too long"),
				}}}, nil
			},
			want: "InvalidParameterValue",
		},
		{
			name: "an entry SQS said nothing about",
			respond: func(int, *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
				return &sqs.SendMessageBatchOutput{}, nil
			},
			want: "no_result",
		},
		{
			name: "a call that failed",
			respond: func(int, *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
				return nil, errors.New("connection reset with " + secret)
			},
			want: "send_failed",
		},
		{
			name: "a call cut by its deadline",
			respond: func(int, *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
				return nil, fmt.Errorf("operation error SQS: %w", context.DeadlineExceeded)
			},
			want: "deadline_exceeded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, rec := tracingtest.New(t)

			sqsout.NewPublisher(&fakeSQS{respond: tt.respond}, queues, tr, discardLog).Publish(t.Context(), records(1))

			span := tracingtest.One(t, rec, "send wallet-events.fifo")
			attrs := tracingtest.Attributes(span)
			if span.Status().Code != codes.Error || attrs["error.type"] != tt.want {
				t.Errorf("status %+v, error.type %q; want an error of type %q", span.Status(), attrs["error.type"], tt.want)
			}
		})
	}
}

func TestPublish_RefusedEntryFailsThatRecordOnly(t *testing.T) {
	recs := records(12)
	refused := recs[3]
	fake := &fakeSQS{respond: func(call int, in *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
		out := succeedAll(in)
		if call == 0 {
			out.Successful = out.Successful[:0]
			for _, e := range in.Entries {
				if aws.ToString(e.MessageDeduplicationId) == refused.EventID.String() {
					out.Failed = append(out.Failed, types.BatchResultErrorEntry{
						Id: e.Id, Code: aws.String("InvalidParameterValue"), Message: aws.String("attribute too long"), SenderFault: true,
					})
					continue
				}
				out.Successful = append(out.Successful, types.SendMessageBatchResultEntry{Id: e.Id})
			}
		}
		return out, nil
	}}

	results := sqsout.NewPublisher(fake, queues, tracing.Tracer{}, discardLog).Publish(t.Context(), recs)

	if len(results) != len(recs) {
		t.Fatalf("got %d results, want %d", len(results), len(recs))
	}
	for _, r := range recs {
		err := results[r.EventID]
		if r.EventID != refused.EventID {
			if err != nil {
				t.Errorf("result of %s = %v, want nil: only %s was refused", r.EventID, err, refused.EventID)
			}
			continue
		}
		if err == nil {
			t.Fatalf("result of the refused event = nil, want an error")
		}
		for _, part := range []string{"InvalidParameterValue", "attribute too long"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("error %q does not say %q", err, part)
			}
		}
		requireNoPayload(t, err)
	}
}

func TestPublish_FailedCallFailsItsBatchOnly(t *testing.T) {
	recs := records(15)
	boom := errors.New("connection reset")
	fake := &fakeSQS{respond: func(call int, in *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
		if call == 0 {
			return nil, boom
		}
		return succeedAll(in), nil
	}}

	results := sqsout.NewPublisher(fake, queues, tracing.Tracer{}, discardLog).Publish(t.Context(), recs)

	if len(fake.calls) != 2 {
		t.Fatalf("got %d calls, want 2: a failed batch must not stop the next one", len(fake.calls))
	}
	for i, r := range recs {
		err := results[r.EventID]
		switch {
		case i < 10 && !errors.Is(err, boom):
			t.Errorf("result of record %d = %v, want the error of its call", i, err)
		case i >= 10 && err != nil:
			t.Errorf("result of record %d = %v, want nil: its batch went through", i, err)
		}
		requireNoPayload(t, err)
	}
}

func TestPublish_LogsAFailedCallOnce(t *testing.T) {
	recs := records(25)
	boom := errors.New("connection reset")
	fake := &fakeSQS{respond: func(call int, in *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
		if call < 2 {
			return nil, boom
		}
		return succeedAll(in), nil
	}}
	logs := &logLines{}

	sqsout.NewPublisher(fake, queues, tracing.Tracer{}, logs.logger()).Publish(t.Context(), recs)

	failed := warning{msg: "event batch not sent", counts: map[string]int{"events": 10}, says: []string{boom.Error()}}
	requireWarnings(t, logs.decode(t), []warning{failed, failed})
}

func TestPublish_LogsTheRefusalsOfACallOnce(t *testing.T) {
	recs := records(12)
	refused := map[string]bool{recs[3].EventID.String(): true, recs[7].EventID.String(): true}
	fake := &fakeSQS{respond: func(_ int, in *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
		out := &sqs.SendMessageBatchOutput{}
		for _, e := range in.Entries {
			if refused[aws.ToString(e.Id)] {
				out.Failed = append(out.Failed, types.BatchResultErrorEntry{
					Id: e.Id, Code: aws.String("InternalError"), Message: aws.String("try again"),
				})
				continue
			}
			out.Successful = append(out.Successful, types.SendMessageBatchResultEntry{Id: e.Id})
		}
		return out, nil
	}}
	logs := &logLines{}

	sqsout.NewPublisher(fake, queues, tracing.Tracer{}, logs.logger()).Publish(t.Context(), recs)

	requireWarnings(t, logs.decode(t), []warning{{
		msg:    "events of a batch not accepted",
		counts: map[string]int{"events": 10, "failed": 2},
		says:   []string{recs[3].EventID.String(), recs[7].EventID.String(), "InternalError", "try again"},
	}})
}

func TestPublish_LogsWhatWasNotSentOnce(t *testing.T) {
	recs := records(35)
	ctx, cancel := context.WithCancel(t.Context())
	fake := &fakeSQS{respond: func(_ int, in *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
		cancel()
		return succeedAll(in), nil
	}}
	logs := &logLines{}

	sqsout.NewPublisher(fake, queues, tracing.Tracer{}, logs.logger()).Publish(ctx, recs)

	requireWarnings(t, logs.decode(t), []warning{{
		msg:    "events not sent",
		counts: map[string]int{"events": 25},
		says:   []string{context.Canceled.Error()},
	}})
}

func TestPublish_EntryWithoutResultFails(t *testing.T) {
	recs := records(3)
	fake := &fakeSQS{respond: func(_ int, in *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
		out := succeedAll(in)
		out.Successful = append(out.Successful[1:], types.SendMessageBatchResultEntry{Id: aws.String("stranger")})
		return out, nil
	}}

	results := sqsout.NewPublisher(fake, queues, tracing.Tracer{}, discardLog).Publish(t.Context(), recs)

	if len(results) != len(recs) {
		t.Fatalf("got %d results, want %d: an unknown id must not add one", len(results), len(recs))
	}
	if results[recs[0].EventID] == nil {
		t.Error("an entry SQS did not answer for counts as published")
	}
	for _, r := range recs[1:] {
		if err := results[r.EventID]; err != nil {
			t.Errorf("result of %s = %v, want nil", r.EventID, err)
		}
	}
}

func TestPublish_StopsWhenContextEnds(t *testing.T) {
	recs := records(25)
	ctx, cancel := context.WithCancel(t.Context())
	fake := &fakeSQS{respond: func(_ int, in *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
		cancel()
		return succeedAll(in), nil
	}}

	results := sqsout.NewPublisher(fake, queues, tracing.Tracer{}, discardLog).Publish(ctx, recs)

	if len(fake.calls) != 1 {
		t.Fatalf("got %d calls, want 1: nothing is sent once the context ended", len(fake.calls))
	}
	if len(results) != len(recs) {
		t.Fatalf("got %d results, want %d", len(results), len(recs))
	}
	for i, r := range recs {
		err := results[r.EventID]
		switch {
		case i < 10 && err != nil:
			t.Errorf("result of record %d = %v, want nil: SQS accepted it", i, err)
		case i >= 10 && !errors.Is(err, context.Canceled):
			t.Errorf("result of record %d = %v, want context.Canceled", i, err)
		}
	}
}

func TestPublish_NothingToSend(t *testing.T) {
	fake := &fakeSQS{}

	results := sqsout.NewPublisher(fake, queues, tracing.Tracer{}, discardLog).Publish(t.Context(), nil)

	if len(fake.calls) != 0 || len(results) != 0 {
		t.Errorf("Publish(nil) made %d calls and %d results, want none", len(fake.calls), len(results))
	}
}
