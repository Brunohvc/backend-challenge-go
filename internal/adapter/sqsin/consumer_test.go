package sqsin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/awsclient"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

var (
	_ sqsin.SQSAPI           = (*sqs.Client)(nil)
	_ sqsin.DLQ              = (*sqsin.DLQMover)(nil)
	_ sqsin.Submitter        = (*wagering.Service)(nil)
	_ sqsin.Metrics          = (*metrics.Metrics)(nil)
	_ sqsin.IdentityResolver = sqsin.TokenAttributeIdentity{}
)

const (
	testPlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	testWalletID = "0192f291-27dd-7d3f-8071-5f8685deef37"
	consumerName = "wager-transactions"
	tokenA       = "token-of-provider-a"
	gameMarker   = "game-only-in-the-body"
	twoMoreTicks = 700 * time.Millisecond
)

var testQueues = awsclient.Queues{
	Input:    "http://sqs.local/000000000000/wager-transactions.fifo",
	InputDLQ: "dlq-url",
	Events:   "events-url",
}

func testConfig() config.ConsumerConfig {
	return config.ConsumerConfig{
		Enabled:           true,
		Name:              consumerName,
		MaxGroups:         8,
		VisibilityTimeout: 30 * time.Second,
		ProcessingTimeout: 15 * time.Second,
		DrainTimeout:      5 * time.Second,
		WaitTime:          20 * time.Second,
		MaxReceives:       10,
	}
}

func wagerBody(t *testing.T, messageID string, edit func(data map[string]any)) string {
	t.Helper()
	data := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": "tx-" + messageID,
		"idempotencyKey":        "provider-a:tx-" + messageID,
		"playerId":              testPlayerID,
		"walletId":              testWalletID,
		"roundId":               "round-1",
		"gameId":                gameMarker,
		"kind":                  "BET",
		"money":                 map[string]any{"amount": "25.00", "currency": "BRL"},
	}
	if edit != nil {
		edit(data)
	}
	b, err := json.Marshal(map[string]any{
		"messageId":  messageID,
		"type":       sqsin.MessageType,
		"occurredAt": "2026-10-16T12:00:00.000Z",
		"data":       data,
	})
	if err != nil {
		t.Fatalf("encode body: %v", err)
	}
	return string(b)
}

func inbound(sqsID, groupID, body string, opts ...func(*types.Message)) types.Message {
	msg := types.Message{
		MessageId:     aws.String(sqsID),
		ReceiptHandle: aws.String(receipt(sqsID)),
		Body:          aws.String(body),
		Attributes: map[string]string{
			string(types.MessageSystemAttributeNameMessageGroupId):          groupID,
			string(types.MessageSystemAttributeNameApproximateReceiveCount): "1",
			string(types.MessageSystemAttributeNameSentTimestamp):           "1789900000000",
		},
		MessageAttributes: map[string]types.MessageAttributeValue{
			sqsin.ProviderTokenAttribute: stringAttr(tokenA),
		},
	}
	for _, o := range opts {
		o(&msg)
	}
	return msg
}

func receipt(sqsID string) string { return "receipt-" + sqsID }

func receivedTimes(n int) func(*types.Message) {
	return func(m *types.Message) {
		m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)] = strconv.Itoa(n)
	}
}

func withAttribute(name, value string) func(*types.Message) {
	return func(m *types.Message) { m.MessageAttributes[name] = stringAttr(value) }
}

func stringAttr(v string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
}

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

type receiveResult struct {
	msgs []types.Message
	err  error
}

type sqsOp struct {
	kind    string
	handle  string
	seconds int32
}

func deleted(sqsID string) sqsOp { return sqsOp{kind: "delete", handle: receipt(sqsID)} }

func hidden(sqsID string, seconds int32) sqsOp {
	return sqsOp{kind: "visibility", handle: receipt(sqsID), seconds: seconds}
}

func renewed(sqsID string, seconds int32) sqsOp {
	return sqsOp{kind: "renew", handle: receipt(sqsID), seconds: seconds}
}

type fakeSQS struct {
	batches chan receiveResult
	events  *eventLog

	beforeRenew func(handles []string)
	lost        map[string]types.BatchResultErrorEntry

	mu        sync.Mutex
	receives  []*sqs.ReceiveMessageInput
	canceled  int
	ops       []sqsOp
	renewals  [][]string
	deleteErr error
}

func newFakeSQS() *fakeSQS { return &fakeSQS{batches: make(chan receiveResult, 16)} }

func (f *fakeSQS) push(msgs ...types.Message) { f.batches <- receiveResult{msgs: msgs} }

func (f *fakeSQS) emptyLongPoll() { f.push() }

func (f *fakeSQS) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	f.receives = append(f.receives, in)
	f.mu.Unlock()
	f.events.add("receive")
	select {
	case r := <-f.batches:
		if r.err != nil {
			return nil, r.err
		}
		return &sqs.ReceiveMessageOutput{Messages: r.msgs}, nil
	case <-ctx.Done():
		f.mu.Lock()
		f.canceled++
		f.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (f *fakeSQS) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, sqsOp{kind: "delete", handle: aws.ToString(in.ReceiptHandle)})
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, sqsOp{kind: "visibility", handle: aws.ToString(in.ReceiptHandle), seconds: in.VisibilityTimeout})
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibilityBatch(_ context.Context, in *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	handles := make([]string, len(in.Entries))
	for i, e := range in.Entries {
		handles[i] = aws.ToString(e.ReceiptHandle)
	}
	if f.beforeRenew != nil {
		f.beforeRenew(handles)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewals = append(f.renewals, handles)
	out := &sqs.ChangeMessageVisibilityBatchOutput{}
	for _, e := range in.Entries {
		if failure, ok := f.lost[aws.ToString(e.ReceiptHandle)]; ok {
			failure.Id, failure.SenderFault = e.Id, true
			out.Failed = append(out.Failed, failure)
			continue
		}
		f.ops = append(f.ops, sqsOp{kind: "renew", handle: aws.ToString(e.ReceiptHandle), seconds: e.VisibilityTimeout})
		out.Successful = append(out.Successful, types.ChangeMessageVisibilityBatchResultEntry{Id: e.Id})
	}
	return out, nil
}

func (f *fakeSQS) renewalBatches() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.renewals)
}

func (f *fakeSQS) allOps() []sqsOp {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ops)
}

func (f *fakeSQS) opsOn(sqsID string) []sqsOp {
	var ops []sqsOp
	for _, op := range f.allOps() {
		if op.handle == receipt(sqsID) {
			ops = append(ops, op)
		}
	}
	return ops
}

func (f *fakeSQS) receiveCalls() []*sqs.ReceiveMessageInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.receives)
}

func (f *fakeSQS) canceledReceives() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.canceled
}

type dlqMove struct {
	sqsID, group, reason, code string
}

type fakeDLQ struct {
	mu    sync.Mutex
	moves []dlqMove
	err   error
}

func (f *fakeDLQ) Move(_ context.Context, msg types.Message, groupID, reason, code string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.moves = append(f.moves, dlqMove{sqsID: aws.ToString(msg.MessageId), group: groupID, reason: reason, code: code})
	return f.err
}

func (f *fakeDLQ) all() []dlqMove {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.moves)
}

var unknownKeyIdentity = sqsin.TokenAttributeIdentity{
	Verifier: fakeVerifier{err: fmt.Errorf("%w: %w", oidc.ErrInvalidToken, oidc.ErrUnknownKey)},
}

type fakeIdentity struct{ err error }

func (f fakeIdentity) ProviderFor(_ context.Context, msg types.Message) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	provider, ok := strings.CutPrefix(aws.ToString(msg.MessageAttributes[sqsin.ProviderTokenAttribute].StringValue), "token-of-")
	if !ok {
		return "", sqsin.ErrInvalidProviderToken
	}
	return provider, nil
}

type identityFunc func(ctx context.Context, msg types.Message) (string, error)

func (f identityFunc) ProviderFor(ctx context.Context, msg types.Message) (string, error) {
	return f(ctx, msg)
}

type submission struct {
	cmd         wager.Command
	src         wagering.Source
	fields      logging.Fields
	span        trace.SpanContext
	deadline    time.Time
	hasDeadline bool
}

type fakeSubmitter struct {
	fn func(ctx context.Context, cmd wager.Command, src wagering.Source) (wagering.Result, error)

	mu    sync.Mutex
	calls []submission
}

func (f *fakeSubmitter) Submit(ctx context.Context, cmd wager.Command, src wagering.Source) (wagering.Result, error) {
	deadline, ok := ctx.Deadline()
	f.mu.Lock()
	f.calls = append(f.calls, submission{
		cmd: cmd, src: src, fields: logging.FieldsFrom(ctx), span: trace.SpanContextFromContext(ctx),
		deadline: deadline, hasDeadline: ok,
	})
	f.mu.Unlock()
	if f.fn == nil {
		return decided(wager.StatusProcessed), nil
	}
	return f.fn(ctx, cmd, src)
}

func (f *fakeSubmitter) all() []submission {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func (f *fakeSubmitter) submitted() []string {
	var ids []string
	for _, s := range f.all() {
		ids = append(ids, s.src.CausationID)
	}
	return ids
}

var decidedTxID = uuid.MustParse("0192f300-0000-7000-8000-000000000001")

func decided(status wager.Status) wagering.Result {
	return wagering.Result{Transaction: wager.Snapshot{ID: decidedTxID, Status: status}}
}

type fakeMetrics struct {
	mu       sync.Mutex
	messages []string
	reasons  []string
}

func (m *fakeMetrics) Message(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, result)
}

func (m *fakeMetrics) DLQ(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reasons = append(m.reasons, reason)
}

func (m *fakeMetrics) results() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.messages)
}

func (m *fakeMetrics) dlqReasons() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.reasons)
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(b.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

type harness struct {
	cfg      config.ConsumerConfig
	sqs      *fakeSQS
	dlq      *fakeDLQ
	sub      *fakeSubmitter
	identity sqsin.IdentityResolver
	ping     func(context.Context) error
	m        *fakeMetrics
	logs     *syncBuffer
	tracer   tracing.Tracer
	c        *sqsin.Consumer
}

func newHarness(cfg config.ConsumerConfig) *harness {
	return &harness{
		cfg:      cfg,
		sqs:      newFakeSQS(),
		dlq:      &fakeDLQ{},
		sub:      &fakeSubmitter{},
		identity: fakeIdentity{},
		ping:     func(context.Context) error { return nil },
		m:        &fakeMetrics{},
		logs:     &syncBuffer{},
	}
}

func (h *harness) start(t *testing.T) {
	t.Helper()
	h.startIn(t.Context(), t)
}

func (h *harness) startIn(ctx context.Context, t *testing.T) {
	t.Helper()
	log := logging.New(h.logs, slog.LevelDebug, "test")
	h.c = sqsin.NewConsumer(h.cfg, testQueues, sqsin.Deps{
		Client: h.sqs, Identity: h.identity, DLQ: h.dlq, Submitter: h.sub, Ping: h.ping, Metrics: h.m, Log: log,
		Tracer: h.tracer,
	})
	h.c.SetRetryTiming(10*time.Millisecond, 10*time.Millisecond)
	if err := h.c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = h.c.Stop(ctx)
	})
}

func (h *harness) stop(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := h.c.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	waitWithin(t, 5*time.Second, what, cond)
}

func waitWithin(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func submitErr(err error) func(context.Context, wager.Command, wagering.Source) (wagering.Result, error) {
	return func(context.Context, wager.Command, wagering.Source) (wagering.Result, error) {
		return wagering.Result{}, err
	}
}

func submitResult(res wagering.Result) func(context.Context, wager.Command, wagering.Source) (wagering.Result, error) {
	return func(context.Context, wager.Command, wagering.Source) (wagering.Result, error) { return res, nil }
}

func TestConsumer_Outcomes(t *testing.T) {
	const retryWait = 8
	dead := func(reason, code string) *dlqMove {
		return &dlqMove{sqsID: "m1", group: "wallet-1", reason: reason, code: code}
	}
	tests := []struct {
		name         string
		edit         func(data map[string]any)
		bodyOverride string
		identity     sqsin.IdentityResolver
		submit       func(context.Context, wager.Command, wagering.Source) (wagering.Result, error)
		wantSubmit   bool
		wantResult   string
		wantOps      []sqsOp
		wantMove     *dlqMove
	}{
		{name: "processed", submit: submitResult(decided(wager.StatusProcessed)),
			wantSubmit: true, wantResult: "processed", wantOps: []sqsOp{deleted("m1")}},
		{name: "rejected", submit: submitResult(decided(wager.StatusRejected)),
			wantSubmit: true, wantResult: "processed", wantOps: []sqsOp{deleted("m1")}},
		{name: "pending reference", submit: submitResult(decided(wager.StatusPendingReference)),
			wantSubmit: true, wantResult: "processed", wantOps: []sqsOp{deleted("m1")}},
		{name: "replay of the idempotency key", submit: submitResult(wagering.Result{Transaction: wager.Snapshot{ID: decidedTxID, Status: wager.StatusProcessed}, Replay: true}),
			wantSubmit: true, wantResult: "duplicate", wantOps: []sqsOp{deleted("m1")}},
		{name: "message already handled", submit: submitResult(wagering.Result{Transaction: wager.Snapshot{ID: decidedTxID, Status: wager.StatusProcessed}, DuplicateMessage: true}),
			wantSubmit: true, wantResult: "duplicate", wantOps: []sqsOp{deleted("m1")}},
		{name: "message id reused", submit: submitErr(fmt.Errorf("inbox: %w", wagering.ErrMessageIDReused)),
			wantSubmit: true, wantResult: "dlq", wantMove: dead(sqsin.ReasonMessageIDReused, "")},
		{name: "idempotency key reused", submit: submitErr(wagering.ErrIdempotencyKeyReused),
			wantSubmit: true, wantResult: "dlq", wantMove: dead(sqsin.ReasonIdempotencyKeyReused, "")},
		{name: "duplicate external transaction", submit: submitErr(wagering.ErrDuplicateExternalTransaction),
			wantSubmit: true, wantResult: "dlq", wantMove: dead(sqsin.ReasonDuplicateExternalTransaction, "")},
		{name: "wallet not found", submit: submitErr(fmt.Errorf("%w: %w", wagering.ErrWalletNotFound, app.ErrNotFound)),
			wantSubmit: true, wantResult: "dlq", wantMove: dead(sqsin.ReasonWalletNotFound, "")},
		{name: "permanent failure", submit: submitErr(fmt.Errorf("wagering: %w: broken invariant", app.ErrPermanent)),
			wantSubmit: true, wantResult: "dlq", wantMove: dead(sqsin.ReasonProcessingFailed, "")},
		{name: "database unavailable", submit: submitErr(fmt.Errorf("%w: connection refused", app.ErrUnavailable)),
			wantSubmit: true, wantResult: "retry", wantOps: []sqsOp{hidden("m1", retryWait)}},
		{name: "processing deadline", submit: submitErr(fmt.Errorf("query: %w", context.DeadlineExceeded)),
			wantSubmit: true, wantResult: "retry", wantOps: []sqsOp{hidden("m1", retryWait)}},
		{name: "unclassified error", submit: submitErr(errors.New("something nobody classified")),
			wantSubmit: true, wantResult: "retry", wantOps: []sqsOp{hidden("m1", retryWait)}},
		{name: "panic", submit: func(context.Context, wager.Command, wagering.Source) (wagering.Result, error) { panic("boom") },
			wantSubmit: true, wantResult: "retry", wantOps: []sqsOp{hidden("m1", retryWait)}},
		{name: "not a JSON object", bodyOverride: "not json",
			wantResult: "dlq", wantMove: dead(sqsin.ReasonInvalidMessage, "INVALID_REQUEST")},
		{name: "unknown field", edit: func(d map[string]any) { d["extra"] = "x" },
			wantResult: "dlq", wantMove: dead(sqsin.ReasonInvalidMessage, "INVALID_REQUEST")},
		{name: "identifier with whitespace", edit: func(d map[string]any) { d["roundId"] = "round 1" },
			wantResult: "dlq", wantMove: dead(sqsin.ReasonInvalidMessage, "INVALID_REQUEST")},
		{name: "opening kind", edit: func(d map[string]any) { d["kind"] = "OPENING" },
			wantResult: "dlq", wantMove: dead(sqsin.ReasonInvalidMessage, "INVALID_KIND")},
		{name: "amount not canonical", edit: func(d map[string]any) { d["money"] = map[string]any{"amount": "25", "currency": "BRL"} },
			wantResult: "dlq", wantMove: dead(sqsin.ReasonInvalidMessage, "INVALID_MONEY")},
		{name: "missing provider token", identity: fakeIdentity{err: sqsin.ErrMissingProviderToken},
			wantResult: "dlq", wantMove: dead(sqsin.ReasonUnauthenticated, "MISSING_PROVIDER_TOKEN")},
		{name: "malformed body without a provider token", bodyOverride: "not json", identity: fakeIdentity{err: sqsin.ErrMissingProviderToken},
			wantResult: "dlq", wantMove: dead(sqsin.ReasonUnauthenticated, "MISSING_PROVIDER_TOKEN")},
		{name: "expired provider token", identity: fakeIdentity{err: fmt.Errorf("%w: %w", sqsin.ErrInvalidProviderToken, oidc.ErrExpiredToken)},
			wantResult: "dlq", wantMove: dead(sqsin.ReasonUnauthenticated, "INVALID_PROVIDER_TOKEN")},
		{name: "token not of a provider", identity: fakeIdentity{err: sqsin.ErrNotProvider},
			wantResult: "dlq", wantMove: dead(sqsin.ReasonForbidden, "NOT_PROVIDER")},
		{name: "provider identity mismatch", edit: func(d map[string]any) { d["providerId"] = "provider-b" },
			wantResult: "dlq", wantMove: dead(sqsin.ReasonProviderIdentityMismatch, "")},
		{name: "provider identity mismatch with an invalid command", edit: func(d map[string]any) { d["providerId"], d["kind"] = "provider-b", "OPENING" },
			wantResult: "dlq", wantMove: dead(sqsin.ReasonProviderIdentityMismatch, "")},
		{name: "provider id missing", edit: func(d map[string]any) { delete(d, "providerId") },
			wantResult: "dlq", wantMove: dead(sqsin.ReasonInvalidMessage, "INVALID_REQUEST")},
		{name: "provider id empty", edit: func(d map[string]any) { d["providerId"] = "" },
			wantResult: "dlq", wantMove: dead(sqsin.ReasonInvalidMessage, "INVALID_REQUEST")},
		{name: "provider id with whitespace", edit: func(d map[string]any) { d["providerId"] = "provider-a " },
			wantResult: "dlq", wantMove: dead(sqsin.ReasonInvalidMessage, "INVALID_REQUEST")},
		{name: "sent timestamp not requested", identity: fakeIdentity{err: sqsin.ErrMissingSentTimestamp},
			wantResult: "retry", wantOps: []sqsOp{hidden("m1", retryWait)}},
		{name: "token verifier unavailable", identity: fakeIdentity{err: errors.New("oidc: key set unreachable")},
			wantResult: "retry", wantOps: []sqsOp{hidden("m1", retryWait)}},
		{name: "provider token rejected by the verifier", identity: sqsin.TokenAttributeIdentity{Verifier: fakeVerifier{err: oidc.ErrInvalidToken}},
			wantResult: "dlq", wantMove: dead(sqsin.ReasonUnauthenticated, "INVALID_PROVIDER_TOKEN")},
		{name: "provider token signed with a key not in the key set", identity: unknownKeyIdentity,
			wantResult: "retry", wantOps: []sqsOp{hidden("m1", retryWait)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(testConfig())
			h.sub.fn = tt.submit
			if tt.identity != nil {
				h.identity = tt.identity
			}
			body := tt.bodyOverride
			if body == "" {
				body = wagerBody(t, "msg-1", tt.edit)
			}
			h.start(t)

			h.sqs.push(inbound("m1", "wallet-1", body, receivedTimes(3)))
			waitFor(t, "the message to be settled", func() bool { return len(h.m.results()) > 0 })
			h.stop(t)

			if got := h.m.results(); !slices.Equal(got, []string{tt.wantResult}) {
				t.Errorf("results = %v, want [%s]", got, tt.wantResult)
			}
			if got := h.sqs.allOps(); !slices.Equal(got, tt.wantOps) {
				t.Errorf("queue calls = %+v, want %+v", got, tt.wantOps)
			}
			var wantMoves []dlqMove
			var wantReasons []string
			if tt.wantMove != nil {
				wantMoves, wantReasons = []dlqMove{*tt.wantMove}, []string{tt.wantMove.reason}
			}
			if got := h.dlq.all(); !slices.Equal(got, wantMoves) {
				t.Errorf("dead-letter moves = %+v, want %+v", got, wantMoves)
			}
			if got := h.m.dlqReasons(); !slices.Equal(got, wantReasons) {
				t.Errorf("DLQ reasons = %v, want %v", got, wantReasons)
			}
			if got := len(h.sub.all()) > 0; got != tt.wantSubmit {
				t.Errorf("Submit called = %v, want %v", got, tt.wantSubmit)
			}
			if logs := h.logs.String(); strings.Contains(logs, tokenA) || strings.Contains(logs, gameMarker) {
				t.Errorf("the logs carry the token or the body:\n%s", logs)
			}
			mismatch := tt.wantMove != nil && tt.wantMove.reason == sqsin.ReasonProviderIdentityMismatch
			if warned := strings.Contains(h.logs.String(), `"security":true`); warned != mismatch {
				t.Errorf("security warning logged = %v, want %v", warned, mismatch)
			}
			if n := h.c.HeldMessages(); n != 0 {
				t.Errorf("%d messages still held after the message was settled, want 0", n)
			}
		})
	}
}

func TestConsumer_SubmitsCommandWithMessageSource(t *testing.T) {
	h := newHarness(testConfig())
	h.start(t)
	withCorrelation := wagerBody(t, "msg-1", nil)
	withoutCorrelation := wagerBody(t, "msg-2", func(d map[string]any) { d["walletId"] = "0192f291-27dd-7d3f-8071-5f8685deef38" })
	malformedCorrelation := wagerBody(t, "msg-3", func(d map[string]any) { d["walletId"] = "0192f291-27dd-7d3f-8071-5f8685deef39" })

	h.sqs.push(
		inbound("m1", "wallet-1", withCorrelation, withAttribute(sqsin.CorrelationIDAttribute, "corr-1")),
		inbound("m2", "wallet-2", withoutCorrelation),
		inbound("m3", "wallet-3", malformedCorrelation, withAttribute(sqsin.CorrelationIDAttribute, "not allowed\n")),
	)
	waitFor(t, "three submissions", func() bool { return len(h.sub.all()) == 3 })
	h.stop(t)

	wantCorrelation := map[string]string{"msg-1": "corr-1", "msg-2": "msg-2", "msg-3": "msg-3"}
	bodies := map[string]string{"msg-1": withCorrelation, "msg-2": withoutCorrelation, "msg-3": malformedCorrelation}
	for _, s := range h.sub.all() {
		id := s.src.CausationID
		env, in, err := sqsin.ParseEnvelope([]byte(bodies[id]))
		if err != nil {
			t.Fatalf("ParseEnvelope(%s): %v", id, err)
		}
		hash, err := sqsin.InboxHash(env)
		if err != nil {
			t.Fatalf("InboxHash(%s): %v", id, err)
		}
		wantCmd, err := wager.NewCommand(in)
		if err != nil {
			t.Fatalf("NewCommand(%s): %v", id, err)
		}
		wantSrc := wagering.Source{
			Channel:       wagering.ChannelSQS,
			CorrelationID: wantCorrelation[id],
			CausationID:   id,
			Inbox:         &wagering.InboxMessage{Consumer: consumerName, MessageID: id, Hash: hash},
		}
		if !reflect.DeepEqual(s.src, wantSrc) {
			t.Errorf("%s: source = %+v (inbox %+v), want %+v (inbox %+v)", id, s.src, s.src.Inbox, wantSrc, wantSrc.Inbox)
		}
		if s.cmd != wantCmd {
			t.Errorf("%s: command = %+v, want %+v", id, s.cmd, wantCmd)
		}
		wantFields := logging.Fields{CorrelationID: wantCorrelation[id], MessageID: id, WalletID: in.WalletID, ProviderID: "provider-a"}
		if s.fields != wantFields {
			t.Errorf("%s: log fields = %+v, want %+v", id, s.fields, wantFields)
		}
		if !s.hasDeadline || time.Until(s.deadline) > testConfig().ProcessingTimeout {
			t.Errorf("%s: Submit context deadline = %v (set %v), want within the processing timeout", id, s.deadline, s.hasDeadline)
		}
	}
}

func TestConsumer_ReceiveRequest(t *testing.T) {
	h := newHarness(testConfig())
	h.start(t)
	h.sqs.emptyLongPoll()
	waitFor(t, "two receives", func() bool { return len(h.sqs.receiveCalls()) >= 2 })
	h.stop(t)

	calls := h.sqs.receiveCalls()
	in := calls[0]
	if aws.ToString(in.QueueUrl) != testQueues.Input || in.MaxNumberOfMessages != 10 ||
		in.WaitTimeSeconds != 20 || in.VisibilityTimeout != 30 {
		t.Errorf("receive = queue %s, max %d, wait %ds, visibility %ds; want %s, 10, 20s, 30s",
			aws.ToString(in.QueueUrl), in.MaxNumberOfMessages, in.WaitTimeSeconds, in.VisibilityTimeout, testQueues.Input)
	}
	if !slices.Equal(in.MessageSystemAttributeNames, []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}) {
		t.Errorf("system attributes = %v, want [All]", in.MessageSystemAttributeNames)
	}
	if !slices.Equal(in.MessageAttributeNames, []string{"All"}) {
		t.Errorf("message attributes = %v, want [All]", in.MessageAttributeNames)
	}
	first, second := aws.ToString(calls[0].ReceiveRequestAttemptId), aws.ToString(calls[1].ReceiveRequestAttemptId)
	if first == "" || first == second {
		t.Errorf("attempt ids = %q, %q; want one of its own per receive", first, second)
	}
}

func TestConsumer_GroupStopsAtFirstRetry(t *testing.T) {
	h := newHarness(testConfig())
	h.sub.fn = func(_ context.Context, _ wager.Command, src wagering.Source) (wagering.Result, error) {
		if src.CausationID == "msg-a2" {
			return wagering.Result{}, errors.New("transient")
		}
		return decided(wager.StatusProcessed), nil
	}
	h.start(t)

	h.sqs.push(
		inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil)),
		inbound("b1", "wallet-b", wagerBody(t, "msg-b1", nil)),
		inbound("a2", "wallet-a", wagerBody(t, "msg-a2", nil)),
		inbound("a3", "wallet-a", wagerBody(t, "msg-a3", nil)),
	)
	waitFor(t, "a3 to be given back", func() bool { return len(h.sqs.opsOn("a3")) > 0 })
	waitFor(t, "b1 to be deleted", func() bool { return len(h.sqs.opsOn("b1")) > 0 })
	h.stop(t)

	var groupA []string
	for _, id := range h.sub.submitted() {
		if strings.HasPrefix(id, "msg-a") {
			groupA = append(groupA, id)
		}
	}
	if want := []string{"msg-a1", "msg-a2"}; !slices.Equal(groupA, want) {
		t.Errorf("group a submitted = %v, want %v", groupA, want)
	}
	var opsA []sqsOp
	for _, op := range h.sqs.allOps() {
		if op.handle != receipt("b1") {
			opsA = append(opsA, op)
		}
	}
	if want := []sqsOp{deleted("a1"), hidden("a2", 2), hidden("a3", 0)}; !slices.Equal(opsA, want) {
		t.Errorf("group a calls = %+v, want %+v", opsA, want)
	}
	if got := h.sqs.opsOn("b1"); !slices.Equal(got, []sqsOp{deleted("b1")}) {
		t.Errorf("b1 calls = %+v, want it deleted", got)
	}
}

func TestConsumer_IdentityVerdictOnCanceledContextIsRetried(t *testing.T) {
	verdictOnceCanceled := identityFunc(func(ctx context.Context, _ types.Message) (string, error) {
		<-ctx.Done()
		return "", fmt.Errorf("%w: %w", sqsin.ErrInvalidProviderToken, oidc.ErrInvalidToken)
	})
	t.Run("processing timeout", func(t *testing.T) {
		cfg := testConfig()
		cfg.ProcessingTimeout = 50 * time.Millisecond
		h := newHarness(cfg)
		h.identity = verdictOnceCanceled
		h.start(t)

		h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil)))
		waitFor(t, "the message to be settled", func() bool { return len(h.m.results()) > 0 })
		h.stop(t)

		if got := h.m.results(); !slices.Equal(got, []string{"retry"}) {
			t.Errorf("results = %v, want [retry]", got)
		}
		if got, want := h.sqs.allOps(), []sqsOp{hidden("m1", 2)}; !slices.Equal(got, want) {
			t.Errorf("queue calls = %+v, want %+v", got, want)
		}
		if got := h.dlq.all(); len(got) != 0 {
			t.Errorf("dead-letter moves = %+v, want none", got)
		}
	})
	t.Run("shutdown", func(t *testing.T) {
		cfg := testConfig()
		cfg.DrainTimeout = 50 * time.Millisecond
		h := newHarness(cfg)
		started := make(chan struct{})
		h.identity = identityFunc(func(ctx context.Context, msg types.Message) (string, error) {
			close(started)
			return verdictOnceCanceled(ctx, msg)
		})
		h.start(t)

		h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil)))
		<-started
		h.stop(t)

		if got := h.m.results(); len(got) != 0 {
			t.Errorf("results = %v, want none", got)
		}
		if got, want := h.sqs.allOps(), []sqsOp{hidden("m1", 0)}; !slices.Equal(got, want) {
			t.Errorf("queue calls = %+v, want %+v", got, want)
		}
		if got := h.dlq.all(); len(got) != 0 {
			t.Errorf("dead-letter moves = %+v, want none", got)
		}
	})
}

func TestConsumer_DeadLettersOnceRetriesAreExhausted(t *testing.T) {
	transient := errors.New("something nobody classified")
	unavailable := fmt.Errorf("%w: connection refused", app.ErrUnavailable)
	exhausted := []dlqMove{{sqsID: "m1", group: "wallet-1", reason: sqsin.ReasonRetriesExhausted}}
	tests := []struct {
		name         string
		receives     int
		err          error
		identity     sqsin.IdentityResolver
		databaseDown bool
		moveErr      error
		wantResult   string
		wantOps      []sqsOp
		wantMoves    []dlqMove
		wantReasons  []string
	}{
		{name: "below the limit", receives: 9, err: transient,
			wantResult: "retry", wantOps: []sqsOp{hidden("m1", 300)}},
		{name: "at the limit", receives: 10, err: transient,
			wantResult: "dlq", wantMoves: exhausted, wantReasons: []string{sqsin.ReasonRetriesExhausted}},
		{name: "past the limit", receives: 12, err: transient,
			wantResult: "dlq", wantMoves: exhausted, wantReasons: []string{sqsin.ReasonRetriesExhausted}},
		{name: "database unavailable but answering", receives: 10, err: unavailable,
			wantResult: "dlq", wantMoves: exhausted, wantReasons: []string{sqsin.ReasonRetriesExhausted}},
		{name: "database down", receives: 10, err: unavailable, databaseDown: true,
			wantResult: "retry", wantOps: []sqsOp{hidden("m1", 300)}},
		{name: "dead-letter queue refuses the move", receives: 10, err: transient, moveErr: errors.New("throttled"),
			wantResult: "retry", wantOps: []sqsOp{hidden("m1", 300)}, wantMoves: exhausted},
		{name: "signing key still unknown at the limit", receives: 10, identity: unknownKeyIdentity,
			wantResult: "dlq", wantMoves: exhausted, wantReasons: []string{sqsin.ReasonRetriesExhausted}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(testConfig())
			h.sub.fn = submitErr(tt.err)
			if tt.identity != nil {
				h.identity = tt.identity
			}
			h.dlq.err = tt.moveErr
			if tt.databaseDown {
				h.ping = func(context.Context) error { return errors.New("connection refused") }
			}
			h.start(t)

			h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil), receivedTimes(tt.receives)))
			waitFor(t, "the message to be settled", func() bool { return len(h.m.results()) > 0 })
			h.stop(t)

			if got := h.m.results(); !slices.Equal(got, []string{tt.wantResult}) {
				t.Errorf("results = %v, want [%s]", got, tt.wantResult)
			}
			if got := h.sqs.allOps(); !slices.Equal(got, tt.wantOps) {
				t.Errorf("queue calls = %+v, want %+v", got, tt.wantOps)
			}
			if got := h.dlq.all(); !slices.Equal(got, tt.wantMoves) {
				t.Errorf("dead-letter moves = %+v, want %+v", got, tt.wantMoves)
			}
			if got := h.m.dlqReasons(); !slices.Equal(got, tt.wantReasons) {
				t.Errorf("DLQ reasons = %v, want %v", got, tt.wantReasons)
			}
		})
	}
}

func TestConsumer_PanicStackIsLoggedOnce(t *testing.T) {
	tests := []struct {
		name       string
		receives   int
		wantResult string
		wantMoves  []dlqMove
	}{
		{name: "retried", receives: 3, wantResult: "retry"},
		{name: "retries exhausted", receives: 10, wantResult: "dlq",
			wantMoves: []dlqMove{{sqsID: "m1", group: "wallet-1", reason: sqsin.ReasonRetriesExhausted}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(testConfig())
			h.sub.fn = func(context.Context, wager.Command, wagering.Source) (wagering.Result, error) { panic("boom") }
			h.start(t)

			h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil), receivedTimes(tt.receives)))
			waitFor(t, "the message to be settled", func() bool { return len(h.m.results()) > 0 })
			h.stop(t)

			if got := h.m.results(); !slices.Equal(got, []string{tt.wantResult}) {
				t.Errorf("results = %v, want [%s]", got, tt.wantResult)
			}
			if got := h.dlq.all(); !slices.Equal(got, tt.wantMoves) {
				t.Errorf("dead-letter moves = %+v, want %+v", got, tt.wantMoves)
			}
			var withStack []map[string]any
			for _, l := range h.logs.lines(t) {
				if stack, ok := l["stack"].(string); ok && stack != "" {
					withStack = append(withStack, l)
				}
			}
			if len(withStack) != 1 {
				t.Fatalf("%d log lines with a stack, want 1:\n%s", len(withStack), h.logs.String())
			}
			if l := withStack[0]; l["level"] != "ERROR" || l["panic"] != "boom" {
				t.Errorf("stack logged at %v for the panic %v, want ERROR for boom", l["level"], l["panic"])
			}
		})
	}
}

func TestConsumer_InflatedFollowerGetsARealAttempt(t *testing.T) {
	h := newHarness(testConfig())
	h.sub.fn = func(_ context.Context, _ wager.Command, src wagering.Source) (wagering.Result, error) {
		if src.CausationID == "msg-a1" {
			return wagering.Result{}, errors.New("transient")
		}
		return decided(wager.StatusProcessed), nil
	}
	h.start(t)

	h.sqs.push(
		inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil), receivedTimes(12)),
		inbound("a2", "wallet-a", wagerBody(t, "msg-a2", nil), receivedTimes(12)),
	)
	waitFor(t, "both messages settled", func() bool { return len(h.m.results()) == 2 })
	h.stop(t)

	if got, want := h.dlq.all(), []dlqMove{{sqsID: "a1", group: "wallet-a", reason: sqsin.ReasonRetriesExhausted}}; !slices.Equal(got, want) {
		t.Errorf("dead-letter moves = %+v, want %+v", got, want)
	}
	if got, want := h.sub.submitted(), []string{"msg-a1", "msg-a2"}; !slices.Equal(got, want) {
		t.Errorf("submitted = %v, want %v", got, want)
	}
	if got, want := h.sqs.allOps(), []sqsOp{deleted("a2")}; !slices.Equal(got, want) {
		t.Errorf("queue calls = %+v, want %+v", got, want)
	}
	if got, want := h.m.results(), []string{"dlq", "processed"}; !slices.Equal(got, want) {
		t.Errorf("results = %v, want %v", got, want)
	}
}

func TestConsumer_ProcessesAtMostMaxGroupsAtOnce(t *testing.T) {
	cfg := testConfig()
	cfg.MaxGroups = 2
	h := newHarness(cfg)
	release := make(chan struct{})
	var running, peak atomic.Int32
	h.sub.fn = func(context.Context, wager.Command, wagering.Source) (wagering.Result, error) {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		running.Add(-1)
		return decided(wager.StatusProcessed), nil
	}
	h.start(t)

	var batch []types.Message
	for i := range 5 {
		id := fmt.Sprintf("g%d", i)
		batch = append(batch, inbound(id, "wallet-"+id, wagerBody(t, "msg-"+id, nil)))
	}
	h.sqs.push(batch...)
	waitFor(t, "two groups running", func() bool { return running.Load() == 2 })
	time.Sleep(50 * time.Millisecond)
	if n, receives := len(h.sub.all()), len(h.sqs.receiveCalls()); n != 2 || receives != 1 {
		t.Errorf("with every slot busy: %d submissions and %d receives, want 2 and 1", n, receives)
	}
	close(release)
	waitFor(t, "five messages processed", func() bool { return len(h.m.results()) == 5 })
	h.stop(t)

	if p := peak.Load(); p != 2 {
		t.Errorf("groups at once = %d, want 2", p)
	}
}

func TestConsumer_StopDrainsThenGivesBackUnstarted(t *testing.T) {
	h := newHarness(testConfig())
	started, release := make(chan struct{}), make(chan struct{})
	ctxErr := make(chan error, 1)
	h.sub.fn = func(ctx context.Context, _ wager.Command, _ wagering.Source) (wagering.Result, error) {
		close(started)
		<-release
		ctxErr <- ctx.Err()
		return decided(wager.StatusProcessed), nil
	}
	h.start(t)
	h.sqs.push(
		inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil)),
		inbound("a2", "wallet-a", wagerBody(t, "msg-a2", nil)),
		inbound("a3", "wallet-a", wagerBody(t, "msg-a3", nil)),
	)
	<-started

	stopped := make(chan error, 1)
	go func() { stopped <- h.c.Stop(context.Background()) }()
	waitFor(t, "polling to stop", func() bool { return h.sqs.canceledReceives() == 1 })
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned %v while a message was in process", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if err := <-ctxErr; err != nil {
		t.Errorf("the message in process saw its context end (%v) before the drain timeout", err)
	}
	if got := h.sub.submitted(); !slices.Equal(got, []string{"msg-a1"}) {
		t.Errorf("submitted = %v, want only msg-a1", got)
	}
	if want := []sqsOp{deleted("a1"), hidden("a2", 0), hidden("a3", 0)}; !slices.Equal(h.sqs.allOps(), want) {
		t.Errorf("queue calls = %+v, want %+v", h.sqs.allOps(), want)
	}
	if got := h.m.results(); !slices.Equal(got, []string{"processed"}) {
		t.Errorf("results = %v, want [processed]", got)
	}
}

func TestConsumer_StopGivesBackGroupsWaitingForASlot(t *testing.T) {
	cfg := testConfig()
	cfg.MaxGroups = 1
	h := newHarness(cfg)
	started, release := make(chan struct{}), make(chan struct{})
	h.sub.fn = func(_ context.Context, _ wager.Command, src wagering.Source) (wagering.Result, error) {
		if src.CausationID == "msg-a1" {
			close(started)
			<-release
		}
		return decided(wager.StatusProcessed), nil
	}
	h.start(t)
	h.sqs.push(
		inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil)),
		inbound("b1", "wallet-b", wagerBody(t, "msg-b1", nil)),
		inbound("b2", "wallet-b", wagerBody(t, "msg-b2", nil)),
	)
	<-started

	stopped := make(chan error, 1)
	go func() { stopped <- h.c.Stop(context.Background()) }()
	waitFor(t, "group b to be given back", func() bool { return len(h.sqs.opsOn("b2")) > 0 })
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if want := []sqsOp{hidden("b1", 0), hidden("b2", 0), deleted("a1")}; !slices.Equal(h.sqs.allOps(), want) {
		t.Errorf("queue calls = %+v, want %+v", h.sqs.allOps(), want)
	}
	if got := h.sub.submitted(); !slices.Equal(got, []string{"msg-a1"}) {
		t.Errorf("submitted = %v, want only msg-a1", got)
	}
	if n := h.c.HeldMessages(); n != 0 {
		t.Errorf("%d messages still held after Stop, want 0", n)
	}
}

func TestConsumer_StopCancelsProcessingPastDrainTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.DrainTimeout = 100 * time.Millisecond
	h := newHarness(cfg)
	started := make(chan struct{})
	var canceledAt atomic.Int64
	h.sub.fn = func(ctx context.Context, _ wager.Command, _ wagering.Source) (wagering.Result, error) {
		close(started)
		<-ctx.Done()
		canceledAt.Store(time.Now().UnixNano())
		return wagering.Result{}, fmt.Errorf("%w: %w", app.ErrUnavailable, ctx.Err())
	}
	h.start(t)
	h.sqs.push(
		inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil)),
		inbound("a2", "wallet-a", wagerBody(t, "msg-a2", nil)),
	)
	<-started

	stopping := time.Now()
	h.stop(t)

	if waited := time.Duration(canceledAt.Load() - stopping.UnixNano()); waited < cfg.DrainTimeout {
		t.Errorf("processing canceled %v after Stop, want after the drain timeout of %v", waited, cfg.DrainTimeout)
	}
	if want := []sqsOp{hidden("a1", 0), hidden("a2", 0)}; !slices.Equal(h.sqs.allOps(), want) {
		t.Errorf("queue calls = %+v, want %+v", h.sqs.allOps(), want)
	}
	if got := h.m.results(); len(got) != 0 {
		t.Errorf("results = %v, want none", got)
	}
}

func TestConsumer_StopThatTimesOutStillEndsEverything(t *testing.T) {
	before := goleak.IgnoreCurrent()
	h := newHarness(testConfig())
	started, release := make(chan struct{}), make(chan struct{})
	h.sub.fn = func(context.Context, wager.Command, wagering.Source) (wagering.Result, error) {
		close(started)
		<-release
		return decided(wager.StatusProcessed), nil
	}
	h.start(t)
	h.sqs.push(inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil)))
	<-started

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := h.c.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want it to give up when its context ends", err)
	}
	close(release)
	waitFor(t, "a1 to be settled", func() bool { return len(h.sqs.opsOn("a1")) > 0 })

	goleak.VerifyNone(t, before)
}

func TestConsumer_ExtenderRenewsOnlyHeldMessages(t *testing.T) {
	cfg := testConfig()
	cfg.VisibilityTimeout = time.Second
	cfg.ProcessingTimeout = 900 * time.Millisecond
	h := newHarness(cfg)
	release := make(chan struct{})
	h.sub.fn = func(_ context.Context, _ wager.Command, src wagering.Source) (wagering.Result, error) {
		if src.CausationID == "msg-a1" {
			<-release
		}
		return decided(wager.StatusProcessed), nil
	}
	h.start(t)
	h.sqs.push(
		inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil)),
		inbound("a2", "wallet-a", wagerBody(t, "msg-a2", nil)),
		inbound("b1", "wallet-b", wagerBody(t, "msg-b1", nil)),
	)
	renewals := func(id string) int {
		n := 0
		for _, op := range h.sqs.opsOn(id) {
			if op == renewed(id, 1) {
				n++
			}
		}
		return n
	}
	waitFor(t, "two renewals of a1 and a2", func() bool { return renewals("a1") >= 2 && renewals("a2") >= 2 })
	close(release)
	waitFor(t, "a2 to be deleted", func() bool { return slices.Contains(h.sqs.opsOn("a2"), deleted("a2")) })
	time.Sleep(twoMoreTicks)
	h.stop(t)

	if got := h.sqs.opsOn("b1"); !slices.Equal(got, []sqsOp{deleted("b1")}) {
		t.Errorf("b1 calls = %+v, want only its delete", got)
	}
	for _, id := range []string{"a1", "a2"} {
		ops := h.sqs.opsOn(id)
		if last := ops[len(ops)-1]; last != deleted(id) {
			t.Errorf("%s calls = %+v, want renewals then the delete, and nothing after", id, ops)
		}
		if slices.Contains(ops[:len(ops)-1], deleted(id)) {
			t.Errorf("%s calls = %+v, want a single delete", id, ops)
		}
	}
}

func TestConsumer_SettleWaitsForRenewalInFlight(t *testing.T) {
	cfg := testConfig()
	cfg.VisibilityTimeout = time.Second
	cfg.ProcessingTimeout = 900 * time.Millisecond
	h := newHarness(cfg)
	releases := map[string]chan struct{}{"msg-a1": make(chan struct{}), "msg-b1": make(chan struct{})}
	h.sub.fn = func(_ context.Context, _ wager.Command, src wagering.Source) (wagering.Result, error) {
		<-releases[src.CausationID]
		return decided(wager.StatusProcessed), nil
	}
	blocked, unblock := make(chan []string, 1), make(chan struct{})
	var once sync.Once
	h.sqs.beforeRenew = func(handles []string) {
		once.Do(func() {
			blocked <- handles
			<-unblock
		})
	}
	h.start(t)
	h.sqs.push(
		inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil)),
		inbound("b1", "wallet-b", wagerBody(t, "msg-b1", nil)),
	)

	select {
	case handles := <-blocked:
		if slices.Sort(handles); !slices.Equal(handles, []string{receipt("a1"), receipt("b1")}) {
			t.Fatalf("renewal of %v, want a1 and b1 in one batch", handles)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a renewal")
	}
	close(releases["msg-b1"])
	time.Sleep(50 * time.Millisecond)
	if ops := h.sqs.opsOn("b1"); len(ops) != 0 {
		t.Errorf("b1 calls while its renewal was in flight = %+v, want none", ops)
	}
	close(unblock)
	waitFor(t, "b1 to be deleted", func() bool { return slices.Contains(h.sqs.opsOn("b1"), deleted("b1")) })
	time.Sleep(twoMoreTicks)
	close(releases["msg-a1"])
	waitFor(t, "both messages deleted", func() bool { return len(h.m.results()) == 2 })
	h.stop(t)

	if got, want := h.sqs.opsOn("b1"), []sqsOp{renewed("b1", 1), deleted("b1")}; !slices.Equal(got, want) {
		t.Errorf("b1 calls = %+v, want %+v: the renewal in flight, then the delete, and nothing after", got, want)
	}
}

func TestConsumer_ExtenderRenewsInBatchesOfTen(t *testing.T) {
	cfg := testConfig()
	cfg.VisibilityTimeout = time.Second
	cfg.ProcessingTimeout = 900 * time.Millisecond
	h := newHarness(cfg)
	release := make(chan struct{})
	h.sub.fn = func(_ context.Context, _ wager.Command, src wagering.Source) (wagering.Result, error) {
		if src.CausationID == "msg-0" {
			<-release
		}
		return decided(wager.StatusProcessed), nil
	}
	h.start(t)
	var batch []types.Message
	var want []string
	for i := range 12 {
		id := strconv.Itoa(i)
		batch = append(batch, inbound(id, "wallet-1", wagerBody(t, "msg-"+id, nil)))
		want = append(want, receipt(id))
	}
	h.sqs.push(batch...)
	waitFor(t, "a tick of renewals", func() bool { return len(h.sqs.renewalBatches()) >= 2 })
	close(release)
	waitFor(t, "every message processed", func() bool { return len(h.m.results()) == 12 })
	h.stop(t)

	tick := h.sqs.renewalBatches()[:2]
	if len(tick[0]) != 10 || len(tick[1]) != 2 {
		t.Errorf("batch sizes = %d and %d, want 10 and 2", len(tick[0]), len(tick[1]))
	}
	got := slices.Sorted(slices.Values(slices.Concat(tick...)))
	if slices.Sort(want); !slices.Equal(got, want) {
		t.Errorf("renewed in the tick = %v, want each held message once: %v", got, want)
	}
}

func TestConsumer_ExtenderLetsGoOfLostMessages(t *testing.T) {
	tests := []struct {
		name          string
		code, message string
		dropped       bool
	}{
		{"invalid handle", "ReceiptHandleIsInvalid", "", true},
		{"not in flight, old spelling", "AWS.SimpleQueueService.MessageNotInflight", "", true},
		{"not in flight", "MessageNotInflight", "", true},
		{"stale handle", "InvalidParameterValue",
			"Value AQEB+x for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change.", true},
		{"stale handle, old spelling", "AWS.SimpleQueueService.InvalidParameterValue",
			"Value AQEB+x for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change.", true},
		{"visibility past the limit", "InvalidParameterValue",
			"Value 43201 for parameter VisibilityTimeout is invalid. Reason: Total VisibilityTimeout for the message is beyond the limit [43200 seconds].", false},
		{"internal error", "InternalError", "We encountered an internal error. Please try again.", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.VisibilityTimeout = time.Second
			cfg.ProcessingTimeout = 900 * time.Millisecond
			h := newHarness(cfg)
			h.sqs.lost = map[string]types.BatchResultErrorEntry{
				receipt("a1"): {Code: aws.String(tt.code), Message: aws.String(tt.message)},
			}
			release := make(chan struct{})
			h.sub.fn = func(context.Context, wager.Command, wagering.Source) (wagering.Result, error) {
				<-release
				return decided(wager.StatusProcessed), nil
			}
			h.start(t)
			h.sqs.push(
				inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil)),
				inbound("b1", "wallet-b", wagerBody(t, "msg-b1", nil)),
			)
			waitFor(t, "three ticks", func() bool { return len(h.sqs.renewalBatches()) >= 3 })
			close(release)
			waitFor(t, "both messages processed", func() bool { return len(h.m.results()) == 2 })
			h.stop(t)

			attempts := 0
			for _, handles := range h.sqs.renewalBatches() {
				if slices.Contains(handles, receipt("a1")) {
					attempts++
				}
			}
			if tt.dropped && attempts != 1 {
				t.Errorf("renewals of a1 = %d, want only the one that found it lost", attempts)
			}
			if !tt.dropped && attempts < 3 {
				t.Errorf("renewals of a1 = %d, want one per tick", attempts)
			}
			if warnings := strings.Count(h.logs.String(), "no longer in flight"); tt.dropped && warnings != 1 {
				t.Errorf("%d warnings about a1 being lost, want 1:\n%s", warnings, h.logs.String())
			}
			if got := renewalsOf(h, "b1"); got < 3 {
				t.Errorf("renewals of b1 = %d, want one per tick", got)
			}
		})
	}
}

func renewalsOf(h *harness, sqsID string) int {
	n := 0
	for _, op := range h.sqs.opsOn(sqsID) {
		if op.kind == "renew" {
			n++
		}
	}
	return n
}

func TestConsumer_CircuitPausesPollingUntilDatabaseAnswers(t *testing.T) {
	cfg := testConfig()
	cfg.MaxGroups = 1
	h := newHarness(cfg)
	events := &eventLog{}
	h.sqs.events = events
	var pings atomic.Int32
	h.ping = func(context.Context) error {
		if pings.Add(1) <= 3 {
			events.add("ping:err")
			return errors.New("connection refused")
		}
		events.add("ping:ok")
		return nil
	}
	h.sub.fn = func(_ context.Context, _ wager.Command, src wagering.Source) (wagering.Result, error) {
		events.add("submit:" + src.CausationID)
		if src.CausationID == "msg-1" {
			return wagering.Result{}, fmt.Errorf("%w: connection refused", app.ErrUnavailable)
		}
		return decided(wager.StatusProcessed), nil
	}
	h.start(t)
	h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil)))
	h.sqs.push(inbound("m2", "wallet-2", wagerBody(t, "msg-2", nil)))
	waitFor(t, "both messages settled", func() bool { return len(h.m.results()) == 2 })
	h.stop(t)

	want := []string{"receive", "submit:msg-1", "ping:err", "ping:err", "ping:err", "ping:ok", "receive", "submit:msg-2"}
	if got := events.all(); len(got) < len(want) || !slices.Equal(got[:len(want)], want) {
		t.Errorf("events = %v, want them to start with %v", got, want)
	}
	if got := h.m.results(); !slices.Equal(got, []string{"retry", "processed"}) {
		t.Errorf("results = %v, want [retry processed]", got)
	}
	if got := h.sqs.opsOn("m1"); !slices.Equal(got, []sqsOp{hidden("m1", 2)}) {
		t.Errorf("m1 calls = %+v, want it hidden for its retry", got)
	}
}

func overlappingGroups(h *harness, err error) {
	b1Started := make(chan struct{})
	h.sub.fn = func(_ context.Context, _ wager.Command, src wagering.Source) (wagering.Result, error) {
		switch src.CausationID {
		case "msg-a1":
			<-b1Started
			return wagering.Result{}, err
		case "msg-b1":
			close(b1Started)
			for deadline := time.Now().Add(5 * time.Second); len(h.sqs.opsOn("a1")) == 0; {
				if time.Now().After(deadline) {
					return wagering.Result{}, errors.New("a1 was never settled")
				}
				time.Sleep(time.Millisecond)
			}
		}
		return decided(wager.StatusProcessed), nil
	}
}

func pushOverlappingGroups(t *testing.T, h *harness) {
	t.Helper()
	h.sqs.push(
		inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil)),
		inbound("b1", "wallet-b", wagerBody(t, "msg-b1", nil)),
		inbound("b2", "wallet-b", wagerBody(t, "msg-b2", nil)),
		inbound("a2", "wallet-a", wagerBody(t, "msg-a2", nil)),
	)
}

func TestConsumer_UnavailableWithDatabaseAnsweringRetriesOnlyThatMessage(t *testing.T) {
	h := newHarness(testConfig())
	var pings atomic.Int32
	h.ping = func(context.Context) error {
		pings.Add(1)
		return nil
	}
	overlappingGroups(h, fmt.Errorf("%w: %w", app.ErrUnavailable, context.DeadlineExceeded))
	h.start(t)

	pushOverlappingGroups(t, h)
	waitFor(t, "group a to stop and group b to end", func() bool {
		return len(h.sqs.opsOn("a2")) > 0 && len(h.sqs.opsOn("b2")) > 0
	})
	h.sqs.push(inbound("c1", "wallet-c", wagerBody(t, "msg-c1", nil)))
	waitFor(t, "c1 to be processed", func() bool { return len(h.sqs.opsOn("c1")) > 0 })
	h.stop(t)

	for id, want := range map[string][]sqsOp{
		"a1": {hidden("a1", 2)},
		"a2": {hidden("a2", 0)},
		"b1": {deleted("b1")},
		"b2": {deleted("b2")},
		"c1": {deleted("c1")},
	} {
		if got := h.sqs.opsOn(id); !slices.Equal(got, want) {
			t.Errorf("%s calls = %+v, want %+v", id, got, want)
		}
	}
	if n := pings.Load(); n != 1 {
		t.Errorf("pings = %d, want 1: the one that told the failure was not the database's", n)
	}
	if strings.Contains(h.logs.String(), "polling paused") {
		t.Errorf("polling was paused for a database that answers:\n%s", h.logs.String())
	}
}

func TestConsumer_UnavailableWithDatabaseDownOpensCircuit(t *testing.T) {
	h := newHarness(testConfig())
	var pings atomic.Int32
	h.ping = func(context.Context) error {
		if pings.Add(1) <= 2 {
			return errors.New("connection refused")
		}
		return nil
	}
	overlappingGroups(h, fmt.Errorf("%w: connection refused", app.ErrUnavailable))
	h.start(t)

	pushOverlappingGroups(t, h)
	waitFor(t, "b2 to be given back", func() bool { return len(h.sqs.opsOn("b2")) > 0 })
	if n := pings.Load(); n != 1 {
		t.Errorf("pings before polling paused = %d, want 1: the one that found the database down", n)
	}
	h.sqs.emptyLongPoll()
	waitFor(t, "the database to answer again", func() bool { return pings.Load() >= 3 })
	h.stop(t)

	for id, want := range map[string][]sqsOp{
		"a1": {hidden("a1", 2)},
		"a2": {hidden("a2", 0)},
		"b1": {deleted("b1")},
		"b2": {hidden("b2", 0)},
	} {
		if got := h.sqs.opsOn(id); !slices.Equal(got, want) {
			t.Errorf("%s calls = %+v, want %+v", id, got, want)
		}
	}
	if got := h.sub.submitted(); slices.Contains(got, "msg-b2") {
		t.Errorf("submitted = %v, want msg-b2 given back untried while the circuit was open", got)
	}
	if !strings.Contains(h.logs.String(), "polling paused") {
		t.Errorf("no log of the paused polling in:\n%s", h.logs.String())
	}
}

func TestConsumer_ProviderMismatchNeverReachesSubmit(t *testing.T) {
	h := newHarness(testConfig())
	h.start(t)
	h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", func(d map[string]any) { d["providerId"] = "provider-b" })))
	waitFor(t, "the message to be settled", func() bool { return len(h.m.results()) > 0 })
	h.stop(t)

	if n := len(h.sub.all()); n != 0 {
		t.Errorf("Submit called %d times, want never", n)
	}
	var warned bool
	for _, line := range h.logs.lines(t) {
		if line["security"] == true {
			warned = line["level"] == "WARN" && line["providerId"] == "provider-a" &&
				line["claimedProviderId"] == "provider-b" && line["messageId"] == "msg-1"
		}
	}
	if !warned {
		t.Errorf("no security warning naming both providers in:\n%s", h.logs.String())
	}
}

func TestConsumer_DeadLetterMoveFailureRetries(t *testing.T) {
	h := newHarness(testConfig())
	h.dlq.err = errors.New("throttled")
	h.start(t)
	h.sqs.push(
		inbound("a1", "wallet-a", "not json"),
		inbound("a2", "wallet-a", wagerBody(t, "msg-a2", nil)),
	)
	waitFor(t, "a2 to be given back", func() bool { return len(h.sqs.opsOn("a2")) > 0 })
	h.stop(t)

	if want := []sqsOp{hidden("a1", 2), hidden("a2", 0)}; !slices.Equal(h.sqs.allOps(), want) {
		t.Errorf("queue calls = %+v, want %+v", h.sqs.allOps(), want)
	}
	if got := h.m.results(); !slices.Equal(got, []string{"retry"}) {
		t.Errorf("results = %v, want [retry]", got)
	}
	if got := h.m.dlqReasons(); len(got) != 0 {
		t.Errorf("DLQ reasons = %v, want none: nothing reached the DLQ", got)
	}
}

func TestConsumer_DeleteFailureEndsGroup(t *testing.T) {
	h := newHarness(testConfig())
	h.sqs.deleteErr = errors.New("throttled")
	h.start(t)
	h.sqs.push(
		inbound("a1", "wallet-a", wagerBody(t, "msg-a1", nil)),
		inbound("a2", "wallet-a", wagerBody(t, "msg-a2", nil)),
	)
	waitFor(t, "a2 to be given back", func() bool { return len(h.sqs.opsOn("a2")) > 0 })
	h.stop(t)

	if got := h.sub.submitted(); !slices.Equal(got, []string{"msg-a1"}) {
		t.Errorf("submitted = %v, want only msg-a1", got)
	}
	if want := []sqsOp{deleted("a1"), hidden("a2", 0)}; !slices.Equal(h.sqs.allOps(), want) {
		t.Errorf("queue calls = %+v, want %+v", h.sqs.allOps(), want)
	}
	if got := h.m.results(); !slices.Equal(got, []string{"retry"}) {
		t.Errorf("results = %v, want [retry]: a1 stays in the queue, and a2 was given back untried", got)
	}
}

func TestConsumer_ReceiveFailureIsRetried(t *testing.T) {
	h := newHarness(testConfig())
	h.start(t)
	h.sqs.batches <- receiveResult{err: errors.New("throttled")}
	h.sqs.push(inbound("m1", "wallet-1", wagerBody(t, "msg-1", nil)))
	waitFor(t, "the message to be processed", func() bool { return len(h.m.results()) > 0 })
	h.stop(t)

	if got := h.sqs.opsOn("m1"); !slices.Equal(got, []sqsOp{deleted("m1")}) {
		t.Errorf("m1 calls = %+v, want it deleted", got)
	}
	if !strings.Contains(h.logs.String(), "throttled") {
		t.Errorf("the receive failure was not logged:\n%s", h.logs.String())
	}
}

func fakeDeps(ping func(context.Context) error) sqsin.Deps {
	return sqsin.Deps{
		Client: newFakeSQS(), Identity: fakeIdentity{}, DLQ: &fakeDLQ{}, Submitter: &fakeSubmitter{}, Ping: ping, Metrics: &fakeMetrics{},
	}
}

func TestConsumer_StartRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		edit func(*config.ConsumerConfig)
	}{
		{"no name", func(c *config.ConsumerConfig) { c.Name = "" }},
		{"no group", func(c *config.ConsumerConfig) { c.MaxGroups = 0 }},
		{"visibility under a second", func(c *config.ConsumerConfig) { c.VisibilityTimeout = 500 * time.Millisecond }},
		{"processing outlasts visibility", func(c *config.ConsumerConfig) { c.ProcessingTimeout = c.VisibilityTimeout }},
		{"no processing time", func(c *config.ConsumerConfig) { c.ProcessingTimeout = 0 }},
		{"long poll past 20s", func(c *config.ConsumerConfig) { c.WaitTime = 21 * time.Second }},
		{"no long poll", func(c *config.ConsumerConfig) { c.WaitTime = 0 }},
		{"long poll under a second", func(c *config.ConsumerConfig) { c.WaitTime = 999 * time.Millisecond }},
		{"negative drain", func(c *config.ConsumerConfig) { c.DrainTimeout = -time.Second }},
		{"no receive", func(c *config.ConsumerConfig) { c.MaxReceives = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.edit(&cfg)
			c := sqsin.NewConsumer(cfg, testQueues, fakeDeps(func(context.Context) error { return nil }))
			if err := c.Start(t.Context()); err == nil {
				_ = c.Stop(t.Context())
				t.Fatal("Start succeeded, want an error")
			}
		})
	}
	t.Run("no ping", func(t *testing.T) {
		c := sqsin.NewConsumer(testConfig(), testQueues, fakeDeps(nil))
		if err := c.Start(t.Context()); err == nil {
			_ = c.Stop(t.Context())
			t.Fatal("Start succeeded, want an error")
		}
	})
}

func TestConsumer_StartAndStopLifecycle(t *testing.T) {
	c := sqsin.NewConsumer(testConfig(), testQueues, fakeDeps(func(context.Context) error { return nil }))
	if err := c.Stop(t.Context()); err != nil {
		t.Fatalf("Stop before Start = %v, want nil", err)
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := c.Start(t.Context()); err == nil {
		t.Error("second Start succeeded, want an error")
	}
	for i := range 2 {
		if err := c.Stop(t.Context()); err != nil {
			t.Fatalf("Stop #%d: %v", i+1, err)
		}
	}
}
