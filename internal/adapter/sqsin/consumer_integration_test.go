//go:build integration

package sqsin_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/awsclient"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var (
	kcOnce sync.Once
	kc     *testenv.Keycloak
	errKC  error

	pgOnce   sync.Once
	sharedPG *testenv.Postgres
	errPG    error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if kc != nil {
		if err := kc.Terminate(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	if sharedPG != nil {
		if err := sharedPG.Terminate(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	os.Exit(code)
}

func keycloak(t *testing.T) *testenv.Keycloak {
	t.Helper()
	kcOnce.Do(func() { kc, errKC = testenv.StartKeycloak(context.Background(), "") })
	if errKC != nil {
		t.Fatalf("StartKeycloak: %v", errKC)
	}
	return kc
}

type queueEnv struct {
	ms       *testenv.MiniStack
	provider *sqs.Client
	app      *sqs.Client
	identity sqsin.TokenAttributeIdentity
	token    string
}

func newQueueEnv(t *testing.T) *queueEnv {
	t.Helper()
	ctx := t.Context()
	ms, err := testenv.StartMiniStack(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("StartMiniStack: %v", err)
	}
	t.Cleanup(func() { _ = ms.Terminate(context.Background()) })
	provider, err := ms.Client(ctx, "provider-a")
	if err != nil {
		t.Fatalf("provider client: %v", err)
	}
	appClient, err := ms.Client(ctx, "wallet-app")
	if err != nil {
		t.Fatalf("wallet-app client: %v", err)
	}

	k := keycloak(t)
	v := oidc.NewVerifier(config.OIDCConfig{Issuer: k.Issuer, JWKSURL: k.JWKSURL, Audience: "wagering-api", ClockSkew: 30 * time.Second}, nil)
	if err := v.Start(ctx); err != nil {
		t.Fatalf("verifier Start: %v", err)
	}
	t.Cleanup(func() { _ = v.Stop(context.Background()) })
	token, err := k.Token(ctx, "provider-a", "provider-a-secret")
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	return &queueEnv{ms: ms, provider: provider, app: appClient, identity: sqsin.TokenAttributeIdentity{Verifier: v}, token: token}
}

func (e *queueEnv) send(t *testing.T, client *sqs.Client, queueURL, groupID, dedupID, body string) string {
	t.Helper()
	out, err := client.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(groupID),
		MessageDeduplicationId: aws.String(dedupID),
		MessageAttributes:      map[string]types.MessageAttributeValue{sqsin.ProviderTokenAttribute: stringAttr(e.token)},
	})
	if err != nil {
		t.Fatalf("SendMessage %s: %v", dedupID, err)
	}
	return aws.ToString(out.MessageId)
}

func (e *queueEnv) start(t *testing.T, cfg config.ConsumerConfig, client sqsin.SQSAPI, dlqClient sqsin.DLQClient, q awsclient.Queues, sub sqsin.Submitter, m sqsin.Metrics) *sqsin.Consumer {
	t.Helper()
	c, _ := e.run(t, cfg, consumerRun{client: client, dlqClient: dlqClient, queues: q, sub: sub, metrics: m})
	return c
}

type consumerRun struct {
	client    sqsin.SQSAPI
	dlqClient sqsin.DLQClient
	queues    awsclient.Queues
	sub       sqsin.Submitter
	metrics   sqsin.Metrics
	ping      func(context.Context) error
	pingEvery time.Duration
}

func (e *queueEnv) run(t *testing.T, cfg config.ConsumerConfig, r consumerRun) (*sqsin.Consumer, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("consumer logs:\n%s", logs.String())
		}
	})
	ping, pingEvery := r.ping, r.pingEvery
	if ping == nil {
		ping = func(context.Context) error { return nil }
	}
	if pingEvery == 0 {
		pingEvery = 100 * time.Millisecond
	}
	c := sqsin.NewConsumer(cfg, r.queues, sqsin.Deps{
		Client:    r.client,
		Identity:  e.identity,
		DLQ:       sqsin.NewDLQMover(r.dlqClient, r.queues.Input, r.queues.InputDLQ),
		Submitter: r.sub,
		Ping:      ping,
		Metrics:   r.metrics,
		Log:       logging.New(logs, slog.LevelDebug, "it"),
	})
	c.SetRetryTiming(pingEvery, 100*time.Millisecond)
	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Stop(ctx)
	})
	return c, logs
}

func stopConsumer(t *testing.T, c *sqsin.Consumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := c.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func receiveAll(t *testing.T, root *sqs.Client, queueURL string, wait time.Duration) []types.Message {
	t.Helper()
	out, err := root.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(queueURL),
		MaxNumberOfMessages:         10,
		WaitTimeSeconds:             int32(wait / time.Second),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		MessageAttributeNames:       []string{"All"},
	})
	if err != nil {
		t.Fatalf("ReceiveMessage %s: %v", queueURL, err)
	}
	return out.Messages
}

func requireEmpty(t *testing.T, root *sqs.Client, queueURL string) {
	t.Helper()
	attrs, err := root.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes: %v", err)
	}
	for name, v := range attrs.Attributes {
		if v != "0" {
			t.Errorf("%s of %s = %s, want 0", name, queueURL, v)
		}
	}
}

type batchRecorder struct {
	sqsin.SQSAPI

	mu       sync.Mutex
	receives int
	batches  [][]types.Message
}

func (r *batchRecorder) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	r.mu.Lock()
	r.receives++
	r.mu.Unlock()
	out, err := r.SQSAPI.ReceiveMessage(ctx, in, optFns...)
	if err == nil && len(out.Messages) > 0 {
		r.mu.Lock()
		r.batches = append(r.batches, out.Messages)
		r.mu.Unlock()
	}
	return out, err
}

func (r *batchRecorder) receiveCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.receives
}

func (r *batchRecorder) sizes() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	sizes := make([]int, len(r.batches))
	for i, b := range r.batches {
		sizes[i] = len(b)
	}
	return sizes
}

func (r *batchRecorder) receiveCounts(t *testing.T, messageID string) []string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	var counts []string
	for _, b := range r.batches {
		for _, msg := range b {
			if env, _, err := sqsin.ParseEnvelope([]byte(aws.ToString(msg.Body))); err == nil && env.MessageID == messageID {
				counts = append(counts, msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
			}
		}
	}
	return counts
}

func (r *batchRecorder) deliveries() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var got []string
	for _, b := range r.batches {
		for _, msg := range b {
			got = append(got, aws.ToString(msg.MessageId)+" receive "+
				msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
		}
	}
	return got
}

func integrationConfig() config.ConsumerConfig {
	cfg := testConfig()
	cfg.WaitTime = time.Second
	return cfg
}

func TestConsumer_InvalidMessageToDLQ(t *testing.T) {
	e := newQueueEnv(t)
	body := wagerBody(t, "msg-1", func(d map[string]any) { d["externalTransactionId"] = "tx 1" })
	sqsID := e.send(t, e.provider, e.ms.Queues.Input, "wallet-1", "msg-1", body)
	sub, m := &fakeSubmitter{}, &fakeMetrics{}

	c := e.start(t, integrationConfig(), e.app, e.app, e.ms.Queues, sub, m)
	waitWithin(t, 20*time.Second, "the message to be dead-lettered", func() bool { return len(m.results()) > 0 })
	stopConsumer(t, c)

	root := e.ms.RootClient()
	moved := receiveAll(t, root, e.ms.Queues.InputDLQ, 2*time.Second)
	if len(moved) != 1 {
		t.Fatalf("DLQ holds %d messages, want 1", len(moved))
	}
	dl := moved[0]
	if aws.ToString(dl.Body) != body {
		t.Errorf("DLQ body = %s, want the original", aws.ToString(dl.Body))
	}
	if g := dl.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]; g != "wallet-1" {
		t.Errorf("DLQ group = %q, want wallet-1", g)
	}
	for name, want := range map[string]string{
		sqsin.FailureReasonAttribute:   sqsin.ReasonInvalidMessage,
		sqsin.FailureCodeAttribute:     string(wager.ValidationInvalidRequest),
		sqsin.SourceMessageIDAttribute: sqsID,
	} {
		if got := aws.ToString(dl.MessageAttributes[name].StringValue); got != want {
			t.Errorf("DLQ attribute %s = %q, want %q", name, got, want)
		}
	}
	requireEmpty(t, root, e.ms.Queues.Input)
	if n := len(sub.all()); n != 0 {
		t.Errorf("Submit called %d times, want never", n)
	}
	if got := m.dlqReasons(); !slices.Equal(got, []string{sqsin.ReasonInvalidMessage}) {
		t.Errorf("DLQ reasons = %v, want [%s]", got, sqsin.ReasonInvalidMessage)
	}
}

func TestConsumer_StopReleasesUnstarted(t *testing.T) {
	e := newQueueEnv(t)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("msg-%d", i)
		e.send(t, e.provider, e.ms.Queues.Input, "wallet-1", id, wagerBody(t, id, nil))
	}
	started := make(chan struct{})
	sub, m := &fakeSubmitter{}, &fakeMetrics{}
	sub.fn = func(_ context.Context, _ wager.Command, src wagering.Source) (wagering.Result, error) {
		if src.CausationID == "msg-1" {
			close(started)
			time.Sleep(time.Second)
		}
		return decided(wager.StatusProcessed), nil
	}

	rec := &batchRecorder{SQSAPI: e.app}

	c := e.start(t, integrationConfig(), rec, e.app, e.ms.Queues, sub, m)
	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the first message")
	}
	stopConsumer(t, c)

	if got := rec.sizes(); !slices.Equal(got, []int{3}) {
		t.Fatalf("batches received = %v, want the three messages in one", got)
	}

	got := receiveAll(t, e.ms.RootClient(), e.ms.Queues.Input, 2*time.Second)
	var ids []string
	for _, msg := range got {
		env, _, err := sqsin.ParseEnvelope([]byte(aws.ToString(msg.Body)))
		if err != nil {
			t.Fatalf("ParseEnvelope: %v", err)
		}
		ids = append(ids, env.MessageID)
	}
	if want := []string{"msg-2", "msg-3"}; !slices.Equal(ids, want) {
		t.Errorf("receivable right after Stop: %v, want %v (visibility timeout %s)", ids, want, integrationConfig().VisibilityTimeout)
	}
	if got := sub.submitted(); !slices.Equal(got, []string{"msg-1"}) {
		t.Errorf("submitted = %v, want only msg-1", got)
	}
	if got := m.results(); !slices.Equal(got, []string{"processed"}) {
		t.Errorf("results = %v, want [processed]", got)
	}
}

func TestConsumer_VisibilityExtendedForSlowGroup(t *testing.T) {
	e := newQueueEnv(t)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("msg-%d", i)
		e.send(t, e.provider, e.ms.Queues.Input, "wallet-1", id, wagerBody(t, id, nil))
	}
	sub, m := &fakeSubmitter{}, &fakeMetrics{}
	sub.fn = func(ctx context.Context, _ wager.Command, _ wagering.Source) (wagering.Result, error) {
		select {
		case <-time.After(8 * time.Second):
		case <-ctx.Done():
			return wagering.Result{}, ctx.Err()
		}
		return decided(wager.StatusProcessed), nil
	}
	cfg := integrationConfig()
	cfg.VisibilityTimeout = 10 * time.Second
	cfg.ProcessingTimeout = 9 * time.Second
	rec := &batchRecorder{SQSAPI: e.app}

	c := e.start(t, cfg, rec, e.app, e.ms.Queues, sub, m)
	waitWithin(t, 45*time.Second, "three messages processed", func() bool { return len(m.results()) >= 3 })
	time.Sleep(3 * time.Second)
	stopConsumer(t, c)

	if got := rec.sizes(); len(got) == 0 || got[0] != 3 {
		t.Fatalf("batches received = %v, want the three messages in the first one", got)
	}
	if got, want := sub.submitted(), []string{"msg-1", "msg-2", "msg-3"}; !slices.Equal(got, want) {
		t.Errorf("submitted = %v, want each message once, in order: %v", got, want)
	}
	if got := m.results(); !slices.Equal(got, []string{"processed", "processed", "processed"}) {
		t.Errorf("results = %v, want three processed", got)
	}
	requireEmpty(t, e.ms.RootClient(), e.ms.Queues.Input)
}

func scratchQueues(t *testing.T, root *sqs.Client, name string, maxReceives int) awsclient.Queues {
	t.Helper()
	ctx := t.Context()
	fifo := map[string]string{
		string(types.QueueAttributeNameFifoQueue):                 "true",
		string(types.QueueAttributeNameContentBasedDeduplication): "false",
	}
	dlq, err := root.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name + "-dlq.fifo"), Attributes: fifo})
	if err != nil {
		t.Fatalf("CreateQueue %s-dlq: %v", name, err)
	}
	got, err := root.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       dlq.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes %s-dlq: %v", name, err)
	}
	redrive, err := json.Marshal(map[string]string{
		"deadLetterTargetArn": got.Attributes[string(types.QueueAttributeNameQueueArn)],
		"maxReceiveCount":     strconv.Itoa(maxReceives),
	})
	if err != nil {
		t.Fatalf("encode redrive policy: %v", err)
	}
	attrs := map[string]string{string(types.QueueAttributeNameRedrivePolicy): string(redrive)}
	for k, v := range fifo {
		attrs[k] = v
	}
	in, err := root.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name + ".fifo"), Attributes: attrs})
	if err != nil {
		t.Fatalf("CreateQueue %s: %v", name, err)
	}
	return awsclient.Queues{Input: aws.ToString(in.QueueUrl), InputDLQ: aws.ToString(dlq.QueueUrl)}
}

func TestConsumer_RetriesExhaustedToDLQ(t *testing.T) {
	const maxReceives = 3
	e := newQueueEnv(t)
	head := wagerBody(t, "msg-1", nil)
	headID := e.send(t, e.provider, e.ms.Queues.Input, "wallet-1", "msg-1", head)
	e.send(t, e.provider, e.ms.Queues.Input, "wallet-1", "msg-2", wagerBody(t, "msg-2", nil))
	sub, m := &fakeSubmitter{}, &fakeMetrics{}
	sub.fn = func(_ context.Context, _ wager.Command, src wagering.Source) (wagering.Result, error) {
		if src.CausationID == "msg-1" {
			return wagering.Result{}, errors.New("a failure that should have passed")
		}
		return decided(wager.StatusProcessed), nil
	}
	cfg := integrationConfig()
	cfg.MaxReceives = maxReceives
	rec := &batchRecorder{SQSAPI: e.app}

	c := e.start(t, cfg, rec, e.app, e.ms.Queues, sub, m)
	waitWithin(t, 30*time.Second, "both messages settled", func() bool { return len(m.results()) == maxReceives+1 })
	stopConsumer(t, c)

	if got, want := sub.submitted(), []string{"msg-1", "msg-1", "msg-1", "msg-2"}; !slices.Equal(got, want) {
		t.Errorf("submitted = %v, want %v", got, want)
	}
	if got, want := m.results(), []string{"retry", "retry", "dlq", "processed"}; !slices.Equal(got, want) {
		t.Errorf("results = %v, want %v", got, want)
	}
	if got, want := rec.receiveCounts(t, "msg-2"), []string{"1", "2", "3"}; !slices.Equal(got, want) {
		t.Errorf("receive counts of msg-2 = %v, want %v: received with the head every time", got, want)
	}
	root := e.ms.RootClient()
	moved := receiveAll(t, root, e.ms.Queues.InputDLQ, 2*time.Second)
	if len(moved) != 1 || aws.ToString(moved[0].Body) != head {
		t.Fatalf("DLQ = %d messages, want the head alone", len(moved))
	}
	for name, want := range map[string]string{
		sqsin.FailureReasonAttribute:   sqsin.ReasonRetriesExhausted,
		sqsin.SourceMessageIDAttribute: headID,
	} {
		if got := aws.ToString(moved[0].MessageAttributes[name].StringValue); got != want {
			t.Errorf("DLQ attribute %s = %q, want %q", name, got, want)
		}
	}
	requireEmpty(t, root, e.ms.Queues.Input)
}

func TestConsumer_RedriveAfterMaxReceives(t *testing.T) {
	const maxReceives = 2
	e := newQueueEnv(t)
	root := e.ms.RootClient()
	q := scratchQueues(t, root, "redrive", maxReceives)
	body := wagerBody(t, "msg-1", nil)
	e.send(t, root, q.Input, "wallet-1", "msg-1", body)
	sub, m := &fakeSubmitter{}, &fakeMetrics{}
	sub.fn = submitErr(fmt.Errorf("%w: connection refused", app.ErrUnavailable))
	cfg := integrationConfig()
	cfg.MaxReceives = maxReceives + 1

	c := e.start(t, cfg, root, root, q, sub, m)
	var redriven []types.Message
	waitWithin(t, 30*time.Second, "the message to be redriven", func() bool {
		redriven = receiveAll(t, root, q.InputDLQ, time.Second)
		return len(redriven) > 0
	})
	stopConsumer(t, c)

	if len(redriven) != 1 || aws.ToString(redriven[0].Body) != body {
		t.Fatalf("DLQ = %d messages, want the original one", len(redriven))
	}
	if got := sub.submitted(); !slices.Equal(got, []string{"msg-1", "msg-1"}) {
		t.Errorf("submitted = %v, want msg-1 once per receive, %d times", got, maxReceives)
	}
	if got := m.results(); !slices.Equal(got, []string{"retry", "retry"}) {
		t.Errorf("results = %v, want two retries and no DLQ move by the consumer", got)
	}
	if got := m.dlqReasons(); len(got) != 0 {
		t.Errorf("DLQ reasons = %v, want none: the redrive policy moved it", got)
	}
	requireEmpty(t, root, q.Input)
}
