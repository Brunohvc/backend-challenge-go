//go:build integration

package outboxrelay_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsout"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
	"github.com/Brunohvc/backend-challenge-go/internal/worker"
	"github.com/Brunohvc/backend-challenge-go/internal/worker/outboxrelay"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var (
	env               *testenv.Env
	pool              *pgxpool.Pool
	uow               *postgres.UnitOfWork
	appSQS, readerSQS *sqs.Client
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var err error
	if env, err = testenv.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() {
		if err := env.Terminate(context.WithoutCancel(ctx)); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()

	pool, err = postgres.NewPool(ctx, config.DBConfig{
		URL: env.PG.AppDSN, MaxConns: 10, LockTimeout: 2 * time.Second, StatementTimeout: 5 * time.Second,
	}, "outboxrelay-it")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer pool.Close()
	uow = postgres.NewUnitOfWork(pool, tracing.Tracer{})

	if appSQS, err = env.AWS.Client(ctx, "wallet-app"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if readerSQS, err = env.AWS.Client(ctx, "events-reader"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}

var itConfig = config.OutboxConfig{
	Enabled:      true,
	PollInterval: 10 * time.Millisecond,
	BatchSize:    50,
	Lease:        30 * time.Second,
	BackoffMax:   5 * time.Minute,
}

const waitFor = 15 * time.Second

func resetDB(t *testing.T) {
	t.Helper()
	if err := env.PG.Reset(t.Context()); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

func testLog(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func newRelay(t *testing.T, pub outboxrelay.Publisher, m outboxrelay.Metrics) *outboxrelay.Relay {
	t.Helper()
	return outboxrelay.New(uow, pub, "it", itConfig, m, tracing.Tracer{}, testLog(t))
}

func eventsPublisher(t *testing.T) *sqsout.Publisher {
	return sqsout.NewPublisher(appSQS, env.AWS.Queues, tracing.Tracer{}, testLog(t))
}

func appendEvents(t *testing.T, wallets ...uuid.UUID) []app.OutboxRecord {
	t.Helper()
	return appendEventsIn(t.Context(), t, uow, wallets...)
}

func appendEventsIn(ctx context.Context, t *testing.T, u app.UnitOfWork, wallets ...uuid.UUID) []app.OutboxRecord {
	t.Helper()
	recs := make([]app.OutboxRecord, len(wallets))
	versions := make(map[uuid.UUID]int64)
	for i, w := range wallets {
		versions[w]++
		amount, err := money.FromMinor(100, money.BRL())
		if err != nil {
			t.Fatalf("money: %v", err)
		}
		before, err := money.FromMinor(10_000-100*(versions[w]-1), money.BRL())
		if err != nil {
			t.Fatalf("money: %v", err)
		}
		after, err := before.Sub(amount)
		if err != nil {
			t.Fatalf("money: %v", err)
		}
		e, err := event.NewWalletBalanceChanged(event.Meta{
			EventID:       idgen.V7{}.New(),
			WalletID:      w,
			CorrelationID: fmt.Sprintf("corr-%d", i),
			OccurredAt:    time.Date(2026, 10, 16, 12, 0, i, 0, time.UTC),
		}, event.WalletBalanceChangedData{
			WalletID:      w.String(),
			TransactionID: idgen.V7{}.New().String(),
			Direction:     "DEBIT",
			Money:         amount,
			BalanceBefore: before,
			BalanceAfter:  after,
			WalletVersion: versions[w] + 1,
		})
		if err != nil {
			t.Fatalf("NewWalletBalanceChanged: %v", err)
		}
		if recs[i], err = app.NewOutboxRecord(e); err != nil {
			t.Fatalf("NewOutboxRecord: %v", err)
		}
	}
	err := u.Do(ctx, func(ctx context.Context, r app.Repos) error {
		return r.Outbox().Append(ctx, recs...)
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return recs
}

func newWallets(n int) []uuid.UUID {
	ids := make([]uuid.UUID, n)
	for i := range ids {
		ids[i] = idgen.V7{}.New()
	}
	return ids
}

func drain(t *testing.T, relay *outboxrelay.Relay) int {
	t.Helper()
	for passes := 0; ; passes++ {
		didWork, err := relay.RunOnce(t.Context())
		if err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		if !didWork {
			return passes
		}
		if passes > 1_000 {
			t.Fatal("the relay keeps finding work")
		}
	}
}

func runUntilClaimed(t *testing.T, relay *outboxrelay.Relay) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for {
		didWork, err := relay.RunOnce(t.Context())
		if err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		if didWork {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the relay claimed nothing within %s", waitFor)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func claimAs(t *testing.T, owner string, lease time.Duration) []app.OutboxRecord {
	t.Helper()
	var recs []app.OutboxRecord
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		recs, err = r.Outbox().ClaimHeads(ctx, owner, lease, itConfig.BatchSize)
		return err
	})
	if err != nil {
		t.Fatalf("ClaimHeads(%s): %v", owner, err)
	}
	return recs
}

type outboxRow struct {
	attempts    int
	published   bool
	locked      bool
	dueInFuture bool
	dueWithin   time.Duration
	lastError   string
}

func readRow(t *testing.T, id uuid.UUID) outboxRow {
	t.Helper()
	var r outboxRow
	var due time.Duration
	err := pool.QueryRow(t.Context(), `
SELECT attempts, published_at IS NOT NULL, locked_by IS NOT NULL OR locked_until IS NOT NULL,
       next_attempt_at > now(), next_attempt_at - now(), COALESCE(last_error, '')
  FROM outbox_events WHERE id = $1`, id).
		Scan(&r.attempts, &r.published, &r.locked, &r.dueInFuture, &due, &r.lastError)
	if err != nil {
		t.Fatalf("read outbox row %s: %v", id, err)
	}
	r.dueWithin = due
	return r
}

func requireAllPublished(t *testing.T, recs []app.OutboxRecord) {
	t.Helper()
	for _, rec := range recs {
		if row := readRow(t, rec.EventID); !row.published || row.locked {
			t.Errorf("event %s: published %t, leased %t; want published and released", rec.EventID, row.published, row.locked)
		}
	}
}

type message struct {
	eventID       string
	aggregateID   string
	body          string
	groupID       string
	dedupID       string
	eventType     string
	eventVersion  string
	versionType   string
	correlationID string
	traceParent   string
}

func receive(t *testing.T, recs []app.OutboxRecord, want int) []message {
	t.Helper()
	ours := make(map[string]bool, len(recs))
	for _, r := range recs {
		ours[r.EventID.String()] = true
	}
	var got []message
	deadline := time.Now().Add(waitFor)
	settled := time.Time{}
	for {
		switch {
		case len(got) >= want && settled.IsZero():
			settled = time.Now()
		case !settled.IsZero() && time.Since(settled) > time.Second:
			return got
		case time.Now().After(deadline):
			t.Fatalf("received %d of %d messages within %s", len(got), want, waitFor)
		}
		out, err := readerSQS.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(env.AWS.Queues.Events),
			MaxNumberOfMessages:   10,
			WaitTimeSeconds:       1,
			MessageAttributeNames: []string{"All"},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameMessageGroupId, types.MessageSystemAttributeNameMessageDeduplicationId,
			},
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		for _, m := range out.Messages {
			if _, err := readerSQS.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{
				QueueUrl: aws.String(env.AWS.Queues.Events), ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				t.Fatalf("DeleteMessage: %v", err)
			}
			msg := toMessage(t, m)
			if ours[msg.eventID] {
				got = append(got, msg)
			}
		}
	}
}

func toMessage(t *testing.T, m types.Message) message {
	t.Helper()
	var body struct {
		EventID     string `json:"eventId"`
		AggregateID string `json:"aggregateId"`
	}
	if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &body); err != nil {
		t.Fatalf("message body is not an envelope: %v", err)
	}
	attr := func(name string) types.MessageAttributeValue { return m.MessageAttributes[name] }
	return message{
		eventID:       body.EventID,
		aggregateID:   body.AggregateID,
		body:          aws.ToString(m.Body),
		groupID:       m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)],
		dedupID:       m.Attributes[string(types.MessageSystemAttributeNameMessageDeduplicationId)],
		eventType:     aws.ToString(attr(sqsout.EventTypeAttribute).StringValue),
		eventVersion:  aws.ToString(attr(sqsout.EventVersionAttribute).StringValue),
		versionType:   aws.ToString(attr(sqsout.EventVersionAttribute).DataType),
		correlationID: aws.ToString(attr(sqsout.CorrelationIDAttribute).StringValue),
		traceParent:   aws.ToString(attr(sqsout.TraceParentAttribute).StringValue),
	}
}

func byAggregate(msgs []message) map[string][]string {
	out := make(map[string][]string)
	for _, m := range msgs {
		out[m.aggregateID] = append(out[m.aggregateID], m.eventID)
	}
	return out
}

func appendOrder(recs []app.OutboxRecord) map[string][]string {
	out := make(map[string][]string)
	for _, r := range recs {
		out[r.AggregateID] = append(out[r.AggregateID], r.EventID.String())
	}
	return out
}

func requireOrder(t *testing.T, got []message, recs []app.OutboxRecord) {
	t.Helper()
	want := appendOrder(recs)
	gotOrder := byAggregate(got)
	if len(gotOrder) != len(want) {
		t.Errorf("received events of %d aggregates, want %d", len(gotOrder), len(want))
	}
	for agg, ids := range want {
		if !slices.Equal(gotOrder[agg], ids) {
			t.Errorf("aggregate %s: received %v, want %v in append order", agg, gotOrder[agg], ids)
		}
	}
}

func TestRelay_PublishesInAggregateOrder(t *testing.T) {
	resetDB(t)
	w := newWallets(2)
	a, b := w[0], w[1]
	recs := appendEvents(t, a, b, a, a, b)
	m := newMetrics()
	relay := newRelay(t, eventsPublisher(t), m)

	if passes := drain(t, relay); passes != 3 {
		t.Errorf("the relay worked %d passes, want 3: one head per aggregate at a time", passes)
	}

	got := receive(t, recs, len(recs))
	if len(got) != len(recs) {
		t.Fatalf("received %d messages, want %d", len(got), len(recs))
	}
	requireOrder(t, got, recs)
	stored := make(map[string]app.OutboxRecord, len(recs))
	for _, r := range recs {
		stored[r.EventID.String()] = r
	}
	for _, msg := range got {
		rec := stored[msg.eventID]
		if msg.groupID != rec.AggregateID || msg.aggregateID != rec.AggregateID {
			t.Errorf("event %s: group %q, body aggregate %q; want both %q", msg.eventID, msg.groupID, msg.aggregateID, rec.AggregateID)
		}
		if msg.dedupID != msg.eventID {
			t.Errorf("event %s: deduplication id %q, want the event id", msg.eventID, msg.dedupID)
		}
		if msg.eventType != event.TypeWalletBalanceChanged || msg.eventVersion != "1" || msg.versionType != "Number" ||
			msg.correlationID != rec.CorrelationID {
			t.Errorf("event %s: attributes type %q, version %s %q, correlation %q; want %q, Number \"1\", %q",
				msg.eventID, msg.eventType, msg.versionType, msg.eventVersion, msg.correlationID,
				event.TypeWalletBalanceChanged, rec.CorrelationID)
		}
		requireSameJSON(t, msg.body, rec.Payload)
	}
	requireAllPublished(t, recs)
	requireResults(t, m, map[string]int{"published": len(recs)})
}

func requireSameJSON(t *testing.T, body string, payload []byte) {
	t.Helper()
	var got, want any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("body: %v", err)
	}
	if err := json.Unmarshal(payload, &want); err != nil {
		t.Fatalf("payload: %v", err)
	}
	g, _ := json.Marshal(got)
	w, _ := json.Marshal(want)
	if string(g) != string(w) {
		t.Errorf("published body %s, want the stored snapshot %s", g, w)
	}
}

type countingSQS struct {
	next sqsout.Client
	mu   sync.Mutex
	sent map[string]int
}

func (c *countingSQS) SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	c.mu.Lock()
	for _, e := range in.Entries {
		c.sent[aws.ToString(e.MessageDeduplicationId)]++
	}
	c.mu.Unlock()
	return c.next.SendMessageBatch(ctx, in, optFns...)
}

func TestRelay_TwoRelaysNoDoublePublishOnHappyPath(t *testing.T) {
	resetDB(t)
	const walletsN, perWallet = 20, 5
	wallets := newWallets(walletsN)
	var order []uuid.UUID
	for range perWallet {
		order = append(order, wallets...)
	}
	recs := appendEvents(t, order...)

	counter := &countingSQS{next: appSQS, sent: make(map[string]int)}
	pub := sqsout.NewPublisher(counter, env.AWS.Queues, tracing.Tracer{}, testLog(t))
	cfg := itConfig
	cfg.BatchSize = 5
	metrics := []*fakeMetrics{newMetrics(), newMetrics()}
	var runners []*worker.Runner
	for i, m := range metrics {
		relay := outboxrelay.New(uow, pub, "it", cfg, m, tracing.Tracer{}, testLog(t))
		runners = append(runners, worker.NewRunner(fmt.Sprintf("relay-%d", i), cfg.PollInterval, relay.RunOnce, testLog(t)))
	}
	for _, r := range runners {
		if err := r.Start(t.Context()); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	stopAll := func() {
		for _, r := range runners {
			ctx, cancel := context.WithTimeout(context.Background(), waitFor)
			if err := r.Stop(ctx); err != nil {
				t.Errorf("Stop: %v", err)
			}
			cancel()
		}
	}
	t.Cleanup(stopAll)

	waitUntilNothingPending(t)
	stopAll()

	if len(counter.sent) != len(recs) {
		t.Errorf("sent %d distinct events, want %d", len(counter.sent), len(recs))
	}
	for _, r := range recs {
		if n := counter.sent[r.EventID.String()]; n != 1 {
			t.Errorf("event %s was sent %d times, want exactly once", r.EventID, n)
		}
	}
	total := map[string]int{}
	for i, m := range metrics {
		res := m.results()
		t.Logf("relay %d: %v", i, res)
		if res["published"] == 0 {
			t.Errorf("relay %d published nothing: the relays did not share the work", i)
		}
		for k, v := range res {
			total[k] += v
		}
	}
	if want := map[string]int{"published": len(recs)}; !maps.Equal(total, want) {
		t.Errorf("results of both relays = %v, want %v", total, want)
	}

	got := receive(t, recs, len(recs))
	if len(got) != len(recs) {
		t.Errorf("received %d messages, want %d", len(got), len(recs))
	}
	requireOrder(t, got, recs)
	requireAllPublished(t, recs)
}

func waitUntilNothingPending(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for {
		var n int64
		if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&n); err != nil {
			t.Fatalf("count pending: %v", err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d events still pending after %s", n, waitFor)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRelay_RecoversAbandonedLease(t *testing.T) {
	resetDB(t)
	w := newWallets(1)[0]
	recs := appendEvents(t, w, w)
	const crashed = "crashed/deadbeef"
	if claimed := claimAs(t, crashed, 500*time.Millisecond); len(claimed) != 1 || claimed[0].EventID != recs[0].EventID {
		t.Fatalf("the crashed relay claimed %v, want the head %s", claimed, recs[0].EventID)
	}
	m := newMetrics()
	relay := newRelay(t, eventsPublisher(t), m)

	if didWork, err := relay.RunOnce(t.Context()); err != nil || didWork {
		t.Fatalf("RunOnce under a live lease = %t, %v; want false, nil", didWork, err)
	}

	runUntilClaimed(t, relay)
	if row := readRow(t, recs[0].EventID); !row.published || row.attempts != 2 {
		t.Errorf("abandoned head: published %t after %d claims; want published by the second claim", row.published, row.attempts)
	}
	drain(t, relay)

	got := receive(t, recs, len(recs))
	if len(got) != len(recs) {
		t.Fatalf("received %d messages, want %d", len(got), len(recs))
	}
	requireOrder(t, got, recs)
	requireAllPublished(t, recs)
	requireResults(t, m, map[string]int{"published": 2})
}

func TestRelay_RepublishKeepsEventID(t *testing.T) {
	resetDB(t)
	recs := appendEvents(t, newWallets(3)...)
	claimed := claimAs(t, "crashed/0badf00d", 500*time.Millisecond)
	if len(claimed) != len(recs) {
		t.Fatalf("claimed %d events, want %d", len(claimed), len(recs))
	}
	for id, err := range eventsPublisher(t).Publish(t.Context(), claimed) {
		if err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}
	m := newMetrics()
	relay := newRelay(t, eventsPublisher(t), m)

	runUntilClaimed(t, relay)

	requireAllPublished(t, recs)
	requireResults(t, m, map[string]int{"published": len(recs)})
	got := receive(t, recs, len(recs))
	t.Logf("received %d messages for %d events", len(got), len(recs))
	stored := make(map[string]app.OutboxRecord, len(recs))
	for _, r := range recs {
		stored[r.EventID.String()] = r
	}
	seen := make(map[string]bool)
	for _, msg := range got {
		rec, ok := stored[msg.eventID]
		if !ok || msg.dedupID != msg.eventID {
			t.Errorf("message of event %q with deduplication id %q, want an outbox event under its own id", msg.eventID, msg.dedupID)
			continue
		}
		requireSameJSON(t, msg.body, rec.Payload)
		seen[msg.eventID] = true
	}
	for id := range stored {
		if !seen[id] {
			t.Errorf("event %s was never received", id)
		}
	}
}

func TestRelay_FailureBacksOff(t *testing.T) {
	resetDB(t)
	w := newWallets(1)[0]
	recs := appendEvents(t, w, w)
	missing := env.AWS.Queues
	missing.Events = strings.Replace(missing.Events, "wallet-events.fifo", "no-such-queue.fifo", 1)
	if missing.Events == env.AWS.Queues.Events {
		t.Fatalf("events queue URL %q does not name wallet-events.fifo", missing.Events)
	}
	m := newMetrics()
	broken := newRelay(t, sqsout.NewPublisher(appSQS, missing, tracing.Tracer{}, testLog(t)), m)

	didWork, err := broken.RunOnce(t.Context())

	if err != nil || !didWork {
		t.Fatalf("RunOnce() = %t, %v; want true, nil", didWork, err)
	}
	head := readRow(t, recs[0].EventID)
	if head.published || head.locked || head.attempts != 1 || head.lastError == "" {
		t.Errorf("failed head: published %t, leased %t, attempts %d, last error %q; want unpublished, released, 1 attempt and the error",
			head.published, head.locked, head.attempts, head.lastError)
	}
	if !head.dueInFuture || head.dueWithin > 2*time.Second || head.dueWithin < time.Second/2 {
		t.Errorf("failed head due in %s (in the future %t), want within 1s to 2s", head.dueWithin, head.dueInFuture)
	}
	t.Logf("last error: %s", head.lastError)
	if next := readRow(t, recs[1].EventID); next.attempts != 0 {
		t.Errorf("the event behind the head was claimed %d times, want 0: it waits for the head", next.attempts)
	}
	requireResults(t, m, map[string]int{"failed": 1})
	if didWork, err := broken.RunOnce(t.Context()); err != nil || didWork {
		t.Errorf("RunOnce() while backing off = %t, %v; want false, nil", didWork, err)
	}

	healthy := newRelay(t, eventsPublisher(t), m)
	runUntilClaimed(t, healthy)
	if head := readRow(t, recs[0].EventID); !head.published || head.attempts != 2 {
		t.Errorf("head after the backoff: published %t after %d claims, want published by the second", head.published, head.attempts)
	}
	drain(t, healthy)
	got := receive(t, recs, len(recs))
	if len(got) != len(recs) {
		t.Fatalf("received %d messages, want %d", len(got), len(recs))
	}
	requireOrder(t, got, recs)
	requireAllPublished(t, recs)
}

func TestRelay_ReleaseHandsOverAtOnce(t *testing.T) {
	resetDB(t)
	recs := appendEvents(t, newWallets(2)...)
	stopping := newRelay(t, eventsPublisher(t), newMetrics())
	if claimed := claimAs(t, stopping.Owner(), time.Hour); len(claimed) != len(recs) {
		t.Fatalf("claimed %d events, want %d", len(claimed), len(recs))
	}

	if err := stopping.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	m := newMetrics()
	if passes := drain(t, newRelay(t, eventsPublisher(t), m)); passes != 1 {
		t.Errorf("the next relay worked %d passes, want 1 right after the release", passes)
	}
	requireAllPublished(t, recs)
	requireResults(t, m, map[string]int{"published": len(recs)})
	receive(t, recs, len(recs))
}

func TestRelay_Stats(t *testing.T) {
	resetDB(t)
	m := newMetrics()
	relay := newRelay(t, eventsPublisher(t), m)

	if _, err := relay.StatsOnce(t.Context()); err != nil {
		t.Fatalf("StatsOnce: %v", err)
	}
	if got := m.lastPending(t); got != (pending{}) {
		t.Errorf("empty outbox: Pending(%d, %s), want (0, 0)", got.n, got.oldest)
	}

	recs := appendEvents(t, newWallets(3)...)
	time.Sleep(20 * time.Millisecond)
	didWork, err := relay.StatsOnce(t.Context())
	if err != nil || didWork {
		t.Fatalf("StatsOnce() = %t, %v; want false, nil", didWork, err)
	}
	if got := m.lastPending(t); got.n != 3 || got.oldest < 20*time.Millisecond || got.oldest > waitFor {
		t.Errorf("Pending(%d, %s), want 3 events, the oldest waiting at least 20ms", got.n, got.oldest)
	}

	drain(t, relay)
	if _, err := relay.StatsOnce(t.Context()); err != nil {
		t.Fatalf("StatsOnce: %v", err)
	}
	if got := m.lastPending(t); got != (pending{}) {
		t.Errorf("after publishing: Pending(%d, %s), want (0, 0)", got.n, got.oldest)
	}
	receive(t, recs, len(recs))
}

func TestRelay_PublishedEventContinuesTheTraceOfTheChange(t *testing.T) {
	resetDB(t)
	tr, rec := tracingtest.New(t)
	traced := postgres.NewUnitOfWork(pool, tr)
	ctx, request := tr.Start(t.Context(), "request")
	recs := appendEventsIn(ctx, t, traced, newWallets(1)...)
	request.End()
	untraced := appendEvents(t, newWallets(1)...)
	relay := outboxrelay.New(traced, sqsout.NewPublisher(appSQS, env.AWS.Queues, tr, testLog(t)), "it", itConfig,
		newMetrics(), tr, testLog(t))

	drain(t, relay)

	msgs := receive(t, append(recs, untraced...), 2)
	byEvent := make(map[string]message, len(msgs))
	for _, m := range msgs {
		byEvent[m.eventID] = m
	}
	units := slices.DeleteFunc(tracingtest.Named(rec, "uow.do"), func(s sdktrace.ReadOnlySpan) bool {
		return !tracingtest.ChildOf(s, tracingtest.One(t, rec, "request"))
	})
	if len(units) != 1 {
		t.Fatalf("%d units of work under the request, want the one that appended the event", len(units))
	}
	unit := units[0]
	sends := make(map[string]sdktrace.ReadOnlySpan)
	for _, s := range tracingtest.Named(rec, "send wallet-events.fifo") {
		sends[tracingtest.Attributes(s)["outbox.event_id"]] = s
	}
	send, ok := sends[recs[0].EventID.String()]
	if !ok {
		t.Fatalf("no send span for the traced event; ended: %v", tracingtest.Names(rec))
	}
	if !tracingtest.ChildOf(send, unit) {
		t.Errorf("the send is under %s in trace %s, want the unit of work %s that wrote the event",
			send.Parent().SpanID(), send.SpanContext().TraceID(), unit.SpanContext().SpanID())
	}
	sc := send.SpanContext()
	if got, want := byEvent[recs[0].EventID.String()].traceParent,
		"00-"+sc.TraceID().String()+"-"+sc.SpanID().String()+"-01"; got != want {
		t.Errorf("published %s = %q, want the send span %q", sqsout.TraceParentAttribute, got, want)
	}
	pass := tracingtest.Named(rec, "outbox.relay")
	if len(pass) == 0 || !slices.ContainsFunc(pass[0].Links(), func(l sdktrace.Link) bool {
		return l.SpanContext.SpanID() == unit.SpanContext().SpanID()
	}) {
		t.Errorf("no relay pass links to the unit of work that wrote the event")
	}
	if fresh := sends[untraced[0].EventID.String()]; fresh == nil || fresh.SpanContext().TraceID() == sc.TraceID() {
		t.Errorf("the untraced event was not sent in a trace of its own")
	}
}
