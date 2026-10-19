package outboxrelay_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsout"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
	"github.com/Brunohvc/backend-challenge-go/internal/worker"
	"github.com/Brunohvc/backend-challenge-go/internal/worker/outboxrelay"
)

var (
	_ outboxrelay.Publisher = (*sqsout.Publisher)(nil)
	_ outboxrelay.Metrics   = (*metrics.Metrics)(nil)
	_ worker.Task           = (*outboxrelay.Relay)(nil).RunOnce
	_ worker.Task           = (*outboxrelay.Relay)(nil).StatsOnce
)

var unitConfig = config.OutboxConfig{
	Enabled:      true,
	PollInterval: time.Second,
	BatchSize:    7,
	Lease:        30 * time.Second,
	BackoffMax:   5 * time.Minute,
}

const (
	unitInstance = "wallet-1"
	unitSuffix   = "0a1b2c3d"
	unitOwner    = unitInstance + "/" + unitSuffix
)

type fakeOutbox struct {
	mu sync.Mutex

	claim      func(owner string, lease time.Duration, limit int) ([]app.OutboxRecord, error)
	lost       map[uuid.UUID]bool
	publishErr error
	stats      app.OutboxStats

	published []ack
	failed    []failure
	released  []string
}

type ack struct {
	id      uuid.UUID
	owner   string
	ctxDead bool
}

type failure struct {
	id      uuid.UUID
	owner   string
	retryIn time.Duration
	lastErr string
}

func (f *fakeOutbox) Append(context.Context, ...app.OutboxRecord) error { return nil }

func (f *fakeOutbox) ClaimHeads(_ context.Context, owner string, lease time.Duration, limit int) ([]app.OutboxRecord, error) {
	if f.claim == nil {
		return nil, nil
	}
	return f.claim(owner, lease, limit)
}

func (f *fakeOutbox) MarkPublished(ctx context.Context, id uuid.UUID, owner string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.publishErr != nil {
		return false, f.publishErr
	}
	f.published = append(f.published, ack{id: id, owner: owner, ctxDead: ctx.Err() != nil})
	return !f.lost[id], nil
}

func (f *fakeOutbox) MarkFailed(_ context.Context, id uuid.UUID, owner string, retryIn time.Duration, lastErr string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, failure{id: id, owner: owner, retryIn: retryIn, lastErr: lastErr})
	return !f.lost[id], nil
}

func (f *fakeOutbox) Release(_ context.Context, owner string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, owner)
	return nil
}

func (f *fakeOutbox) Stats(context.Context) (app.OutboxStats, error) { return f.stats, nil }

type fakeRepos struct {
	app.Repos
	outbox *fakeOutbox
}

func (r fakeRepos) Outbox() app.OutboxRepository { return r.outbox }

type fakeUnitOfWork struct {
	outbox    *fakeOutbox
	units     int
	snapshots int
	spans     []trace.SpanContext
}

func (u *fakeUnitOfWork) Do(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	u.units++
	u.spans = append(u.spans, trace.SpanContextFromContext(ctx))
	return fn(ctx, fakeRepos{outbox: u.outbox})
}

func (u *fakeUnitOfWork) Snapshot(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	u.snapshots++
	return fn(ctx, fakeRepos{outbox: u.outbox})
}

type fakePublisher struct {
	calls   [][]app.OutboxRecord
	publish func(ctx context.Context, recs []app.OutboxRecord) map[uuid.UUID]error
}

func (p *fakePublisher) Publish(ctx context.Context, recs []app.OutboxRecord) map[uuid.UUID]error {
	p.calls = append(p.calls, recs)
	if p.publish != nil {
		return p.publish(ctx, recs)
	}
	out := make(map[uuid.UUID]error, len(recs))
	for _, r := range recs {
		out[r.EventID] = nil
	}
	return out
}

type fakeMetrics struct {
	mu        sync.Mutex
	published map[string]int
	pending   []pending
}

type pending struct {
	n      int64
	oldest time.Duration
}

func newMetrics() *fakeMetrics { return &fakeMetrics{published: make(map[string]int)} }

func (m *fakeMetrics) Published(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published[result]++
}

func (m *fakeMetrics) Pending(n int64, oldest time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending = append(m.pending, pending{n, oldest})
}

func (m *fakeMetrics) results() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.published)
}

func (m *fakeMetrics) lastPending(t *testing.T) pending {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pending) == 0 {
		t.Fatal("Pending was never reported")
	}
	return m.pending[len(m.pending)-1]
}

func requireResults(t *testing.T, m *fakeMetrics, want map[string]int) {
	t.Helper()
	if got := m.results(); !maps.Equal(got, want) {
		t.Errorf("Published results = %v, want %v", got, want)
	}
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type logLines struct{ buf bytes.Buffer }

func (l *logLines) logger() *slog.Logger {
	return logging.New(&l.buf, slog.LevelDebug, "test")
}

func (l *logLines) withMessage(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var lines []map[string]any
	dec := json.NewDecoder(bytes.NewReader(l.buf.Bytes()))
	dec.UseNumber()
	for dec.More() {
		var line map[string]any
		if err := dec.Decode(&line); err != nil {
			t.Fatalf("log line: %v", err)
		}
		if line[slog.MessageKey] == msg {
			lines = append(lines, line)
		}
	}
	return lines
}

func unitRecords(n, attempts int) []app.OutboxRecord {
	recs := make([]app.OutboxRecord, n)
	for i := range recs {
		recs[i] = app.OutboxRecord{
			EventID:       idgen.V7{}.New(),
			AggregateType: "Wallet",
			AggregateID:   idgen.V7{}.New().String(),
			EventType:     "WalletBalanceChanged",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			CorrelationID: "corr",
			Attempts:      attempts,
		}
	}
	return recs
}

func failEvery(err error) func(context.Context, []app.OutboxRecord) map[uuid.UUID]error {
	return func(_ context.Context, recs []app.OutboxRecord) map[uuid.UUID]error {
		out := make(map[uuid.UUID]error, len(recs))
		for _, r := range recs {
			out[r.EventID] = err
		}
		return out
	}
}

func claimOnce(recs []app.OutboxRecord) func(string, time.Duration, int) ([]app.OutboxRecord, error) {
	done := false
	return func(string, time.Duration, int) ([]app.OutboxRecord, error) {
		if done {
			return nil, nil
		}
		done = true
		return recs, nil
	}
}

type unitRig struct {
	outbox  *fakeOutbox
	uow     *fakeUnitOfWork
	pub     *fakePublisher
	metrics *fakeMetrics
	logs    *logLines
	relay   *outboxrelay.Relay
}

func newUnitRig(cfg config.OutboxConfig) *unitRig {
	return newTracedRig(cfg, tracing.Tracer{})
}

func newTracedRig(cfg config.OutboxConfig, tr tracing.Tracer) *unitRig {
	outbox := &fakeOutbox{lost: make(map[uuid.UUID]bool)}
	r := &unitRig{outbox: outbox, uow: &fakeUnitOfWork{outbox: outbox}, pub: &fakePublisher{}, metrics: newMetrics(), logs: &logLines{}}
	r.relay = outboxrelay.NewWithSuffix(r.uow, r.pub, unitInstance, cfg, r.metrics, tr, r.logs.logger(),
		func() string { return unitSuffix })
	return r
}

func TestNew_OwnerIsInstanceAndRandomSuffix(t *testing.T) {
	if got := newUnitRig(unitConfig).relay.Owner(); got != unitOwner {
		t.Errorf("Owner() = %q, want %q", got, unitOwner)
	}

	a := outboxrelay.New(&fakeUnitOfWork{}, &fakePublisher{}, unitInstance, unitConfig, newMetrics(), tracing.Tracer{}, discardLog())
	b := outboxrelay.New(&fakeUnitOfWork{}, &fakePublisher{}, unitInstance, unitConfig, newMetrics(), tracing.Tracer{}, discardLog())
	format := regexp.MustCompile(`^` + regexp.QuoteMeta(unitInstance) + `/[0-9a-f]{8}$`)
	for _, owner := range []string{a.Owner(), b.Owner()} {
		if !format.MatchString(owner) {
			t.Errorf("Owner() = %q, want %s/<8 hex digits>", owner, unitInstance)
		}
	}
	if a.Owner() == b.Owner() {
		t.Errorf("two relays of one instance share the owner %q", a.Owner())
	}
}

func TestRunOnce_ClaimsAsConfigured(t *testing.T) {
	rig := newUnitRig(unitConfig)
	var owner string
	var lease time.Duration
	var limit int
	rig.outbox.claim = func(o string, l time.Duration, n int) ([]app.OutboxRecord, error) {
		owner, lease, limit = o, l, n
		return nil, nil
	}

	didWork, err := rig.relay.RunOnce(t.Context())

	if err != nil || didWork {
		t.Fatalf("RunOnce() = %t, %v; want false, nil with nothing due", didWork, err)
	}
	if owner != unitOwner || lease != unitConfig.Lease || limit != unitConfig.BatchSize {
		t.Errorf("ClaimHeads(%q, %s, %d), want (%q, %s, %d)", owner, lease, limit, unitOwner, unitConfig.Lease, unitConfig.BatchSize)
	}
	if len(rig.pub.calls) != 0 || rig.uow.units != 1 {
		t.Errorf("with nothing claimed: %d publishes and %d units of work, want 0 and only the claim's", len(rig.pub.calls), rig.uow.units)
	}
}

func TestRunOnce_SettlesEveryOutcome(t *testing.T) {
	rig := newUnitRig(unitConfig)
	recs := unitRecords(4, 1)
	published, lost, failed, failedLost := recs[0], recs[1], recs[2], recs[3]
	rig.outbox.claim = claimOnce(recs)
	rig.outbox.lost[lost.EventID] = true
	rig.outbox.lost[failedLost.EventID] = true
	boom := errors.New("sqs: throttled")
	rig.pub.publish = func(context.Context, []app.OutboxRecord) map[uuid.UUID]error {
		return map[uuid.UUID]error{published.EventID: nil, lost.EventID: nil, failed.EventID: boom, failedLost.EventID: boom}
	}

	didWork, err := rig.relay.RunOnce(t.Context())

	if err != nil || !didWork {
		t.Fatalf("RunOnce() = %t, %v; want true, nil", didWork, err)
	}
	if len(rig.pub.calls) != 1 || len(rig.pub.calls[0]) != len(recs) {
		t.Fatalf("Publish calls = %v, want one with the %d claimed events", rig.pub.calls, len(recs))
	}
	for i, r := range rig.pub.calls[0] {
		if r.EventID != recs[i].EventID {
			t.Errorf("published event %d = %s, want %s: the claim order must be kept", i, r.EventID, recs[i].EventID)
		}
	}
	if got := rig.outbox.published; len(got) != 2 || got[0].id != published.EventID || got[1].id != lost.EventID ||
		got[0].owner != unitOwner || got[1].owner != unitOwner {
		t.Errorf("MarkPublished calls = %+v, want %s then %s as %s", got, published.EventID, lost.EventID, unitOwner)
	}
	got := rig.outbox.failed
	if len(got) != 2 {
		t.Fatalf("MarkFailed calls = %+v, want one for %s and one for %s", got, failed.EventID, failedLost.EventID)
	}
	for i, id := range []uuid.UUID{failed.EventID, failedLost.EventID} {
		if got[i].id != id || got[i].owner != unitOwner || got[i].lastErr != boom.Error() ||
			got[i].retryIn < time.Second || got[i].retryIn > 2*time.Second {
			t.Errorf("MarkFailed call %d = %+v, want %s as %s with %q, retrying in 1s to 2s", i, got[i], id, unitOwner, boom)
		}
	}
	if rig.uow.units != 2 {
		t.Errorf("units of work = %d, want 2", rig.uow.units)
	}
	requireResults(t, rig.metrics, map[string]int{"published": 1, "lease_lost": 2, "failed": 1})
}

func TestRunOnce_BackoffDoublesWithAttempts(t *testing.T) {
	cfg := unitConfig
	cfg.BackoffMax = time.Minute
	tests := []struct {
		attempts int
		bound    time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{5, 32 * time.Second},
		{6, time.Minute},
		{1_000, time.Minute},
	}
	for _, tt := range tests {
		rig := newUnitRig(cfg)
		recs := unitRecords(20, tt.attempts)
		rig.outbox.claim = claimOnce(recs)
		rig.pub.publish = failEvery(errors.New("down"))

		if _, err := rig.relay.RunOnce(t.Context()); err != nil {
			t.Fatalf("RunOnce() error = %v", err)
		}
		if got := len(rig.outbox.failed); got != len(recs) {
			t.Fatalf("after claim %d: %d MarkFailed calls, want %d", tt.attempts, got, len(recs))
		}
		waits := make(map[time.Duration]bool)
		for _, f := range rig.outbox.failed {
			if f.retryIn < tt.bound/2 || f.retryIn > tt.bound {
				t.Errorf("after claim %d: retry in %s, want within [%s, %s]", tt.attempts, f.retryIn, tt.bound/2, tt.bound)
			}
			waits[f.retryIn] = true
		}
		if len(waits) < 2 {
			t.Errorf("after claim %d: the %d failed events all wait %v, want their waits spread", tt.attempts, len(recs), waits)
		}
	}
}

func TestRunOnce_LogsTheWaitItRecorded(t *testing.T) {
	rig := newUnitRig(unitConfig)
	recs := unitRecords(5, 4)
	rig.outbox.claim = claimOnce(recs)
	rig.pub.publish = failEvery(errors.New("down"))

	if _, err := rig.relay.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	recorded := make(map[string]time.Duration, len(rig.outbox.failed))
	for _, f := range rig.outbox.failed {
		recorded[f.id.String()] = f.retryIn
	}
	lines := rig.logs.withMessage(t, "outbox event not published")
	if len(lines) != len(recs) {
		t.Fatalf("logged %d failures, want %d", len(lines), len(recs))
	}
	for _, line := range lines {
		id, _ := line["eventId"].(string)
		if want, ok := recorded[id]; !ok || line["retryIn"] != want.String() {
			t.Errorf("logged retryIn %v for %s, want the %s MarkFailed recorded", line["retryIn"], id, want)
		}
	}
}

func TestRunOnce_LogsEachFailedEventAtDebug(t *testing.T) {
	rig := newUnitRig(unitConfig)
	recs := unitRecords(7, 1)
	rig.outbox.claim = claimOnce(recs)
	rig.pub.publish = failEvery(errors.New("connection refused"))

	if _, err := rig.relay.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	lines := rig.logs.withMessage(t, "outbox event not published")
	if len(lines) != len(recs) {
		t.Fatalf("logged %d failed events, want %d", len(lines), len(recs))
	}
	for _, line := range lines {
		if line[slog.LevelKey] != slog.LevelDebug.String() {
			t.Errorf("failed event %v logged at %v, want %s", line["eventId"], line[slog.LevelKey], slog.LevelDebug)
		}
	}
}

func TestRunOnce_MissingOutcomeIsAFailure(t *testing.T) {
	rig := newUnitRig(unitConfig)
	recs := unitRecords(2, 1)
	rig.outbox.claim = claimOnce(recs)
	rig.pub.publish = func(context.Context, []app.OutboxRecord) map[uuid.UUID]error {
		return map[uuid.UUID]error{recs[0].EventID: nil}
	}

	if _, err := rig.relay.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	if got := rig.outbox.failed; len(got) != 1 || got[0].id != recs[1].EventID || got[0].lastErr == "" {
		t.Errorf("MarkFailed calls = %+v, want one for %s with a reason", got, recs[1].EventID)
	}
	requireResults(t, rig.metrics, map[string]int{"published": 1, "failed": 1})
}

func TestRunOnce_PublishDeadlineIsHalfTheLease(t *testing.T) {
	rig := newUnitRig(unitConfig)
	rig.outbox.claim = claimOnce(unitRecords(1, 1))
	var remaining time.Duration
	var hasDeadline bool
	rig.pub.publish = func(ctx context.Context, recs []app.OutboxRecord) map[uuid.UUID]error {
		var deadline time.Time
		deadline, hasDeadline = ctx.Deadline()
		remaining = time.Until(deadline)
		return map[uuid.UUID]error{recs[0].EventID: nil}
	}

	if _, err := rig.relay.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	if !hasDeadline || remaining <= 0 || remaining > unitConfig.Lease/2 {
		t.Errorf("Publish deadline in %s (set %t), want within half the lease (%s)", remaining, hasDeadline, unitConfig.Lease/2)
	}
}

func TestRunOnce_SettlesWhatItPublishedWhenStopping(t *testing.T) {
	rig := newUnitRig(unitConfig)
	recs := unitRecords(1, 1)
	rig.outbox.claim = claimOnce(recs)
	ctx, stop := context.WithCancel(t.Context())
	var publishCtxDead bool
	rig.pub.publish = func(pctx context.Context, recs []app.OutboxRecord) map[uuid.UUID]error {
		stop()
		publishCtxDead = pctx.Err() != nil
		return map[uuid.UUID]error{recs[0].EventID: nil}
	}

	didWork, err := rig.relay.RunOnce(ctx)

	if err != nil || !didWork {
		t.Fatalf("RunOnce() = %t, %v; want true, nil", didWork, err)
	}
	if publishCtxDead {
		t.Error("stopping canceled the publish of a batch already claimed")
	}
	if got := rig.outbox.published; len(got) != 1 || got[0].ctxDead {
		t.Errorf("MarkPublished calls = %+v, want one on a live context", got)
	}
}

func TestRunOnce_ClaimFailureIsReturned(t *testing.T) {
	rig := newUnitRig(unitConfig)
	boom := errors.New("connection refused")
	rig.outbox.claim = func(string, time.Duration, int) ([]app.OutboxRecord, error) { return nil, boom }

	didWork, err := rig.relay.RunOnce(t.Context())

	if !errors.Is(err, boom) || didWork {
		t.Errorf("RunOnce() = %t, %v; want false and the claim error", didWork, err)
	}
	if len(rig.pub.calls) != 0 {
		t.Errorf("published %d batches after a failed claim", len(rig.pub.calls))
	}
}

func TestRunOnce_SettleFailureIsReturnedUncounted(t *testing.T) {
	rig := newUnitRig(unitConfig)
	rig.outbox.claim = claimOnce(unitRecords(2, 1))
	boom := errors.New("connection reset")
	rig.outbox.publishErr = boom

	_, err := rig.relay.RunOnce(t.Context())

	if !errors.Is(err, boom) {
		t.Errorf("RunOnce() error = %v, want the settle error", err)
	}
	requireResults(t, rig.metrics, map[string]int{})
}

func TestStatsOnce_ReportsPending(t *testing.T) {
	rig := newUnitRig(unitConfig)
	rig.outbox.stats = app.OutboxStats{Pending: 42, OldestAge: 3 * time.Second}

	didWork, err := rig.relay.StatsOnce(t.Context())

	if err != nil || didWork {
		t.Fatalf("StatsOnce() = %t, %v; want false, nil", didWork, err)
	}
	if got := rig.metrics.lastPending(t); got != (pending{42, 3 * time.Second}) {
		t.Errorf("Pending(%d, %s), want (42, 3s)", got.n, got.oldest)
	}
	if rig.uow.snapshots != 1 || rig.uow.units != 0 {
		t.Errorf("stats ran in %d snapshots and %d units of work, want one read-only snapshot", rig.uow.snapshots, rig.uow.units)
	}
}

func TestRelease_ReleasesTheLeasesOfItsOwner(t *testing.T) {
	rig := newUnitRig(unitConfig)

	if err := rig.relay.Release(t.Context()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	if got := rig.outbox.released; len(got) != 1 || got[0] != unitOwner {
		t.Errorf("Release calls = %v, want [%s]", got, unitOwner)
	}
}

func TestRunOnce_TracesThePassThatFoundWork(t *testing.T) {
	tr, rec := tracingtest.New(t)
	rig := newTracedRig(unitConfig, tr)
	ctx, request := tr.Start(t.Context(), "request")
	stored := tracing.TraceParent(ctx)
	request.End()
	recs := unitRecords(2, 1)
	recs[0].TraceParent = stored
	rig.outbox.claim = claimOnce(recs)
	var published trace.SpanContext
	rig.pub.publish = func(ctx context.Context, recs []app.OutboxRecord) map[uuid.UUID]error {
		published = trace.SpanContextFromContext(ctx)
		return failEvery(nil)(ctx, recs)
	}

	if _, err := rig.relay.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	pass := tracingtest.One(t, rec, "outbox.relay")
	if pass.Parent().IsValid() {
		t.Errorf("the pass is under %s, want it to start a trace", pass.Parent().SpanID())
	}
	if links := pass.Links(); len(links) != 1 || !links[0].SpanContext.Equal(request.SpanContext().WithRemote(true)) {
		t.Errorf("links of the pass = %v, want one to the change that wrote the traced event", links)
	}
	if attrs := tracingtest.Attributes(pass); attrs["outbox.claimed"] != "2" {
		t.Errorf("outbox.claimed = %q, want 2", attrs["outbox.claimed"])
	}
	if !published.Equal(pass.SpanContext()) {
		t.Errorf("events published under %s, want the pass %s", published.SpanID(), pass.SpanContext().SpanID())
	}
	if len(rig.uow.spans) != 2 || rig.uow.spans[0].IsValid() || !rig.uow.spans[1].Equal(pass.SpanContext()) {
		t.Errorf("units of work ran under %v, want the claim untraced and the settlement under the pass", rig.uow.spans)
	}
	if got := rig.pub.calls[0][0].TraceParent; got != stored {
		t.Errorf("published traceparent = %q, want the stored %q for the publisher to continue", got, stored)
	}
}

func TestRunOnce_TracesNothingWithoutWork(t *testing.T) {
	tr, rec := tracingtest.New(t)
	rig := newTracedRig(unitConfig, tr)

	if _, err := rig.relay.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	if names := tracingtest.Names(rec); len(names) != 0 {
		t.Errorf("ended spans = %v, want none for a pass that claimed nothing", names)
	}
}

func TestRunOnce_PassRecordsAFailedSettlement(t *testing.T) {
	tr, rec := tracingtest.New(t)
	rig := newTracedRig(unitConfig, tr)
	rig.outbox.claim = claimOnce(unitRecords(1, 1))
	rig.outbox.publishErr = fmt.Errorf("postgres: mark published: %w", app.ErrUnavailable)

	if _, err := rig.relay.RunOnce(t.Context()); err == nil {
		t.Fatal("RunOnce() error = nil, want the settlement failure")
	}

	pass := tracingtest.One(t, rec, "outbox.relay")
	if attrs := tracingtest.Attributes(pass); pass.Status().Code != codes.Error || attrs["error.type"] != "unavailable" {
		t.Errorf("status %+v, error.type %q; want an unavailable error", pass.Status(), attrs["error.type"])
	}
}
