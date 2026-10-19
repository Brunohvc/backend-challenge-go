//go:build integration

package wagering_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock/clocktest"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var pg *testenv.Postgres

func TestMain(m *testing.M) {
	os.Exit(testenv.RunWithPostgres(m, func(p *testenv.Postgres) { pg = p }))
}

const (
	providerA = "provider-a"
	providerB = "provider-b"
)

var t0 = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

var referencePolicy = wager.ReferencePolicy{
	MaxAttempts: 10,
	TTL:         15 * time.Minute,
	BackoffBase: time.Second,
	BackoffMax:  time.Minute,
}

var fromHTTP = wagering.Source{Channel: wagering.ChannelHTTP, CorrelationID: "corr-request-1"}

func newID() uuid.UUID { return idgen.V7{}.New() }

func dbConfig() config.DBConfig {
	return config.DBConfig{
		URL:              pg.AppDSN,
		MaxConns:         8,
		LockTimeout:      2 * time.Second,
		StatementTimeout: 10 * time.Second,
		TxMaxAttempts:    3,
	}
}

type harness struct {
	uow     *postgres.UnitOfWork
	svc     *wagering.Service
	clock   *clocktest.Fixed
	metrics *recordingMetrics
	logs    *logBuffer
}

type harnessOption func(*harnessConfig)

type harnessConfig struct {
	db        config.DBConfig
	wrap      func(app.UnitOfWork) app.UnitOfWork
	reference wager.ReferencePolicy
	tracer    tracing.Tracer
}

func withDB(cfg config.DBConfig) harnessOption {
	return func(c *harnessConfig) { c.db = cfg }
}

func withUnitOfWork(wrap func(app.UnitOfWork) app.UnitOfWork) harnessOption {
	return func(c *harnessConfig) { c.wrap = wrap }
}

func withReferencePolicy(p wager.ReferencePolicy) harnessOption {
	return func(c *harnessConfig) { c.reference = p }
}

func withTracer(tr tracing.Tracer) harnessOption {
	return func(c *harnessConfig) { c.tracer = tr }
}

func newHarness(t *testing.T, opts ...harnessOption) *harness {
	t.Helper()
	if err := pg.Reset(t.Context()); err != nil {
		t.Fatalf("reset: %v", err)
	}
	c := harnessConfig{db: dbConfig(), reference: referencePolicy}
	for _, opt := range opts {
		opt(&c)
	}
	pool, err := postgres.NewPool(t.Context(), c.db, "test", postgres.WithTracing(c.tracer))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(pool.Close)
	requireDueAtOnce(t, pool, t0.Add(c.reference.TTL))

	uow := postgres.NewUnitOfWork(pool, c.tracer)
	var svcUOW app.UnitOfWork = uow
	if c.wrap != nil {
		svcUOW = c.wrap(uow)
	}
	h := &harness{uow: uow, clock: &clocktest.Fixed{T: t0}, metrics: &recordingMetrics{}, logs: &logBuffer{}}
	h.svc = wagering.NewService(svcUOW, h.clock, idgen.V7{}, h.metrics, c.tracer, logging.New(h.logs, slog.LevelDebug, "test"), wagering.Config{
		Reference:             c.reference,
		Tx:                    retry.Policy{Attempts: c.db.TxMaxAttempts, Base: 5 * time.Millisecond, Max: 50 * time.Millisecond},
		PermanentFailureLimit: permanentFailureLimit,
	})
	return h
}

func requireDueAtOnce(t *testing.T, pool *pgxpool.Pool, latest time.Time) {
	t.Helper()
	var now time.Time
	if err := pool.QueryRow(t.Context(), "SELECT now()").Scan(&now); err != nil {
		t.Fatalf("read the clock of the database: %v", err)
	}
	if !now.After(latest) {
		t.Fatalf("the database clock reads %s, not after %s, the fixed clock plus the reference TTL: "+
			"what the tests schedule would not be due yet, so t0 must be moved back", now.UTC(), latest.UTC())
	}
}

const permanentFailureLimit = 3

func connect(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), pg.AppDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func mustMoney(t *testing.T, amount string, c money.Currency) money.Money {
	t.Helper()
	m, err := money.Parse(amount, c.Code())
	if err != nil {
		t.Fatalf("money.Parse(%q) error = %v", amount, err)
	}
	return m
}

func openWallet(t *testing.T, uow app.UnitOfWork, initial string) *wallet.Wallet {
	t.Helper()
	openingID := newID()
	w, entry, err := wallet.Open(wallet.OpenParams{
		ID:             newID(),
		PlayerID:       newID(),
		Initial:        mustMoney(t, initial, money.BRL()),
		OpeningTxID:    openingID,
		OpeningEntryID: newID(),
		Now:            t0,
	})
	if err != nil {
		t.Fatalf("wallet.Open() error = %v", err)
	}
	err = uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		if err := r.Wallets().Insert(ctx, w); err != nil || entry == nil {
			return err
		}
		opening, err := wager.NewOpening(openingID, w.ID(), w.PlayerID(), entry.Amount(), "corr-opening", t0)
		if err != nil {
			return err
		}
		if err := opening.MarkProcessed(wager.Result{BalanceAfter: w.Balance(), WalletVersion: w.Version()}, t0); err != nil {
			return err
		}
		if err := r.Transactions().Insert(ctx, opening); err != nil {
			return err
		}
		return r.Ledger().Append(ctx, *entry)
	})
	if err != nil {
		t.Fatalf("open a wallet with %s: %v", initial, err)
	}
	return w
}

func input(w *wallet.Wallet, kind, amount string) wager.CommandInput {
	ext := "ext-" + newID().String()
	return wager.CommandInput{
		ProviderID:            providerA,
		ExternalTransactionID: ext,
		IdempotencyKey:        "key-" + ext,
		PlayerID:              w.PlayerID().String(),
		WalletID:              w.ID().String(),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  kind,
		Amount:                amount,
		Currency:              w.Currency().Code(),
	}
}

func referring(w *wallet.Wallet, kind, amount string, ref wager.Snapshot) wager.CommandInput {
	in := input(w, kind, amount)
	in.ReferenceExternalTransactionID = ref.ExternalTransactionID
	return in
}

func waitingFor(w *wallet.Wallet, kind, amount, ref string) wager.CommandInput {
	in := input(w, kind, amount)
	in.ReferenceExternalTransactionID = ref
	return in
}

func mustCommand(t *testing.T, in wager.CommandInput) wager.Command {
	t.Helper()
	cmd, err := wager.NewCommand(in)
	if err != nil {
		t.Fatalf("NewCommand(%+v) error = %v", in, err)
	}
	return cmd
}

func submit(t *testing.T, svc *wagering.Service, in wager.CommandInput) wagering.Result {
	t.Helper()
	res, err := svc.Submit(t.Context(), mustCommand(t, in), fromHTTP)
	if err != nil {
		t.Fatalf("Submit(%s %s) error = %v", in.Kind, in.Amount, err)
	}
	return res
}

func requireOutcome(t *testing.T, tx wager.Snapshot, status wager.Status, code wager.FailureCode) {
	t.Helper()
	if tx.Status != status || tx.FailureCode != code {
		t.Fatalf("transaction %s ended %s %q, want %s %q", tx.ID, tx.Status, tx.FailureCode, status, code)
	}
}

func requireResultBalance(t *testing.T, tx wager.Snapshot, want string) {
	t.Helper()
	if tx.ResultBalance == nil {
		t.Fatalf("transaction %s recorded no balance, want %s", tx.ID, want)
	}
	if got := tx.ResultBalance.String(); got != want {
		t.Fatalf("transaction %s recorded the balance %s, want %s", tx.ID, got, want)
	}
}

func requireWallet(t *testing.T, uow app.UnitOfWork, id uuid.UUID, balance string, version int64) {
	t.Helper()
	var w *wallet.Wallet
	err := uow.Snapshot(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		w, err = r.Wallets().Get(ctx, id)
		return err
	})
	if err != nil {
		t.Fatalf("get wallet %s: %v", id, err)
	}
	if w.Balance().String() != balance || w.Version() != version {
		t.Fatalf("wallet holds %s at version %d, want %s at version %d", w.Balance(), w.Version(), balance, version)
	}
}

func ledger(t *testing.T, uow app.UnitOfWork, id uuid.UUID) []wallet.LedgerEntry {
	t.Helper()
	var records []app.LedgerRecord
	err := uow.Snapshot(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		records, err = r.Ledger().Page(ctx, id, 0, 1000)
		return err
	})
	if err != nil {
		t.Fatalf("ledger of wallet %s: %v", id, err)
	}
	entries := make([]wallet.LedgerEntry, len(records))
	for i, rec := range records {
		entries[i] = rec.Entry
	}
	return entries
}

func requireLedger(t *testing.T, uow app.UnitOfWork, id uuid.UUID, dirs ...wallet.Direction) []wallet.LedgerEntry {
	t.Helper()
	entries := ledger(t, uow, id)
	got := make([]wallet.Direction, len(entries))
	for i, e := range entries {
		got[i] = e.Direction()
	}
	want := append([]wallet.Direction{wallet.DirectionCredit}, dirs...)
	if !slices.Equal(got, want) {
		t.Fatalf("ledger of wallet %s = %v, want the opening and %v", id, got, dirs)
	}
	return entries
}

type envelope struct {
	EventID       string         `json:"eventId"`
	EventType     string         `json:"eventType"`
	AggregateID   string         `json:"aggregateId"`
	CorrelationID string         `json:"correlationId"`
	CausationID   string         `json:"causationId"`
	OccurredAt    string         `json:"occurredAt"`
	Version       json.Number    `json:"version"`
	Data          map[string]any `json:"data"`
}

type outboxEvent struct {
	ID            uuid.UUID
	AggregateType string
	AggregateID   string
	Type          string
	CorrelationID string
	CausationID   string
	Envelope      envelope
}

func events(t *testing.T, db *pgx.Conn, id uuid.UUID) []outboxEvent {
	t.Helper()
	rows, err := db.Query(t.Context(),
		`SELECT id, aggregate_type, aggregate_id, event_type, correlation_id, COALESCE(causation_id, ''), payload
		   FROM outbox_events
		  WHERE aggregate_id = $1
		  ORDER BY seq`, id.String())
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	got, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outboxEvent, error) {
		var (
			e       outboxEvent
			payload []byte
		)
		if err := row.Scan(&e.ID, &e.AggregateType, &e.AggregateID, &e.Type, &e.CorrelationID, &e.CausationID, &payload); err != nil {
			return e, err
		}
		dec := json.NewDecoder(bytes.NewReader(payload))
		dec.UseNumber()
		return e, dec.Decode(&e.Envelope)
	})
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	return got
}

func requireEvents(t *testing.T, db *pgx.Conn, tx wager.Snapshot, causation string, types ...string) []outboxEvent {
	t.Helper()
	got := events(t, db, tx.WalletID)
	gotTypes := make([]string, len(got))
	for i, e := range got {
		gotTypes[i] = e.Type
	}
	if !slices.Equal(gotTypes, types) {
		t.Fatalf("events = %v, want %v", gotTypes, types)
	}
	for _, e := range got {
		requireAbout(t, e, tx, causation)
	}
	return got
}

func requireAbout(t *testing.T, e outboxEvent, tx wager.Snapshot, causation string) {
	t.Helper()
	requireAboutAt(t, e, tx, causation, t0)
}

func requireAboutAt(t *testing.T, e outboxEvent, tx wager.Snapshot, causation string, at time.Time) {
	t.Helper()
	env, walletID := e.Envelope, tx.WalletID.String()
	checks := []struct {
		name      string
		got, want any
	}{
		{"aggregate type", e.AggregateType, "Wallet"},
		{"aggregate id", e.AggregateID, walletID},
		{"envelope event id", env.EventID, e.ID.String()},
		{"envelope event type", env.EventType, e.Type},
		{"envelope aggregate id", env.AggregateID, walletID},
		{"data walletId", env.Data["walletId"], walletID},
		{"data transactionId", env.Data["transactionId"], tx.ID.String()},
		{"correlation id", e.CorrelationID, tx.CorrelationID},
		{"envelope correlation id", env.CorrelationID, tx.CorrelationID},
		{"causation id", e.CausationID, causation},
		{"envelope causation id", env.CausationID, causation},
		{"version", env.Version, json.Number("1")},
		{"occurred at", env.OccurredAt, at.UTC().Format("2006-01-02T15:04:05.000Z")},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: %s = %v, want %v", e.Type, c.name, c.got, c.want)
		}
	}
	if kind, ok := env.Data["kind"]; ok && kind != string(tx.Kind) {
		t.Errorf("%s: data kind = %v, want %s", e.Type, kind, tx.Kind)
	}
}

func requireData(t *testing.T, e outboxEvent, want map[string]any) {
	t.Helper()
	for field, w := range want {
		if m, ok := w.(money.Money); ok {
			w = map[string]any{"amount": m.String(), "currency": m.Currency().Code()}
		}
		if got := e.Envelope.Data[field]; !reflect.DeepEqual(got, w) {
			t.Errorf("%s: data %s = %v (%T), want %v (%T)", e.Type, field, got, got, w, w)
		}
	}
}

func deref[T any](p *T) any {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func countRows(t *testing.T, db *pgx.Conn, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func lockWallet(t *testing.T, id uuid.UUID) {
	t.Helper()
	holder := connect(t)
	lock, err := holder.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = lock.Rollback(context.Background()) })
	if _, err := lock.Exec(t.Context(), "SELECT id FROM wallets WHERE id = $1 FOR UPDATE", id); err != nil {
		t.Fatalf("lock wallet %s: %v", id, err)
	}
}

func awaitLockWait(t *testing.T, db *pgx.Conn) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for countRows(t, db, "SELECT count(*) FROM pg_locks WHERE NOT granted") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no session waited for a lock within 5s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type faultSwitch struct{ err atomic.Pointer[error] }

func (f *faultSwitch) arm(err error) { f.err.Store(&err) }

func (f *faultSwitch) disarm() { f.err.Store(nil) }

func (f *faultSwitch) armed() error {
	if err := f.err.Load(); err != nil {
		return *err
	}
	return nil
}

type recordingMetrics struct {
	mu           sync.Mutex
	transactions []recordedTransaction
	duplicates   []string
	retries      []string
}

type recordedTransaction struct {
	channel, kind, status, failureCode string
}

func (m *recordingMetrics) Transaction(channel, kind, status, failureCode string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.transactions = append(m.transactions, recordedTransaction{channel, kind, status, failureCode})
}

func (m *recordingMetrics) Duplicate(channel, source string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.duplicates = append(m.duplicates, channel+"/"+source)
}

func (m *recordingMetrics) TxRetry(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retries = append(m.retries, reason)
}

func (m *recordingMetrics) snapshot() (txs []recordedTransaction, duplicates, retries []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.transactions), slices.Clone(m.duplicates), slices.Clone(m.retries)
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) records(t *testing.T, level slog.Level) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var got []map[string]any
	for line := range bytes.Lines(b.buf.Bytes()) {
		var rec map[string]any
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if rec[slog.LevelKey] == level.String() {
			got = append(got, rec)
		}
	}
	return got
}
