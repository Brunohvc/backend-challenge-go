//go:build integration

package resolver_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wallets"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/worker"
	"github.com/Brunohvc/backend-challenge-go/internal/worker/resolver"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var pg *testenv.Postgres

func TestMain(m *testing.M) {
	os.Exit(testenv.RunWithPostgres(m, func(p *testenv.Postgres) { pg = p }))
}

const (
	provider     = "provider-a"
	settleWithin = 5 * time.Second
	stopWithin   = 5 * time.Second
)

func integrationConfig() config.ResolverConfig {
	return config.ResolverConfig{
		Enabled:               true,
		PollInterval:          20 * time.Millisecond,
		BatchSize:             10,
		Lease:                 30 * time.Second,
		MaxAttempts:           100,
		TTL:                   15 * time.Minute,
		BackoffBase:           50 * time.Millisecond,
		BackoffMax:            200 * time.Millisecond,
		PermanentFailureLimit: 3,
	}
}

type stack struct {
	cfg      config.ResolverConfig
	pool     *pgxpool.Pool
	log      *slog.Logger
	wallets  *wallets.Service
	wagering *wagering.Service
}

func newStack(t *testing.T, cfg config.ResolverConfig) *stack {
	t.Helper()
	if err := pg.Reset(t.Context()); err != nil {
		t.Fatalf("reset: %v", err)
	}
	pool, err := postgres.NewPool(t.Context(), config.DBConfig{
		URL:              pg.AppDSN,
		MaxConns:         8,
		LockTimeout:      2 * time.Second,
		StatementTimeout: 10 * time.Second,
		TxMaxAttempts:    3,
	}, "test")
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(pool.Close)

	uow := postgres.NewUnitOfWork(pool, tracing.Tracer{})
	m, _ := metrics.New()
	log := logging.New(t.Output(), slog.LevelInfo, "test")
	tx := retry.Policy{Attempts: 3, Base: 5 * time.Millisecond, Max: 50 * time.Millisecond}
	return &stack{
		cfg:     cfg,
		pool:    pool,
		log:     log,
		wallets: wallets.NewService(uow, clock.System{}, idgen.V7{}, m, log, tx),
		wagering: wagering.NewService(uow, clock.System{}, idgen.V7{}, m, tracing.Tracer{}, log, wagering.Config{
			Reference: wager.ReferencePolicy{
				MaxAttempts: cfg.MaxAttempts,
				TTL:         cfg.TTL,
				BackoffBase: cfg.BackoffBase,
				BackoffMax:  cfg.BackoffMax,
			},
			Tx:                    tx,
			PermanentFailureLimit: cfg.PermanentFailureLimit,
		}),
	}
}

func startResolver(t *testing.T, s *stack, name string, counted resolver.Metrics) *worker.Runner {
	t.Helper()
	return startResolverOn(t, s, s.wagering, name, counted)
}

func startResolverOn(t *testing.T, s *stack, svc resolver.ResumeService, name string, counted resolver.Metrics) *worker.Runner {
	t.Helper()
	r := resolver.New(svc, s.cfg, counted, tracing.Tracer{}, s.log)
	runner := worker.NewRunner(name, s.cfg.PollInterval, r.RunOnce, s.log)
	if err := runner.Start(t.Context()); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	t.Cleanup(func() { stopRunner(t, runner) })
	return runner
}

type resumeCall struct {
	resolver string
	id       uuid.UUID
	outcome  wagering.ResumeOutcome
}

type resumeLog struct {
	mu    sync.Mutex
	calls []resumeCall
}

func (l *resumeLog) record(c resumeCall) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, c)
}

func (l *resumeLog) all() []resumeCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.calls)
}

type spyService struct {
	resolver.ResumeService
	resolver string
	log      *resumeLog
}

func (s spyService) Resume(ctx context.Context, due app.DueTransaction) (wagering.ResumeOutcome, error) {
	outcome, err := s.ResumeService.Resume(ctx, due)
	s.log.record(resumeCall{resolver: s.resolver, id: due.ID, outcome: outcome})
	return outcome, err
}

func stopRunner(t *testing.T, runner *worker.Runner) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), stopWithin)
	defer cancel()
	if err := runner.Stop(ctx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}
}

func (m *recordedResolutions) count(result string) int {
	n := 0
	for _, r := range m.all() {
		if r == result {
			n++
		}
	}
	return n
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, d)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func openWallet(t *testing.T, s *stack, initial string) wallet.Snapshot {
	t.Helper()
	amount, err := money.Parse(initial, money.BRL().Code())
	if err != nil {
		t.Fatalf("money.Parse(%q) error = %v", initial, err)
	}
	w, err := s.wallets.Open(t.Context(), wallets.OpenCommand{PlayerID: newID(), Initial: amount, CorrelationID: "corr-open"})
	if err != nil {
		t.Fatalf("open a wallet with %s: %v", initial, err)
	}
	return w
}

func command(w wallet.Snapshot, kind wager.Kind, amount, ref string) wager.CommandInput {
	ext := "ext-" + newID().String()
	return wager.CommandInput{
		ProviderID:                     provider,
		ExternalTransactionID:          ext,
		IdempotencyKey:                 "key-" + ext,
		PlayerID:                       w.PlayerID.String(),
		WalletID:                       w.ID.String(),
		RoundID:                        "round-1",
		GameID:                         "game-1",
		Kind:                           string(kind),
		Amount:                         amount,
		Currency:                       w.Balance.Currency().Code(),
		ReferenceExternalTransactionID: ref,
	}
}

func submit(t *testing.T, s *stack, in wager.CommandInput, want wager.Status) wager.Snapshot {
	t.Helper()
	cmd, err := wager.NewCommand(in)
	if err != nil {
		t.Fatalf("NewCommand(%+v) error = %v", in, err)
	}
	res, err := s.wagering.Submit(t.Context(), cmd, wagering.Source{
		Channel:       wagering.ChannelHTTP,
		CorrelationID: "corr-" + in.ExternalTransactionID,
	})
	if err != nil {
		t.Fatalf("Submit(%s %s) error = %v", in.Kind, in.Amount, err)
	}
	if got := res.Transaction.Status; got != want {
		t.Fatalf("%s %s ended %s, want %s", in.Kind, in.Amount, got, want)
	}
	return res.Transaction
}

func awaitingRefunds(t *testing.T, s *stack, w wallet.Snapshot, n int) []uuid.UUID {
	t.Helper()
	refunds := make([]uuid.UUID, n)
	bets := make([]wager.CommandInput, n)
	for i := range n {
		bets[i] = command(w, wager.KindBet, fmt.Sprintf("%d.00", i+1), "")
		refund := command(w, wager.KindRefund, bets[i].Amount, bets[i].ExternalTransactionID)
		refunds[i] = submit(t, s, refund, wager.StatusPendingReference).ID
	}
	for _, bet := range bets {
		submit(t, s, bet, wager.StatusProcessed)
	}
	return refunds
}

func stored(t *testing.T, s *stack, id uuid.UUID) wager.Snapshot {
	t.Helper()
	tx, err := s.wagering.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("Get(%s) error = %v", id, err)
	}
	return tx
}

func countIn(t *testing.T, s *stack, status wager.Status, ids []uuid.UUID) int {
	t.Helper()
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	var n int
	err := s.pool.QueryRow(t.Context(),
		"SELECT count(*) FROM wager_transactions WHERE id = ANY($1::uuid[]) AND status = $2", strs, string(status)).Scan(&n)
	if err != nil {
		t.Fatalf("count the transactions in %s: %v", status, err)
	}
	return n
}

func requireBalance(t *testing.T, s *stack, id uuid.UUID, want string) {
	t.Helper()
	w, err := s.wallets.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("get wallet %s: %v", id, err)
	}
	if got := w.Balance.String(); got != want {
		t.Errorf("wallet %s holds %s, want %s", id, got, want)
	}
	rec, err := s.wallets.Reconcile(t.Context(), id)
	if err != nil {
		t.Fatalf("reconcile wallet %s: %v", id, err)
	}
	if !rec.Consistent {
		t.Errorf("wallet %s holds %s, but its ledger adds up to %s", id, rec.Stored, rec.Calculated)
	}
}

func entriesByTransaction(t *testing.T, s *stack, id uuid.UUID) (map[uuid.UUID][]wallet.LedgerEntry, int) {
	t.Helper()
	page, err := s.wallets.Ledger(t.Context(), id, 0, 1000)
	if err != nil {
		t.Fatalf("ledger of wallet %s: %v", id, err)
	}
	if page.NextAfterSeq != nil {
		t.Fatalf("ledger of wallet %s has more than one page", id)
	}
	byTx := make(map[uuid.UUID][]wallet.LedgerEntry)
	for _, rec := range page.Records {
		byTx[rec.Entry.TransactionID()] = append(byTx[rec.Entry.TransactionID()], rec.Entry)
	}
	return byTx, len(page.Records)
}

func requireCredit(t *testing.T, tx uuid.UUID, entries []wallet.LedgerEntry, amount string) {
	t.Helper()
	if len(entries) != 1 || entries[0].Direction() != wallet.DirectionCredit || entries[0].Amount().String() != amount {
		t.Errorf("transaction %s has the ledger entries %v, want a single credit of %s", tx, entries, amount)
	}
}

type outboxEvent struct {
	Type          string
	CorrelationID string
	CausationID   string
}

func requireEvents(t *testing.T, s *stack, id uuid.UUID, want ...outboxEvent) {
	t.Helper()
	rows, err := s.pool.Query(t.Context(),
		`SELECT event_type, correlation_id, COALESCE(causation_id, '')
		   FROM outbox_events
		  WHERE payload->'data'->>'transactionId' = $1
		  ORDER BY seq`, id.String())
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[outboxEvent])
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("events about transaction %s = %v, want %v", id, got, want)
	}
}

func TestResolver_ResolvesPendingRefund(t *testing.T) {
	s := newStack(t, integrationConfig())
	w := openWallet(t, s, "100.00")
	betIn := command(w, wager.KindBet, "25.00", "")
	refund := submit(t, s, command(w, wager.KindRefund, "25.00", betIn.ExternalTransactionID), wager.StatusPendingReference)
	counted := &recordedResolutions{}
	runner := startResolver(t, s, "resolver", counted)
	eventually(t, settleWithin, "a look for the missing bet", func() bool { return counted.count("rescheduled") > 0 })

	bet := submit(t, s, betIn, wager.StatusProcessed)

	eventually(t, 3*time.Second, "the resolution of the refund", func() bool {
		return stored(t, s, refund.ID).Status == wager.StatusProcessed
	})
	stopRunner(t, runner)
	got := stored(t, s, refund.ID)
	if got.ReferenceTransactionID != (uuid.NullUUID{UUID: bet.ID, Valid: true}) {
		t.Errorf("refund resolved to %v, want the bet %s", got.ReferenceTransactionID, bet.ID)
	}
	if got.ResultBalance == nil {
		t.Error("refund recorded no balance, want 100.00")
	} else if b := got.ResultBalance.String(); b != "100.00" {
		t.Errorf("refund recorded the balance %s, want 100.00", b)
	}
	requireBalance(t, s, w.ID, "100.00")
	entries, _ := entriesByTransaction(t, s, w.ID)
	requireCredit(t, refund.ID, entries[refund.ID], "25.00")
	requireEvents(t, s, refund.ID,
		outboxEvent{event.TypeWagerTransactionPendingReference, refund.CorrelationID, ""},
		outboxEvent{event.TypeWagerTransactionProcessed, refund.CorrelationID, refund.ID.String()},
		outboxEvent{event.TypeWalletBalanceChanged, refund.CorrelationID, refund.ID.String()},
	)
	if n := counted.count("processed"); n != 1 {
		t.Errorf("counted %d processed resolutions, want 1", n)
	}
}

func TestResolver_ExpiresPending(t *testing.T) {
	cfg := integrationConfig()
	cfg.TTL = 3 * time.Second
	s := newStack(t, cfg)
	w := openWallet(t, s, "100.00")
	refund := submit(t, s, command(w, wager.KindRefund, "25.00", "bet-never-sent"), wager.StatusPendingReference)
	if refund.ExpiresAt == nil {
		t.Fatal("the waiting refund has no expiry")
	}
	expiry := *refund.ExpiresAt
	counted := &recordedResolutions{}

	runner := startResolver(t, s, "resolver", counted)

	eventually(t, cfg.TTL+settleWithin, "the rejection of the refund", func() bool {
		return stored(t, s, refund.ID).Status == wager.StatusRejected
	})
	stopRunner(t, runner)
	got := stored(t, s, refund.ID)
	if got.FailureCode != wager.FailureReferenceNotFound {
		t.Errorf("refund rejected with %q, want %q", got.FailureCode, wager.FailureReferenceNotFound)
	}
	if got.ProcessedAt == nil {
		t.Errorf("rejected refund has no processing time")
	} else if got.ProcessedAt.Before(expiry) {
		t.Errorf("refund rejected at %v, want once the wait was over, at %v or later", *got.ProcessedAt, expiry)
	}
	requireBalance(t, s, w.ID, "100.00")
	if entries, _ := entriesByTransaction(t, s, w.ID); len(entries[refund.ID]) != 0 {
		t.Errorf("rejected refund has the ledger entries %v, want none", entries[refund.ID])
	}
	requireEvents(t, s, refund.ID,
		outboxEvent{event.TypeWagerTransactionPendingReference, refund.CorrelationID, ""},
		outboxEvent{event.TypeWagerTransactionRejected, refund.CorrelationID, refund.ID.String()},
	)
	if counted.count("rejected") != 1 || counted.count("rescheduled") == 0 {
		t.Errorf("counted %q, want the looks that missed and then the rejection", counted.all())
	}
}

func TestResolver_TwoResolversNoDoubleProcessing(t *testing.T) {
	const walletCount, refundsPerWallet = 5, 10
	cfg := integrationConfig()
	cfg.BatchSize = 3
	s := newStack(t, cfg)
	refundsOf := make(map[uuid.UUID][]uuid.UUID, walletCount)
	var refunds []uuid.UUID
	for range walletCount {
		w := openWallet(t, s, "100.00")
		refundsOf[w.ID] = awaitingRefunds(t, s, w, refundsPerWallet)
		refunds = append(refunds, refundsOf[w.ID]...)
	}
	first, second := &recordedResolutions{}, &recordedResolutions{}
	resumes := &resumeLog{}
	spy := func(name string) resolver.ResumeService {
		return spyService{ResumeService: s.wagering, resolver: name, log: resumes}
	}

	runners := []*worker.Runner{
		startResolverOn(t, s, spy("resolver-1"), "resolver-1", first),
		startResolverOn(t, s, spy("resolver-2"), "resolver-2", second),
	}

	eventually(t, 20*time.Second, "the resolution of every refund", func() bool {
		return countIn(t, s, wager.StatusProcessed, refunds) == len(refunds)
	})
	for _, r := range runners {
		stopRunner(t, r)
	}
	for walletID, ids := range refundsOf {
		requireBalance(t, s, walletID, "100.00")
		entries, total := entriesByTransaction(t, s, walletID)
		if want := 1 + 2*refundsPerWallet; total != want {
			t.Errorf("wallet %s has %d ledger entries, want the opening, %d bets and %d refunds", walletID, total, refundsPerWallet, refundsPerWallet)
		}
		for i, id := range ids {
			requireCredit(t, id, entries[id], fmt.Sprintf("%d.00", i+1))
		}
	}
	processed := first.count("processed") + second.count("processed")
	if processed != len(refunds) || len(first.all())+len(second.all()) != processed {
		t.Errorf("the resolvers counted %q and %q, want %d processed resolutions between them and nothing else",
			first.all(), second.all(), len(refunds))
	}
	requireResumedOnce(t, resumes.all(), refunds, "resolver-1", "resolver-2")
}

func requireResumedOnce(t *testing.T, calls []resumeCall, ids []uuid.UUID, resolvers ...string) {
	t.Helper()
	if len(calls) != len(ids) {
		t.Errorf("%d Resume calls, want %d, one per refund", len(calls), len(ids))
	}
	resumed := make(map[uuid.UUID]int, len(ids))
	made := make(map[string]int, len(resolvers))
	for _, c := range calls {
		resumed[c.id]++
		made[c.resolver]++
		if c.outcome == wagering.ResumeSkipped {
			t.Errorf("%s was skipped by %s, want every claim to settle its transaction", c.id, c.resolver)
		}
	}
	for _, id := range ids {
		if n := resumed[id]; n != 1 {
			t.Errorf("refund %s was resumed %d times, want once", id, n)
		}
		delete(resumed, id)
	}
	for id, n := range resumed {
		t.Errorf("%s was resumed %d times, want no Resume of what is not a refund of the test", id, n)
	}
	for _, name := range resolvers {
		if made[name] == 0 {
			t.Errorf("%s made no Resume call, want it to share the backlog; calls by resolver: %v", name, made)
		}
	}
	t.Logf("Resume calls by resolver: %v", made)
}

func TestResolver_StopsCleanly(t *testing.T) {
	alreadyRunning := goleak.IgnoreCurrent()
	cfg := integrationConfig()
	cfg.BatchSize = 5
	s := newStack(t, cfg)
	w := openWallet(t, s, "1000.00")
	refunds := awaitingRefunds(t, s, w, 30)
	counted := &recordedResolutions{}
	runner := startResolver(t, s, "resolver", counted)
	eventually(t, settleWithin, "a first resolution", func() bool { return counted.count("processed") > 0 })

	stopRunner(t, runner)

	processed := countIn(t, s, wager.StatusProcessed, refunds)
	if waiting := countIn(t, s, wager.StatusPendingReference, refunds); processed+waiting != len(refunds) {
		t.Errorf("%d refunds processed and %d waiting, want all %d in one or the other", processed, waiting, len(refunds))
	}
	if n := counted.count("processed"); n != processed {
		t.Errorf("counted %d processed resolutions, want %d, one per processed refund", n, processed)
	}
	t.Logf("stopped with %d of %d refunds processed", processed, len(refunds))
	s.pool.Close()
	goleak.VerifyNone(t, alreadyRunning)
}
