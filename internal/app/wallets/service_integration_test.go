//go:build integration

package wallets_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wallets"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var pg *testenv.Postgres

func TestMain(m *testing.M) {
	os.Exit(testenv.RunWithPostgres(m, func(p *testenv.Postgres) { pg = p }))
}

type env struct {
	svc     *wallets.Service
	uow     *postgres.UnitOfWork
	metrics *countingMetrics
}

func newEnv(t *testing.T) *env {
	t.Helper()
	if err := pg.Reset(t.Context()); err != nil {
		t.Fatalf("reset: %v", err)
	}
	pool, err := postgres.NewPool(t.Context(), config.DBConfig{
		URL: pg.AppDSN, MaxConns: 20, LockTimeout: 2 * time.Second, StatementTimeout: 5 * time.Second, TxMaxAttempts: 3,
	}, "wallets-it")
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	e := &env{uow: postgres.NewUnitOfWork(pool, tracing.Tracer{}), metrics: &countingMetrics{}}
	e.svc = wallets.NewService(e.uow, clock.System{}, idgen.V7{}, e.metrics, slog.New(slog.NewJSONHandler(io.Discard, nil)), txPolicy)
	return e
}

func superuser(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), pg.SuperDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func count(t *testing.T, conn *pgx.Conn, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := conn.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func recordBet(t *testing.T, e *env, walletID uuid.UUID, amount string) {
	t.Helper()
	err := e.uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		w, err := r.Wallets().GetForUpdate(ctx, walletID)
		if err != nil {
			return err
		}
		ext := "bet-" + idgen.V7{}.New().String()
		cmd, err := wager.NewCommand(wager.CommandInput{
			ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "key-" + ext,
			PlayerID: w.PlayerID().String(), WalletID: walletID.String(), RoundID: "round-1", GameID: "game-1",
			Kind: "BET", Amount: amount, Currency: "BRL",
		})
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		tx, err := wager.NewExternal(idgen.V7{}.New(), cmd, "c-bet", now)
		if err != nil {
			return err
		}
		version := w.Version()
		entry, err := w.Debit(cmd.Money, tx.ID(), idgen.V7{}.New(), now)
		if err != nil {
			return err
		}
		if err := tx.MarkProcessed(wager.Result{BalanceAfter: w.Balance(), WalletVersion: w.Version()}, now); err != nil {
			return err
		}
		if err := r.Transactions().Insert(ctx, tx); err != nil {
			return err
		}
		if err := r.Ledger().Append(ctx, entry); err != nil {
			return err
		}
		return r.Wallets().UpdateBalance(ctx, w, version)
	})
	if err != nil {
		t.Fatalf("record BET of %s: %v", amount, err)
	}
}

func open(t *testing.T, e *env, initial string) uuid.UUID {
	t.Helper()
	snap, err := e.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: idgen.V7{}.New(), Initial: brl(t, initial), CorrelationID: "c-open"})
	if err != nil {
		t.Fatalf("Open %s: %v", initial, err)
	}
	return snap.ID
}

func TestOpen_PositiveIsAtomic(t *testing.T) {
	e := newEnv(t)
	db := superuser(t)

	id := open(t, e, "1000.00")

	if n := count(t, db, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED' AND origin = 'INTERNAL'`, id); n != 1 {
		t.Errorf("processed OPENING rows = %d, want 1", n)
	}
	if n := count(t, db, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'CREDIT' AND amount_minor = 100000 AND balance_before_minor = 0 AND balance_after_minor = 100000`, id); n != 1 {
		t.Errorf("opening CREDIT entries = %d, want 1", n)
	}
	if n := count(t, db, `SELECT count(*) FROM wallets WHERE id = $1 AND version = 1 AND balance_minor = 100000`, id); n != 1 {
		t.Errorf("wallet at version 1 with 1000.00 = %d rows, want 1", n)
	}

	rows, err := db.Query(t.Context(), `SELECT event_type, aggregate_id, payload FROM outbox_events WHERE aggregate_id = $1 ORDER BY seq`, id.String())
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		eventType, aggregateID string
		payload                []byte
	}
	got, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.eventType, &x.aggregateID, &x.payload)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].eventType != "WagerTransactionProcessed" || got[1].eventType != "WalletBalanceChanged" {
		t.Fatalf("outbox = %+v, want Processed then BalanceChanged", got)
	}
	for _, r := range got {
		var env struct {
			AggregateID   string         `json:"aggregateId"`
			CorrelationID string         `json:"correlationId"`
			CausationID   *string        `json:"causationId"`
			Data          map[string]any `json:"data"`
		}
		if err := json.Unmarshal(r.payload, &env); err != nil {
			t.Fatalf("payload of %s: %v", r.eventType, err)
		}
		if env.AggregateID != id.String() || env.Data["walletId"] != id.String() {
			t.Errorf("%s: aggregateId %s, data.walletId %v; want both %s", r.eventType, env.AggregateID, env.Data["walletId"], id)
		}
		if env.CorrelationID != "c-open" || env.CausationID != nil {
			t.Errorf("%s: correlationId %q, causationId %v; want c-open and none", r.eventType, env.CorrelationID, env.CausationID)
		}
	}
	var processed, changed map[string]any
	if err := json.Unmarshal(got[0].payload, &struct{ Data *map[string]any }{&processed}); err != nil {
		t.Fatalf("payload of %s: %v", got[0].eventType, err)
	}
	if err := json.Unmarshal(got[1].payload, &struct{ Data *map[string]any }{&changed}); err != nil {
		t.Fatalf("payload of %s: %v", got[1].eventType, err)
	}
	if processed["kind"] != "OPENING" || changed["direction"] != "CREDIT" {
		t.Errorf("kind = %v, direction = %v; want the domain spellings OPENING and CREDIT", processed["kind"], changed["direction"])
	}
}

func TestOpen_ZeroCreatesNothingFinancial(t *testing.T) {
	e := newEnv(t)
	db := superuser(t)

	id := open(t, e, "0.00")

	if n := count(t, db, `SELECT count(*) FROM wallets WHERE id = $1 AND version = 1 AND balance_minor = 0`, id); n != 1 {
		t.Errorf("wallet rows = %d, want 1 at 0.00 v1", n)
	}
	for table, sql := range map[string]string{
		"transactions": `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`,
		"ledger":       `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`,
		"outbox":       `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1::text`,
	} {
		if n := count(t, db, sql, id); n != 0 {
			t.Errorf("%s rows = %d, want 0", table, n)
		}
	}
}

func TestOpen_DuplicateConflict(t *testing.T) {
	e := newEnv(t)
	player := idgen.V7{}.New()
	first, err := e.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: player, Initial: brl(t, "10.00"), CorrelationID: "c-1"})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}

	_, err = e.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: player, Initial: brl(t, "20.00"), CorrelationID: "c-2"})

	if !errors.Is(err, wallets.ErrWalletAlreadyExists) {
		t.Errorf("err = %v, want ErrWalletAlreadyExists", err)
	}
	var exists *wallets.AlreadyExistsError
	if !errors.As(err, &exists) || exists.WalletID != first.ID {
		t.Errorf("err = %v, want an AlreadyExistsError naming wallet %s", err, first.ID)
	}
}

type result struct {
	id  uuid.UUID
	err error
}

func TestOpen_ConcurrentSamePlayerCurrency(t *testing.T) {
	e := newEnv(t)
	db := superuser(t)
	player := idgen.V7{}.New()

	const n = 10
	initial := brl(t, "100.00")
	results := make([]result, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Go(func() {
			<-start
			snap, err := e.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: player, Initial: initial, CorrelationID: "c-race"})
			results[i] = result{id: snap.ID, err: err}
		})
	}
	close(start)
	wg.Wait()

	var winner uuid.UUID
	ok, dup := 0, 0
	for _, res := range results {
		switch {
		case res.err == nil:
			ok++
			winner = res.id
		case errors.Is(res.err, wallets.ErrWalletAlreadyExists):
			dup++
		default:
			t.Errorf("unexpected error: %v", res.err)
		}
	}
	if ok != 1 || dup != n-1 {
		t.Errorf("successes = %d, conflicts = %d; want 1 and %d", ok, dup, n-1)
	}
	for _, res := range results {
		if res.err == nil {
			continue
		}
		var exists *wallets.AlreadyExistsError
		if !errors.As(res.err, &exists) || exists.WalletID != winner {
			t.Errorf("conflict = %v, want an AlreadyExistsError naming the winner %s", res.err, winner)
		}
	}
	if w := count(t, db, `SELECT count(*) FROM wallets WHERE player_id = $1`, player); w != 1 {
		t.Errorf("wallets = %d, want 1", w)
	}
	if o := count(t, db, `SELECT count(*) FROM wager_transactions WHERE player_id = $1 AND kind = 'OPENING'`, player); o > 1 {
		t.Errorf("OPENING rows = %d, want at most 1", o)
	}
}

func TestGet(t *testing.T) {
	e := newEnv(t)
	id := open(t, e, "12.34")

	got, err := e.svc.Get(t.Context(), id)
	if err != nil || got.ID != id || got.Balance.String() != "12.34" || got.Version != 1 {
		t.Errorf("Get = %+v, %v; want the wallet with 12.34 v1", got, err)
	}
	if _, err := e.svc.Get(t.Context(), idgen.V7{}.New()); !errors.Is(err, wallets.ErrWalletNotFound) {
		t.Errorf("unknown wallet: err = %v, want ErrWalletNotFound", err)
	}
}

func TestLedger_Pagination(t *testing.T) {
	e := newEnv(t)
	id := open(t, e, "1000.00")
	for _, amount := range []string{"1.00", "2.00", "3.00", "4.00"} {
		recordBet(t, e, id, amount)
	}

	var seen []int64
	var after int64
	for page := 0; ; page++ {
		p, err := e.svc.Ledger(t.Context(), id, after, 2)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, r := range p.Records {
			seen = append(seen, r.Seq)
		}
		if page == 0 {
			recordBet(t, e, id, "5.00")
		}
		if p.NextAfterSeq == nil {
			break
		}
		after = *p.NextAfterSeq
	}

	if len(seen) != 6 {
		t.Fatalf("entries read = %v, want 6", seen)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] <= seen[i-1] {
			t.Fatalf("seqs %v are not strictly increasing: an entry was repeated or misordered", seen)
		}
	}
	if _, err := e.svc.Ledger(t.Context(), idgen.V7{}.New(), 0, 10); !errors.Is(err, wallets.ErrWalletNotFound) {
		t.Errorf("ledger of an unknown wallet: err = %v, want ErrWalletNotFound", err)
	}
}

func TestReconcile_Consistent(t *testing.T) {
	e := newEnv(t)
	id := open(t, e, "1000.00")
	recordBet(t, e, id, "25.00")

	got, err := e.svc.Reconcile(t.Context(), id)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !got.Consistent || got.Stored.String() != "975.00" || got.Calculated.String() != "975.00" ||
		!got.Difference.IsZero() || got.CheckedEntries != 2 || got.WalletID != id {
		t.Errorf("reconciliation = %+v, want 975.00 both ways over 2 entries", got)
	}
	if e.metrics.divergences != 0 {
		t.Errorf("divergences = %d, want none", e.metrics.divergences)
	}
}

func TestReconcile_ZeroEntries(t *testing.T) {
	e := newEnv(t)
	id := open(t, e, "0.00")

	got, err := e.svc.Reconcile(t.Context(), id)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !got.Consistent || got.CheckedEntries != 0 || got.Stored.String() != "0.00" || got.Calculated.String() != "0.00" {
		t.Errorf("reconciliation = %+v, want consistent at 0.00 with no entries", got)
	}
	if _, err := e.svc.Reconcile(t.Context(), idgen.V7{}.New()); !errors.Is(err, wallets.ErrWalletNotFound) {
		t.Errorf("unknown wallet: err = %v, want ErrWalletNotFound", err)
	}
}

func TestReconcile_DetectsDivergence(t *testing.T) {
	e := newEnv(t)
	id := open(t, e, "1000.00")
	db := superuser(t)
	tx, err := db.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `UPDATE wallets SET balance_minor = balance_minor + 500 WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	got, err := e.svc.Reconcile(t.Context(), id)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got.Consistent || got.Stored.String() != "1005.00" || got.Calculated.String() != "1000.00" ||
		got.Difference.String() != "5.00" || got.CheckedEntries != 1 {
		t.Errorf("reconciliation = %+v, want stored 1005.00, calculated 1000.00, difference 5.00", got)
	}
	if e.metrics.divergences != 1 {
		t.Errorf("divergences = %d, want 1", e.metrics.divergences)
	}
	if n := count(t, db, `SELECT count(*) FROM wallets WHERE id = $1 AND balance_minor = 100500 AND version = 1`, id); n != 1 {
		t.Error("the wallet changed after reconciliation, want it left as found")
	}
}
