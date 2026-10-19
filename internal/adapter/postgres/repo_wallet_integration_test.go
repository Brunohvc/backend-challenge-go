//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

var t1 = t0.Add(time.Minute)

var classes = []error{app.ErrNotFound, app.ErrAlreadyExists, app.ErrRetryable, app.ErrUnavailable, app.ErrPermanent}

func dbConfig() config.DBConfig {
	return config.DBConfig{
		URL:              pg.AppDSN,
		MaxConns:         8,
		LockTimeout:      2 * time.Second,
		StatementTimeout: 10 * time.Second,
		TxMaxAttempts:    3,
	}
}

func newPool(t *testing.T, cfg config.DBConfig) *pgxpool.Pool {
	t.Helper()
	pool, err := postgres.NewPool(t.Context(), cfg, "test")
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newUnitOfWork(t *testing.T) (*postgres.UnitOfWork, *pgxpool.Pool) {
	t.Helper()
	pool := newPool(t, dbConfig())
	return postgres.NewUnitOfWork(pool, tracing.Tracer{}), pool
}

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL())
	if err != nil {
		t.Fatalf("money.FromMinor(%d) error = %v", minor, err)
	}
	return m
}

func currency(t *testing.T, code string) money.Currency {
	t.Helper()
	c, err := money.ParseCurrency(code)
	if err != nil {
		t.Fatalf("money.ParseCurrency(%q) error = %v", code, err)
	}
	return c
}

func newWallet(t *testing.T, player uuid.UUID, initial money.Money) (*wallet.Wallet, *wallet.LedgerEntry) {
	t.Helper()
	w, entry, err := wallet.Open(wallet.OpenParams{
		ID:             newID(),
		PlayerID:       player,
		Initial:        initial,
		OpeningTxID:    newID(),
		OpeningEntryID: newID(),
		Now:            t0,
	})
	if err != nil {
		t.Fatalf("wallet.Open() error = %v", err)
	}
	return w, entry
}

func walletFields(w *wallet.Wallet) row {
	return row{
		"id":            w.ID(),
		"player_id":     w.PlayerID(),
		"currency":      w.Currency().Code(),
		"balance_minor": w.Balance().Minor(),
		"version":       w.Version(),
	}
}

func openWallet(t *testing.T, uow app.UnitOfWork, balance int64) (*wallet.Wallet, *wallet.LedgerEntry) {
	t.Helper()
	w, entry := newWallet(t, newID(), brl(t, balance))
	var opening *wager.Transaction
	if entry != nil {
		opening = txOpening(t, w, entry)
	}
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		if err := r.Wallets().Insert(ctx, w); err != nil {
			return err
		}
		if opening == nil {
			return nil
		}
		if err := r.Transactions().Insert(ctx, opening); err != nil {
			return err
		}
		return r.Ledger().Append(ctx, *entry)
	})
	if err != nil {
		t.Fatalf("open a wallet with %d: %v", balance, err)
	}
	return w, entry
}

func movement(ctx context.Context, r app.Repos, id uuid.UUID, kind string, amount int64) (wallet.LedgerEntry, error) {
	w, err := r.Wallets().GetForUpdate(ctx, id)
	if err != nil {
		return wallet.LedgerEntry{}, err
	}
	read := w.Version()
	m, err := money.FromMinor(amount, w.Currency())
	if err != nil {
		return wallet.LedgerEntry{}, err
	}

	tx := externalRow(walletFields(w), kind, amount)
	var entry wallet.LedgerEntry
	switch kind {
	case "BET":
		entry, err = w.Debit(m, get[uuid.UUID](tx, "id"), newID(), t1)
	case "WIN":
		entry, err = w.Credit(m, get[uuid.UUID](tx, "id"), newID(), t1)
	default:
		return wallet.LedgerEntry{}, fmt.Errorf("movement: kind %s moves no money", kind)
	}
	if err != nil {
		return wallet.LedgerEntry{}, err
	}

	tx = tx.set("result_balance_minor", w.Balance().Minor()).set("result_wallet_version", w.Version())
	if err := insert(ctx, postgres.TxOf(r), "wager_transactions", tx); err != nil {
		return wallet.LedgerEntry{}, fmt.Errorf("insert the %s: %w", kind, err)
	}
	if err := r.Ledger().Append(ctx, entry); err != nil {
		return wallet.LedgerEntry{}, err
	}
	return entry, r.Wallets().UpdateBalance(ctx, w, read)
}

func commitMovement(ctx context.Context, uow app.UnitOfWork, id uuid.UUID, kind string, amount int64) (wallet.LedgerEntry, error) {
	var entry wallet.LedgerEntry
	err := uow.Do(ctx, func(ctx context.Context, r app.Repos) error {
		var err error
		entry, err = movement(ctx, r, id, kind, amount)
		return err
	})
	if err != nil {
		return wallet.LedgerEntry{}, fmt.Errorf("%s %d on wallet %s: %w", kind, amount, id, err)
	}
	return entry, nil
}

func moveWallet(t *testing.T, uow app.UnitOfWork, id uuid.UUID, kind string, amount int64) wallet.LedgerEntry {
	t.Helper()
	entry, err := commitMovement(t.Context(), uow, id, kind, amount)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func loadWallet(t *testing.T, uow app.UnitOfWork, id uuid.UUID) (*wallet.Wallet, error) {
	t.Helper()
	var w *wallet.Wallet
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		w, err = r.Wallets().Get(ctx, id)
		return err
	})
	return w, err
}

func mustLoadWallet(t *testing.T, uow app.UnitOfWork, id uuid.UUID) *wallet.Wallet {
	t.Helper()
	w, err := loadWallet(t, uow, id)
	if err != nil {
		t.Fatalf("get wallet %s: %v", id, err)
	}
	return w
}

func requireClass(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	for _, c := range classes {
		if !errors.Is(c, want) && errors.Is(err, c) {
			t.Fatalf("error = %v, is %v as well as %v", err, c, want)
		}
	}
}

func requireSnapshot(t *testing.T, got, want wallet.Snapshot) {
	t.Helper()
	if got.ID != want.ID || got.PlayerID != want.PlayerID || !got.Balance.Equal(want.Balance) ||
		got.Version != want.Version || !got.CreatedAt.Equal(want.CreatedAt) || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("wallet = %+v, want %+v", got, want)
	}
	if got.CreatedAt.Location() != time.UTC || got.UpdatedAt.Location() != time.UTC {
		t.Fatalf("wallet times are in %s and %s, want UTC", got.CreatedAt.Location(), got.UpdatedAt.Location())
	}
}

func holdWalletLock(t *testing.T, uow app.UnitOfWork, id uuid.UUID) (release func()) {
	t.Helper()
	locked, unlock := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- uow.Do(context.WithoutCancel(t.Context()), func(ctx context.Context, r app.Repos) error {
			if _, err := r.Wallets().GetForUpdate(ctx, id); err != nil {
				return err
			}
			close(locked)
			<-unlock
			return nil
		})
	}()
	select {
	case <-locked:
	case err := <-done:
		t.Fatalf("lock wallet %s: %v", id, err)
	}

	var once sync.Once
	release = func() {
		once.Do(func() {
			close(unlock)
			if err := <-done; err != nil {
				t.Errorf("unit of work holding the lock of wallet %s: %v", id, err)
			}
		})
	}
	t.Cleanup(release)
	return release
}

func TestWalletRepo_InsertGetUpdate(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 1000)

	requireSnapshot(t, mustLoadWallet(t, uow, w.ID()).Snapshot(), w.Snapshot())
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		locked, err := r.Wallets().GetForUpdate(ctx, w.ID())
		if err != nil {
			return err
		}
		requireSnapshot(t, locked.Snapshot(), w.Snapshot())
		return nil
	})
	if err != nil {
		t.Fatalf("GetForUpdate: %v", err)
	}

	moveWallet(t, uow, w.ID(), "BET", 250)
	requireSnapshot(t, mustLoadWallet(t, uow, w.ID()).Snapshot(), wallet.Snapshot{
		ID:        w.ID(),
		PlayerID:  w.PlayerID(),
		Balance:   brl(t, 750),
		Version:   2,
		CreatedAt: t0,
		UpdatedAt: t1,
	})
}

func TestWalletRepo_GetUnknownIsNotFound(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	id := newID()

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		if _, err := r.Wallets().Get(ctx, id); !errors.Is(err, app.ErrNotFound) {
			return fmt.Errorf("Get: %w", err)
		}
		_, err := r.Wallets().GetForUpdate(ctx, id)
		return err
	})
	requireClass(t, err, app.ErrNotFound)
}

func TestWalletRepo_DuplicatePlayerCurrency(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	first, _ := openWallet(t, uow, 1000)

	second, _ := newWallet(t, first.PlayerID(), brl(t, 0))
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Wallets().Insert(ctx, second)
	})
	requireClass(t, err, app.ErrAlreadyExists)
	requireSQLState(t, err, uniqueViolation, "ux_wallets_player_currency")
	if _, err := loadWallet(t, uow, second.ID()); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("the refused wallet was stored: %v", err)
	}

	zeroUSD, err := money.FromMinor(0, currency(t, "USD"))
	if err != nil {
		t.Fatal(err)
	}
	usd, _ := newWallet(t, first.PlayerID(), zeroUSD)
	err = uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Wallets().Insert(ctx, usd)
	})
	if err != nil {
		t.Fatalf("insert a USD wallet for the same player: %v", err)
	}
}

func TestWalletRepo_IDByPlayerAndCurrency(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	first, _ := openWallet(t, uow, 1000)
	openWallet(t, uow, 500)

	zeroUSD, err := money.FromMinor(0, currency(t, "USD"))
	if err != nil {
		t.Fatal(err)
	}
	usd, _ := newWallet(t, first.PlayerID(), zeroUSD)
	err = uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Wallets().Insert(ctx, usd)
	})
	if err != nil {
		t.Fatalf("insert a USD wallet for the same player: %v", err)
	}

	err = uow.Snapshot(t.Context(), func(ctx context.Context, r app.Repos) error {
		for _, tc := range []struct {
			currency money.Currency
			want     uuid.UUID
		}{{money.BRL(), first.ID()}, {currency(t, "USD"), usd.ID()}} {
			got, err := r.Wallets().IDByPlayerAndCurrency(ctx, first.PlayerID(), tc.currency)
			if err != nil || got != tc.want {
				t.Errorf("%s wallet = %s, %v; want %s", tc.currency.Code(), got, err, tc.want)
			}
		}
		if _, err := r.Wallets().IDByPlayerAndCurrency(ctx, first.PlayerID(), currency(t, "EUR")); !errors.Is(err, app.ErrNotFound) {
			t.Errorf("EUR wallet: err = %v, want ErrNotFound", err)
		}
		if _, err := r.Wallets().IDByPlayerAndCurrency(ctx, newID(), money.BRL()); !errors.Is(err, app.ErrNotFound) {
			t.Errorf("unknown player: err = %v, want ErrNotFound", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWalletRepo_UpdateVersionConflict(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 1000)

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		stale, err := r.Wallets().Get(ctx, w.ID())
		if err != nil {
			return err
		}
		read := stale.Version()
		if _, err := commitMovement(ctx, uow, w.ID(), "BET", 100); err != nil {
			return err
		}
		if _, err := stale.Debit(brl(t, 300), newID(), newID(), t1); err != nil {
			return err
		}
		return r.Wallets().UpdateBalance(ctx, stale, read)
	})
	requireClass(t, err, app.ErrRetryable)
	if reason := app.RetryReasonOf(err); reason != app.RetryVersion {
		t.Errorf("retry reason = %q, want %q", reason, app.RetryVersion)
	}

	got := mustLoadWallet(t, uow, w.ID())
	if !got.Balance().Equal(brl(t, 900)) || got.Version() != 2 {
		t.Fatalf("wallet = %s at version %d, want the other writer's 9.00 at version 2", got.Balance(), got.Version())
	}
}

func TestWalletRepo_GetForUpdateSerializesWriters(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 1000)

	const writers = 16
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			_, err := commitMovement(t.Context(), uow, w.ID(), "BET", 10)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent BET: %v", err)
		}
	}

	got := mustLoadWallet(t, uow, w.ID())
	if !got.Balance().Equal(brl(t, 1000-writers*10)) || got.Version() != 1+writers {
		t.Fatalf("wallet = %s at version %d, want %s at version %d",
			got.Balance(), got.Version(), brl(t, 1000-writers*10), 1+writers)
	}
	records := ledgerPage(t, uow, w.ID(), 0, 100)
	if len(records) != 1+writers {
		t.Fatalf("ledger has %d entries, want %d", len(records), 1+writers)
	}
	for i := 1; i < len(records); i++ {
		prev, cur := records[i-1].Entry, records[i].Entry
		if !cur.BalanceBefore().Equal(prev.BalanceAfter()) {
			t.Fatalf("entry %d starts from %s, the previous one ended at %s", i, cur.BalanceBefore(), prev.BalanceAfter())
		}
	}
}

func TestWalletRepo_LockTimeoutIsRetryable(t *testing.T) {
	resetDB(t)
	cfg := dbConfig()
	cfg.LockTimeout = 200 * time.Millisecond
	uow := postgres.NewUnitOfWork(newPool(t, cfg), tracing.Tracer{})
	w, _ := openWallet(t, uow, 1000)
	release := holdWalletLock(t, uow, w.ID())

	start := time.Now()
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		_, err := r.Wallets().GetForUpdate(ctx, w.ID())
		return err
	})
	waited := time.Since(start)
	requireClass(t, err, app.ErrRetryable)
	requireSQLState(t, err, "55P03", "")
	if waited < cfg.LockTimeout {
		t.Fatalf("gave up after %s, before the lock timeout of %s", waited, cfg.LockTimeout)
	}

	release()
	moveWallet(t, uow, w.ID(), "BET", 100)
}

func TestWalletRepo_GetForUpdateLeavesReferencingInsertsAlone(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 1000)
	conn := connect(t, pg.AppDSN)
	mustExec(t, conn, "SET lock_timeout = '1s'")
	loss := func() row { return externalRow(walletFields(w), "LOSS", 0) }

	release := holdWalletLock(t, uow, w.ID())
	if err := insert(t.Context(), conn, "wager_transactions", loss()); err != nil {
		t.Fatalf("insert a transaction while the wallet is locked for update: %v", err)
	}
	release()

	other := connect(t, pg.AppDSN)
	tx := begin(t, other)
	mustExec(t, tx, "SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE", w.ID())
	err := insert(t.Context(), conn, "wager_transactions", loss())
	requireSQLState(t, err, "55P03", "")
}

func TestWalletRepo_UnloadableRowIsPermanent(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	gbp := walletRow(0).set("currency", "GBP")
	mustInsert(t, connect(t, pg.AppDSN), "wallets", gbp)

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		_, err := r.Wallets().Get(ctx, get[uuid.UUID](gbp, "id"))
		return err
	})
	requireClass(t, err, app.ErrPermanent)
	if !errors.Is(err, money.ErrUnsupportedCurrency) {
		t.Fatalf("Get() error = %v, want it to say why the row does not load", err)
	}
}
