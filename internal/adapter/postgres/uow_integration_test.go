//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
)

func requireNotStored(t *testing.T, uow app.UnitOfWork, id uuid.UUID) {
	t.Helper()
	if _, err := loadWallet(t, uow, id); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("wallet %s: %v, want it rolled back", id, err)
	}
}

func requireNoConnectionHeld(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if n := pool.Stat().AcquiredConns(); n != 0 {
		t.Fatalf("%d connections still acquired", n)
	}
}

func requireSetting(ctx context.Context, t *testing.T, r app.Repos, name, want string) {
	t.Helper()
	var got string
	if err := postgres.TxOf(r).QueryRow(ctx, "SELECT current_setting($1)", name).Scan(&got); err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if got != want {
		t.Fatalf("%s = %q, want %q", name, got, want)
	}
}

func TestUnitOfWork_RollbackOnError(t *testing.T) {
	resetDB(t)
	uow, pool := newUnitOfWork(t)
	w, _ := newWallet(t, newID(), brl(t, 0))
	errRefused := errors.New("refused by the use case")

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		if err := r.Wallets().Insert(ctx, w); err != nil {
			return err
		}
		return errRefused
	})
	if !errors.Is(err, errRefused) || err.Error() != errRefused.Error() {
		t.Fatalf("Do() error = %v, want %v unchanged", err, errRefused)
	}
	for _, c := range classes {
		if errors.Is(err, c) {
			t.Fatalf("Do() error = %v, classified as %v", err, c)
		}
	}
	requireNotStored(t, uow, w.ID())
	requireNoConnectionHeld(t, pool)
}

func TestUnitOfWork_RollbackOnPanic(t *testing.T) {
	resetDB(t)
	uow, pool := newUnitOfWork(t)
	w, _ := newWallet(t, newID(), brl(t, 0))
	type failure struct{ reason string }
	boom := failure{"bug in the use case"}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
			if err := r.Wallets().Insert(ctx, w); err != nil {
				return err
			}
			panic(boom)
		})
	}()
	if recovered != boom {
		t.Fatalf("recovered %v, want the panic of fn, %v", recovered, boom)
	}
	requireNotStored(t, uow, w.ID())
	requireNoConnectionHeld(t, pool)
	openWallet(t, uow, 0)
}

func TestUnitOfWork_CommitFailureIsClassified(t *testing.T) {
	resetDB(t)
	uow, pool := newUnitOfWork(t)
	w, _ := newWallet(t, newID(), brl(t, 500))

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Wallets().Insert(ctx, w)
	})
	requireClass(t, err, app.ErrPermanent)
	requireSQLState(t, err, ledgerMismatch, "wallet_matches_ledger")
	requireNotStored(t, uow, w.ID())
	requireNoConnectionHeld(t, pool)
}

func TestUnitOfWork_BeginFailureIsClassified(t *testing.T) {
	uow, _ := newUnitOfWork(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	called := false
	err := uow.Do(ctx, func(context.Context, app.Repos) error {
		called = true
		return nil
	})
	requireClass(t, err, app.ErrUnavailable)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() error = %v, want it to wrap context.Canceled", err)
	}
	if called {
		t.Fatal("fn ran without a transaction")
	}
}

func TestUnitOfWork_Snapshot_IsRepeatableRead(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 1000)

	err := uow.Snapshot(t.Context(), func(ctx context.Context, r app.Repos) error {
		requireSetting(ctx, t, r, "transaction_isolation", "repeatable read")
		requireSetting(ctx, t, r, "transaction_read_only", "on")
		before, err := r.Wallets().Get(ctx, w.ID())
		if err != nil {
			return err
		}
		totalsBefore, err := r.Ledger().Totals(ctx, w.ID())
		if err != nil {
			return err
		}

		if _, err := commitMovement(ctx, uow, w.ID(), "BET", 300); err != nil {
			return err
		}

		after, err := r.Wallets().Get(ctx, w.ID())
		if err != nil {
			return err
		}
		requireSnapshot(t, after.Snapshot(), before.Snapshot())
		totalsAfter, err := r.Ledger().Totals(ctx, w.ID())
		if err != nil {
			return err
		}
		if totalsAfter != totalsBefore {
			t.Fatalf("totals = %+v inside the snapshot, then %+v", totalsBefore, totalsAfter)
		}
		page, err := r.Ledger().Page(ctx, w.ID(), 0, 100)
		if err != nil {
			return err
		}
		if len(page) != 1 {
			t.Fatalf("ledger page inside the snapshot has %d entries, want the 1 it started with", len(page))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	err = uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		requireSetting(ctx, t, r, "transaction_isolation", "read committed")
		requireSetting(ctx, t, r, "transaction_read_only", "off")
		before, err := r.Wallets().Get(ctx, w.ID())
		if err != nil {
			return err
		}
		if before.Version() != 2 {
			t.Fatalf("Do sees version %d, want 2, what was committed", before.Version())
		}

		if _, err := commitMovement(ctx, uow, w.ID(), "BET", 100); err != nil {
			return err
		}

		after, err := r.Wallets().Get(ctx, w.ID())
		if err != nil {
			return err
		}
		if !after.Balance().Equal(brl(t, 600)) || after.Version() != 3 {
			t.Fatalf("Do sees %s at version %d after a commit, want 6.00 at version 3", after.Balance(), after.Version())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
}

func TestUnitOfWork_Snapshot_IsReadOnly(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := newWallet(t, newID(), brl(t, 0))

	err := uow.Snapshot(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Wallets().Insert(ctx, w)
	})
	requireClass(t, err, app.ErrPermanent)
	requireSQLState(t, err, "25006", "")
	requireNotStored(t, uow, w.ID())
}

func TestNewPool_SessionSettings(t *testing.T) {
	cfg := dbConfig()
	cfg.MaxConns = 3
	cfg.LockTimeout = 1500 * time.Millisecond
	cfg.StatementTimeout = 4 * time.Second
	pool, err := postgres.NewPool(t.Context(), cfg, "wallet-replica-2")
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	defer pool.Close()

	if got := pool.Config().MaxConns; got != cfg.MaxConns {
		t.Fatalf("MaxConns = %d, want %d", got, cfg.MaxConns)
	}
	for i := range cfg.MaxConns {
		conn, err := pool.Acquire(t.Context())
		if err != nil {
			t.Fatalf("acquire connection %d: %v", i, err)
		}
		defer conn.Release()

		var user, appName, lockTimeout, statementTimeout string
		err = conn.QueryRow(t.Context(), `SELECT current_user, current_setting('application_name'),
			(SELECT setting FROM pg_settings WHERE name = 'lock_timeout'),
			(SELECT setting FROM pg_settings WHERE name = 'statement_timeout')`).
			Scan(&user, &appName, &lockTimeout, &statementTimeout)
		if err != nil {
			t.Fatalf("read the settings of connection %d: %v", i, err)
		}
		if user != "wallet_app" || appName != "wallet-replica-2" || lockTimeout != "1500" || statementTimeout != "4000" {
			t.Fatalf("connection %d: user %q, application_name %q, lock_timeout %sms, statement_timeout %sms; "+
				"want wallet_app, wallet-replica-2, 1500ms, 4000ms", i, user, appName, lockTimeout, statementTimeout)
		}
	}
}

func TestNewPool_ConnectionFailures(t *testing.T) {
	u, err := url.Parse(pg.AppDSN)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("wallet_app", "not-the-password-s3cr3t")
	wrongPassword := u.String()
	nothingListening := strings.Replace(wrongPassword, u.Host, "127.0.0.1:1", 1)
	malformed := strings.Replace(wrongPassword, u.Host, u.Hostname()+":not-a-port", 1)

	tests := []struct {
		name string
		dsn  string
		want error
	}{
		{"wrong password", wrongPassword, app.ErrUnavailable},
		{"nothing listening", nothingListening, app.ErrUnavailable},
		{"malformed url", malformed, app.ErrPermanent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := dbConfig()
			cfg.URL = tc.dsn
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			pool, err := postgres.NewPool(ctx, cfg, "test")
			if err == nil {
				pool.Close()
				t.Fatal("NewPool() succeeded")
			}
			requireClass(t, err, tc.want)
			if strings.Contains(err.Error(), "s3cr3t") {
				t.Fatalf("NewPool() error leaks the password: %v", err)
			}
		})
	}
}

func TestRepos_ErrorsAreClassified(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, opening := openWallet(t, uow, 1000)

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		calls := map[string]func() error{
			"Wallets.Insert": func() error { return r.Wallets().Insert(canceled, w) },
			"Wallets.Get": func() error {
				_, err := r.Wallets().Get(canceled, w.ID())
				return err
			},
			"Wallets.GetForUpdate": func() error {
				_, err := r.Wallets().GetForUpdate(canceled, w.ID())
				return err
			},
			"Wallets.IDByPlayerAndCurrency": func() error {
				_, err := r.Wallets().IDByPlayerAndCurrency(canceled, w.PlayerID(), w.Currency())
				return err
			},
			"Wallets.UpdateBalance": func() error { return r.Wallets().UpdateBalance(canceled, w, w.Version()) },
			"Ledger.Append":         func() error { return r.Ledger().Append(canceled, *opening) },
			"Ledger.Page": func() error {
				_, err := r.Ledger().Page(canceled, w.ID(), 0, 10)
				return err
			},
			"Ledger.Totals": func() error {
				_, err := r.Ledger().Totals(canceled, w.ID())
				return err
			},
		}
		for name, call := range calls {
			err := call()
			if !errors.Is(err, app.ErrUnavailable) || !errors.Is(err, context.Canceled) {
				t.Errorf("%s on a canceled context: %v, want app.ErrUnavailable wrapping context.Canceled", name, err)
			}
		}
		_, err := r.Wallets().Get(ctx, w.ID())
		return err
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
}

func TestUnitOfWork_BusyConnectionIsPermanent(t *testing.T) {
	resetDB(t)
	uow, pool := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 1000)

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		rows, err := postgres.TxOf(r).Query(ctx, "SELECT generate_series(1, 2)")
		if err != nil {
			return err
		}
		defer rows.Close()
		_, err = r.Wallets().Get(ctx, w.ID())
		return err
	})

	requireClass(t, err, app.ErrPermanent)
	if !strings.Contains(err.Error(), "conn busy") {
		t.Fatalf("error = %v, want the busy connection pgx reports", err)
	}
	requireNoConnectionHeld(t, pool)
}
