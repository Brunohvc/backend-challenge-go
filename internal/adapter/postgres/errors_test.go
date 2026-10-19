package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
)

var sentinels = []error{app.ErrNotFound, app.ErrAlreadyExists, app.ErrRetryable, app.ErrUnavailable, app.ErrPermanent}

func pgError(code, constraint string) *pgconn.PgError {
	return &pgconn.PgError{Severity: "ERROR", Code: code, ConstraintName: constraint, Message: "refused"}
}

func connectError(t *testing.T) error {
	t.Helper()
	cfg, err := pgconn.ParseConfig("postgres://wallet_app@127.0.0.1:5432/wallet?sslmode=disable")
	if err != nil {
		t.Fatalf("ParseConfig() error = %v", err)
	}
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return nil, syscall.ECONNREFUSED
	}
	conn, err := pgconn.ConnectConfig(t.Context(), cfg)
	if err == nil {
		_ = conn.Close(t.Context())
		t.Fatal("ConnectConfig() connected through a dialer that always fails")
	}
	var connectErr *pgconn.ConnectError
	if !errors.As(err, &connectErr) {
		t.Fatalf("ConnectConfig() error = %T %v, want a *pgconn.ConnectError", err, err)
	}
	return err
}

type safeToRetryError string

func (e safeToRetryError) Error() string     { return string(e) }
func (e safeToRetryError) SafeToRetry() bool { return true }

var retryReasons = map[string]string{
	"serialization failure": app.RetrySerialization,
	"deadlock":              app.RetryDeadlock,
	"lock timeout":          app.RetryLockTimeout,
	"idempotency key race":  app.RetryUniqueRace,
	"external id race":      app.RetryUniqueRace,
	"reversal race":         app.RetryUniqueRace,
}

func TestClassify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want error
	}{
		{"serialization failure", pgError("40001", ""), app.ErrRetryable},
		{"deadlock", pgError("40P01", ""), app.ErrRetryable},
		{"lock timeout", pgError("55P03", ""), app.ErrRetryable},
		{"idempotency key race", pgError("23505", "ux_tx_provider_key"), app.ErrRetryable},
		{"external id race", pgError("23505", "ux_tx_provider_external"), app.ErrRetryable},
		{"reversal race", pgError("23505", "ux_tx_single_reversal"), app.ErrRetryable},

		{"second wallet for a player and currency", pgError("23505", "ux_wallets_player_currency"), app.ErrAlreadyExists},

		{"duplicate wallet id", pgError("23505", "wallets_pkey"), app.ErrPermanent},
		{"second opening of a wallet", pgError("23505", "ux_tx_single_opening"), app.ErrPermanent},
		{"second entry for a transaction", pgError("23505", "ux_ledger_wallet_tx"), app.ErrPermanent},

		{"connection exception", pgError("08000", ""), app.ErrUnavailable},
		{"connection failure", pgError("08006", ""), app.ErrUnavailable},
		{"connection refused by the server", pgError("08001", ""), app.ErrUnavailable},
		{"admin shutdown", pgError("57P01", ""), app.ErrUnavailable},
		{"crash shutdown", pgError("57P02", ""), app.ErrUnavailable},
		{"cannot connect now", pgError("57P03", ""), app.ErrUnavailable},
		{"statement timeout", pgError("57014", ""), app.ErrUnavailable},
		{"too many connections", pgError("53300", ""), app.ErrUnavailable},
		{"out of memory", pgError("53200", ""), app.ErrUnavailable},
		{"context deadline", context.DeadlineExceeded, app.ErrUnavailable},
		{"wrapped context deadline", fmt.Errorf("query: %w", context.DeadlineExceeded), app.ErrUnavailable},
		{"context canceled", context.Canceled, app.ErrUnavailable},
		{"network error", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, app.ErrUnavailable},
		{"connection closed mid-message", fmt.Errorf("receive message: %w", io.ErrUnexpectedEOF), app.ErrUnavailable},
		{"connection closed", io.EOF, app.ErrUnavailable},
		{"connection never established", connectError(t), app.ErrUnavailable},
		{"pool closed", puddle.ErrClosedPool, app.ErrUnavailable},
		{"connection closed before use", fmt.Errorf("exec: %w", pgconn.ErrConnClosed), app.ErrUnavailable},

		{"numeric out of range", pgError("22003", ""), app.ErrPermanent},
		{"invalid text representation", pgError("22P02", ""), app.ErrPermanent},
		{"check violation", pgError("23514", "ck_wallets_balance"), app.ErrPermanent},
		{"foreign key violation", pgError("23503", "fk_ledger_wallet"), app.ErrPermanent},
		{"not null violation", pgError("23502", ""), app.ErrPermanent},
		{"insufficient privilege", pgError("42501", ""), app.ErrPermanent},
		{"undefined table", pgError("42P01", ""), app.ErrPermanent},
		{"undefined column", pgError("42703", ""), app.ErrPermanent},
		{"ledger does not explain the balance", pgError("WL001", "wallet_matches_ledger"), app.ErrPermanent},
		{"guard violation", pgError("WL002", "wallets_guard"), app.ErrPermanent},
		{"raise exception", pgError("P0001", ""), app.ErrPermanent},
		{"write in a read-only transaction", pgError("25006", ""), app.ErrPermanent},
		{"internal error", pgError("XX000", ""), app.ErrPermanent},
		{"wrapped server error", fmt.Errorf("insert: %w", pgError("23514", "")), app.ErrPermanent},
		{"commit of a failed transaction", pgx.ErrTxCommitRollback, app.ErrPermanent},
		{"scan into the wrong type", errors.New("can't scan into dest[0]"), app.ErrPermanent},
		{"connection busy with another statement", safeToRetryError("conn busy"), app.ErrPermanent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := classify(tc.err)
			if !errors.Is(got, tc.want) {
				t.Fatalf("classify(%v) = %v, want it to wrap %v", tc.err, got, tc.want)
			}
			for _, s := range sentinels {
				if !errors.Is(s, tc.want) && errors.Is(got, s) {
					t.Errorf("classify(%v) = %v, also wraps %v", tc.err, got, s)
				}
			}
			if !errors.Is(got, tc.err) {
				t.Errorf("classify(%v) = %v, lost the cause", tc.err, got)
			}
			cause := tc.err
			var pgErr *pgconn.PgError
			if errors.As(tc.err, &pgErr) && integrityDescriptions[pgErr.Code] != "" {
				cause = &integrityError{pg: pgErr}
			}
			if want := tc.want.Error() + ": " + cause.Error(); got.Error() != want {
				t.Errorf("classify(%v) = %q, want %q", tc.err, got, want)
			}
			if reason, ok := retryReasons[tc.name]; ok {
				if r := app.RetryReasonOf(got); r != reason {
					t.Errorf("RetryReasonOf(classify(%v)) = %q, want %q", tc.err, r, reason)
				}
			} else if errors.Is(got, app.ErrRetryable) {
				t.Errorf("retryable case %q has no expected reason", tc.name)
			}
		})
	}
}

func TestClassify_IntegrityErrorsLeaveTheAmountsOut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code, constraint, message string
		amounts                   []string
	}{
		{"WL001", "wallet_matches_ledger", "wallet 7 balance 98765 does not match ledger 12345", []string{"98765", "12345"}},
		{"WL001", "ledger_entry_check", "ledger entry 7 moves 4321 BRL on wallet 8 but transaction 9 is 8765 BRL on wallet 8", []string{"4321", "8765"}},
		{"WL002", "wallets_guard", "wallet 7: a balance change must move the version from 31 to 32, not to 39", []string{"31", "32", "39"}},
	}
	for _, tc := range tests {
		t.Run(tc.code+" "+tc.constraint, func(t *testing.T) {
			t.Parallel()
			pgErr := &pgconn.PgError{Severity: "ERROR", Code: tc.code, ConstraintName: tc.constraint, Message: tc.message}

			got := classify(fmt.Errorf("commit: %w", pgErr))

			if !errors.Is(got, app.ErrPermanent) {
				t.Errorf("classify() = %v, want it to wrap %v", got, app.ErrPermanent)
			}
			var inChain *pgconn.PgError
			if !errors.As(got, &inChain) || inChain != pgErr {
				t.Errorf("classify() = %v, lost the server error", got)
			}
			text := got.Error()
			for _, amount := range tc.amounts {
				if strings.Contains(text, amount) {
					t.Errorf("classify() = %q, repeats %s from the server's message", text, amount)
				}
			}
			if !strings.Contains(text, tc.code) || !strings.Contains(text, tc.constraint) {
				t.Errorf("classify() = %q, want it to name %s and %s", text, tc.code, tc.constraint)
			}
		})
	}
}

func TestClassify_KeepsTheServerError(t *testing.T) {
	t.Parallel()

	got := classify(fmt.Errorf("insert wallet: %w", pgError("23505", "ux_wallets_player_currency")))
	var pgErr *pgconn.PgError
	if !errors.As(got, &pgErr) || pgErr.ConstraintName != "ux_wallets_player_currency" {
		t.Fatalf("classify() = %v, want the *pgconn.PgError still reachable", got)
	}
}

func TestClassify_Nil(t *testing.T) {
	t.Parallel()

	if got := classify(nil); got != nil {
		t.Fatalf("classify(nil) = %v, want nil", got)
	}
}

func TestClassify_LeavesClassifiedErrorsAlone(t *testing.T) {
	t.Parallel()

	for _, s := range sentinels {
		in := fmt.Errorf("%w: %w", s, pgError("40001", ""))
		got := classify(in)
		if got.Error() != in.Error() {
			t.Errorf("classify(%q) = %q, want it unchanged", in, got)
		}
		for _, other := range sentinels {
			if !errors.Is(other, s) && errors.Is(got, other) {
				t.Errorf("classify(%q) = %q, also wraps %v", in, got, other)
			}
		}
	}
}

func TestMilliseconds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		d    time.Duration
		want string
	}{
		{2 * time.Second, "2000"},
		{1500 * time.Millisecond, "1500"},
		{time.Millisecond, "1"},
		{time.Nanosecond, "1"},
		{time.Millisecond + time.Nanosecond, "2"},
		{0, "0"},
	}
	for _, tc := range tests {
		if got := milliseconds(tc.d); got != tc.want {
			t.Errorf("milliseconds(%s) = %q, want %q", tc.d, got, tc.want)
		}
	}
}
