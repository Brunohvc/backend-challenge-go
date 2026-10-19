//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var pg *testenv.Postgres

func TestMain(m *testing.M) {
	os.Exit(testenv.RunWithPostgres(m, func(p *testenv.Postgres) { pg = p }))
}

var t0 = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

func newID() uuid.UUID { return idgen.V7{}.New() }

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func resetDB(t *testing.T) {
	t.Helper()
	if err := pg.Reset(t.Context()); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

func begin(t *testing.T, conn *pgx.Conn) pgx.Tx {
	t.Helper()
	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	return tx
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type row map[string]any

func get[T any](r row, c string) T {
	v, _ := r[c].(T)
	return v
}

func (r row) set(col string, v any) row {
	c := maps.Clone(r)
	c[col] = v
	return c
}

func insert(ctx context.Context, db execer, table string, r row) error {
	cols := slices.Sorted(maps.Keys(r))
	params := make([]string, len(cols))
	args := make([]any, len(cols))
	for i, c := range cols {
		params[i] = fmt.Sprintf("$%d", i+1)
		args[i] = r[c]
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.Join(params, ", "))
	_, err := db.Exec(ctx, sql, args...)
	return err
}

func mustInsert(t *testing.T, db execer, table string, r row) {
	t.Helper()
	if err := insert(t.Context(), db, table, r); err != nil {
		t.Fatalf("insert into %s: %v", table, err)
	}
}

func mustExec(t *testing.T, db execer, sql string, args ...any) {
	t.Helper()
	if _, err := db.Exec(t.Context(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func mustCommit(t *testing.T, tx pgx.Tx) {
	t.Helper()
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func requireSQLState(t *testing.T, err error, code, constraint string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("error = %v, want SQLSTATE %s", err, code)
	}
	if pgErr.Code != code {
		t.Fatalf("SQLSTATE = %s (%s), want %s", pgErr.Code, pgErr.Message, code)
	}
	if constraint != "" && pgErr.ConstraintName != constraint {
		t.Fatalf("constraint = %q (%s), want %q", pgErr.ConstraintName, pgErr.Message, constraint)
	}
}

func queryInt64(t *testing.T, db execer, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func walletRow(balance int64) row {
	return row{
		"id":            newID(),
		"player_id":     newID(),
		"currency":      "BRL",
		"balance_minor": balance,
		"version":       int64(1),
		"created_at":    t0,
		"updated_at":    t0,
	}
}

func openingRow(w row) row {
	return row{
		"id":                    newID(),
		"origin":                "INTERNAL",
		"kind":                  "OPENING",
		"status":                "PROCESSED",
		"wallet_id":             w["id"],
		"player_id":             w["player_id"],
		"amount_minor":          w["balance_minor"],
		"currency":              w["currency"],
		"result_balance_minor":  w["balance_minor"],
		"result_wallet_version": int64(1),
		"correlation_id":        "corr-opening",
		"created_at":            t0,
		"updated_at":            t0,
		"processed_at":          t0,
	}
}

func externalRow(w row, kind string, amount int64) row {
	ext := "ext-" + newID().String()
	return row{
		"id":                      newID(),
		"origin":                  "EXTERNAL",
		"kind":                    kind,
		"status":                  "PROCESSED",
		"wallet_id":               w["id"],
		"player_id":               w["player_id"],
		"amount_minor":            amount,
		"currency":                w["currency"],
		"provider_id":             "provider-a",
		"external_transaction_id": ext,
		"idempotency_key":         "key-" + ext,
		"payload_hash":            strings.Repeat("0a", 32),
		"round_id":                "round-1",
		"game_id":                 "game-1",
		"result_balance_minor":    int64(0),
		"result_wallet_version":   int64(1),
		"correlation_id":          "corr-" + ext,
		"created_at":              t0,
		"updated_at":              t0,
		"processed_at":            t0,
	}
}

func reversalRow(w, target row, kind string) row {
	return externalRow(w, kind, get[int64](target, "amount_minor")).
		set("reference_external_transaction_id", target["external_transaction_id"]).
		set("reference_transaction_id", target["id"])
}

func pendingReferenceRow(w row) row {
	return externalRow(w, "WIN", 500).
		set("status", "PENDING_REFERENCE").
		set("reference_external_transaction_id", "bet-not-here-yet").
		set("result_balance_minor", nil).
		set("result_wallet_version", nil).
		set("processed_at", nil).
		set("next_attempt_at", t0.Add(time.Second)).
		set("expires_at", t0.Add(time.Hour))
}

func ledgerRow(tx row, dir string, before int64) row {
	amount := get[int64](tx, "amount_minor")
	after := before + amount
	if dir == "DEBIT" {
		after = before - amount
	}
	return row{
		"id":                   newID(),
		"wallet_id":            tx["wallet_id"],
		"transaction_id":       tx["id"],
		"direction":            dir,
		"amount_minor":         amount,
		"currency":             tx["currency"],
		"balance_before_minor": before,
		"balance_after_minor":  after,
		"created_at":           t0,
	}
}

func seedWallet(t *testing.T, conn *pgx.Conn, balance int64) row {
	t.Helper()
	w := walletRow(balance)
	tx := begin(t, conn)
	mustInsert(t, tx, "wallets", w)
	if balance > 0 {
		opening := openingRow(w)
		mustInsert(t, tx, "wager_transactions", opening)
		mustInsert(t, tx, "wallet_ledger_entries", ledgerRow(opening, "CREDIT", 0))
	}
	mustCommit(t, tx)
	return w
}

func move(t *testing.T, conn *pgx.Conn, w row, kind, dir string, amount int64) row {
	t.Helper()
	tx := begin(t, conn)
	movement := externalRow(w, kind, amount)
	entry := ledgerRow(movement, dir, get[int64](w, "balance_minor"))
	mustInsert(t, tx, "wager_transactions", movement)
	mustInsert(t, tx, "wallet_ledger_entries", entry)
	next := w.set("balance_minor", entry["balance_after_minor"]).set("version", get[int64](w, "version")+1)
	mustExec(t, tx, "UPDATE wallets SET balance_minor = $2, version = $3 WHERE id = $1",
		w["id"], next["balance_minor"], next["version"])
	mustCommit(t, tx)
	return next
}

func outboxRow(w row) row {
	return row{
		"id":              newID(),
		"aggregate_type":  "wallet",
		"aggregate_id":    get[uuid.UUID](w, "id").String(),
		"event_type":      "WalletDebited",
		"event_version":   1,
		"payload":         `{"eventId":"e-1","amount":{"amount":"1.00","currency":"BRL"}}`,
		"correlation_id":  "corr-outbox",
		"occurred_at":     t0,
		"next_attempt_at": t0,
		"created_at":      t0,
	}
}
