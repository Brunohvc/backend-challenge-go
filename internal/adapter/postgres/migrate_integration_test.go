//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
)

const latestVersion = 10

func scratchDatabase(t *testing.T) string {
	t.Helper()
	ctx := t.Context()
	name := "scratch_" + strings.ReplaceAll(newID().String(), "-", "")

	super := connect(t, pg.SuperDSN)
	mustExec(t, super, fmt.Sprintf("CREATE DATABASE %s OWNER wallet_owner", name))
	t.Cleanup(func() {
		_, _ = super.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", name))
	})

	scratchSuper := connect(t, withDatabase(t, pg.SuperDSN, name))
	for _, stmt := range []string{
		"ALTER SCHEMA public OWNER TO wallet_owner",
		"REVOKE ALL ON SCHEMA public FROM PUBLIC",
		"GRANT USAGE ON SCHEMA public TO wallet_app",
	} {
		if _, err := scratchSuper.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	return withDatabase(t, pg.OwnerDSN, name)
}

func withDatabase(t *testing.T, dsn, name string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

func requireVersion(t *testing.T, dsn string, want uint) {
	t.Helper()
	got, dirty, err := postgres.MigrationVersion(t.Context(), dsn)
	if err != nil {
		t.Fatalf("MigrationVersion: %v", err)
	}
	if got != want || dirty {
		t.Fatalf("version = %d dirty = %v, want %d clean", got, dirty, want)
	}
}

func schemaObjects(t *testing.T, dsn string) (tables, functions, triggers int64) {
	t.Helper()
	conn := connect(t, dsn)
	tables = queryInt64(t, conn, `SELECT count(*) FROM pg_tables
		WHERE schemaname = 'public' AND tablename <> 'schema_migrations'`)
	functions = queryInt64(t, conn, `SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'`)
	triggers = queryInt64(t, conn, `SELECT count(*) FROM pg_trigger tg JOIN pg_class c ON c.oid = tg.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND NOT tg.tgisinternal`)
	return tables, functions, triggers
}

func TestMigrationVersion_EmptyDatabase(t *testing.T) {
	requireVersion(t, scratchDatabase(t), 0)
}

func TestMigrations_UpDownUp(t *testing.T) {
	ctx := t.Context()
	dsn := scratchDatabase(t)

	if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 0); err != nil {
		t.Fatalf("up: %v", err)
	}
	requireVersion(t, dsn, latestVersion)
	if tables, _, _ := schemaObjects(t, dsn); tables != 5 {
		t.Fatalf("after up: %d tables, want 5", tables)
	}

	if err := postgres.Migrate(ctx, dsn, postgres.DirectionDown, latestVersion); err != nil {
		t.Fatalf("down %d: %v", latestVersion, err)
	}
	requireVersion(t, dsn, 0)
	if tables, functions, triggers := schemaObjects(t, dsn); tables+functions+triggers != 0 {
		t.Fatalf("after down: %d tables, %d functions, %d triggers left, want none", tables, functions, triggers)
	}

	if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 0); err != nil {
		t.Fatalf("up again: %v", err)
	}
	requireVersion(t, dsn, latestVersion)
}

func TestMigrations_UpIsIdempotent(t *testing.T) {
	ctx := t.Context()
	dsn := scratchDatabase(t)
	for range 2 {
		if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 0); err != nil {
			t.Fatalf("up: %v", err)
		}
	}
	requireVersion(t, dsn, latestVersion)
}

func TestMigrations_EachStepReverts(t *testing.T) {
	ctx := t.Context()
	dsn := scratchDatabase(t)
	if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 0); err != nil {
		t.Fatalf("up: %v", err)
	}

	type state struct {
		tables   int64
		granted  bool
		guarded  bool
		dueIndex bool
	}
	want := map[uint]state{
		10: {tables: 5, granted: true, guarded: true, dueIndex: false},
		9:  {tables: 5, granted: true, guarded: true, dueIndex: true},
		8:  {tables: 5, granted: true, guarded: true, dueIndex: true},
		7:  {tables: 5, granted: true, guarded: true, dueIndex: true},
		6:  {tables: 5, granted: true, guarded: true, dueIndex: true},
		5:  {tables: 5, granted: false, guarded: true, dueIndex: true},
		4:  {tables: 5, granted: false, guarded: false, dueIndex: true},
		3:  {tables: 3, granted: false, guarded: false, dueIndex: false},
		2:  {tables: 2, granted: false, guarded: false, dueIndex: false},
		1:  {tables: 1, granted: false, guarded: false, dueIndex: false},
		0:  {tables: 0, granted: false, guarded: false, dueIndex: false},
	}
	requireState := func(v uint) {
		t.Helper()
		requireVersion(t, dsn, v)
		tables, functions, triggers := schemaObjects(t, dsn)
		conn := connect(t, dsn)
		granted := queryInt64(t, conn, `SELECT count(*) FROM information_schema.role_table_grants
			WHERE grantee = 'wallet_app' AND table_schema = 'public'`) > 0
		dueIndex := queryInt64(t, conn, `SELECT count(*) FROM pg_indexes
			WHERE schemaname = 'public' AND indexname = 'ix_outbox_due'`) == 1
		got := state{tables: tables, granted: granted, guarded: functions > 0 && triggers > 0, dueIndex: dueIndex}
		if got != want[v] {
			t.Fatalf("at version %d: %+v, want %+v", v, got, want[v])
		}
	}

	requireState(latestVersion)
	for v := uint(latestVersion - 1); ; v-- {
		if err := postgres.Migrate(ctx, dsn, postgres.DirectionDown, 1); err != nil {
			t.Fatalf("down to %d: %v", v, err)
		}
		requireState(v)
		if v == 0 {
			break
		}
	}

	if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 0); err != nil {
		t.Fatalf("up again: %v", err)
	}
	requireVersion(t, dsn, latestVersion)
}

func TestMigrations_UpBySteps(t *testing.T) {
	ctx := t.Context()
	dsn := scratchDatabase(t)
	if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 2); err != nil {
		t.Fatalf("up 2: %v", err)
	}
	requireVersion(t, dsn, 2)
	if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 0); err != nil {
		t.Fatalf("up: %v", err)
	}
	requireVersion(t, dsn, latestVersion)
}

func TestMigrations_ForceClearsDirty(t *testing.T) {
	ctx := t.Context()
	dsn := scratchDatabase(t)
	if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 0); err != nil {
		t.Fatalf("up: %v", err)
	}
	mustExec(t, connect(t, dsn), "UPDATE schema_migrations SET dirty = true")

	version, dirty, err := postgres.MigrationVersion(ctx, dsn)
	if err != nil || version != latestVersion || !dirty {
		t.Fatalf("MigrationVersion = %d, %v, %v; want %d, dirty", version, dirty, err, latestVersion)
	}
	if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 0); err == nil || !strings.Contains(err.Error(), "Dirty") {
		t.Fatalf("up on a dirty database: error = %v, want the dirty version reported", err)
	}

	if err := postgres.Migrate(ctx, dsn, postgres.DirectionForce, latestVersion); err != nil {
		t.Fatalf("force: %v", err)
	}
	requireVersion(t, dsn, latestVersion)
}

func TestMigrate_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := postgres.Migrate(ctx, pg.OwnerDSN, postgres.DirectionUp, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestMigrate_AcceptsEveryDSNSpelling(t *testing.T) {
	u, err := url.Parse(pg.OwnerDSN)
	if err != nil {
		t.Fatal(err)
	}
	for _, scheme := range []string{"postgres", "postgresql", "pgx5"} {
		u.Scheme = scheme
		if _, _, err := postgres.MigrationVersion(t.Context(), u.String()); err != nil {
			t.Errorf("%s://: %v", scheme, err)
		}
	}
}

func TestMigrate_ErrorsDoNotLeakThePassword(t *testing.T) {
	u, err := url.Parse(pg.OwnerDSN)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("wallet_owner", "not-the-password-s3cr3t")
	wrongPassword := u.String()
	malformed := strings.Replace(wrongPassword, u.Host, u.Hostname()+":not-a-port", 1)

	for name, dsn := range map[string]string{"wrong password": wrongPassword, "malformed": malformed} {
		err := postgres.Migrate(t.Context(), dsn, postgres.DirectionUp, 0)
		if err == nil {
			t.Fatalf("%s: accepted", name)
		}
		if strings.Contains(err.Error(), "s3cr3t") {
			t.Fatalf("%s: error leaks the password: %v", name, err)
		}
	}
}

func TestReset_EmptiesEveryTableAndRestartsIdentities(t *testing.T) {
	ctx := t.Context()
	resetDB(t)
	conn := connect(t, pg.OwnerDSN)
	w := seedWallet(t, conn, 1000)
	move(t, conn, w, "BET", "DEBIT", 100)
	mustInsert(t, conn, "outbox_events", outboxRow(w))

	if err := pg.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	for _, table := range []string{"wallets", "wager_transactions", "wallet_ledger_entries", "outbox_events", "inbox_messages"} {
		if n := queryInt64(t, conn, "SELECT count(*) FROM "+table); n != 0 {
			t.Errorf("%s has %d rows after Reset", table, n)
		}
	}
	requireVersion(t, pg.OwnerDSN, latestVersion)

	w = seedWallet(t, conn, 1000)
	if seq := queryInt64(t, conn, "SELECT seq FROM wallet_ledger_entries WHERE wallet_id = $1", w["id"]); seq != 1 {
		t.Fatalf("first ledger seq after Reset = %d, want 1", seq)
	}
}
