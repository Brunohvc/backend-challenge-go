//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestSchema_OnlyServiceRolesConnect(t *testing.T) {
	super := connect(t, pg.SuperDSN)
	mustExec(t, super, `CREATE ROLE stranger LOGIN PASSWORD 'stranger'`)
	t.Cleanup(func() { _, _ = super.Exec(context.Background(), `DROP ROLE stranger`) })
	u, err := url.Parse(pg.AppDSN)
	if err != nil {
		t.Fatalf("parse AppDSN: %v", err)
	}
	u.User = url.UserPassword("stranger", "stranger")

	conn, err := pgx.Connect(t.Context(), u.String())
	if err == nil {
		_ = conn.Close(context.Background())
		t.Fatal("a role without CONNECT reached the wallet database")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != insufficientPrivilege {
		t.Errorf("err = %v, want SQLSTATE %s", err, insufficientPrivilege)
	}

	for _, dsn := range []string{pg.OwnerDSN, pg.AppDSN} {
		conn := connect(t, dsn)
		if err := conn.Ping(t.Context()); err != nil {
			t.Errorf("service role cannot connect: %v", err)
		}
	}
}
