//go:build integration

package testenv_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

func TestPostgres_PauseAndUnpause(t *testing.T) {
	ctx := t.Context()
	pg, err := testenv.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("StartPostgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.WithoutCancel(ctx)) })
	before, err := pgx.Connect(ctx, pg.AppDSN)
	if err != nil {
		t.Fatalf("connect before the pause: %v", err)
	}
	t.Cleanup(func() { _ = before.Close(context.WithoutCancel(ctx)) })

	if err := pg.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	paused := true
	t.Cleanup(func() {
		if paused {
			_ = pg.Unpause(context.WithoutCancel(ctx))
		}
	})
	const deadline = time.Second
	connectCtx, cancel := context.WithTimeout(ctx, deadline)
	started := time.Now()
	conn, err := pgx.Connect(connectCtx, pg.AppDSN)
	elapsed := time.Since(started)
	cancel()
	if err == nil {
		_ = conn.Close(ctx)
		t.Fatal("connected to a paused server, want the attempt to hang until its deadline")
	}
	if elapsed < deadline {
		t.Fatalf("the attempt failed after %s with %v, before its %s deadline: the server refused it instead of hanging", elapsed, err, deadline)
	}

	if err := pg.Unpause(ctx); err != nil {
		t.Fatalf("Unpause: %v", err)
	}
	paused = false
	if err := before.Ping(ctx); err != nil {
		t.Fatalf("ping on the connection opened before the pause: %v", err)
	}
	after, err := pgx.Connect(ctx, pg.AppDSN)
	if err != nil {
		t.Fatalf("connect after the pause: %v", err)
	}
	_ = after.Close(ctx)
}
