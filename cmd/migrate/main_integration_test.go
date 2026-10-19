//go:build integration

package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var pg *testenv.Postgres

func TestMain(m *testing.M) {
	os.Exit(testenv.RunWithPostgres(m, func(p *testenv.Postgres) { pg = p }))
}

func migrate(t *testing.T, dsn string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	env := func(key string) string {
		if key == "MIGRATIONS_DATABASE_URL" {
			return dsn
		}
		return ""
	}
	code = run(t.Context(), args, env, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestRun_AgainstDatabase(t *testing.T) {
	for _, step := range []struct {
		args []string
		want string
	}{
		{[]string{"version"}, "version 10\n"},
		{[]string{"down", "2"}, "version 8\n"},
		{[]string{"up"}, "version 10\n"},
		{[]string{"up"}, "version 10\n"},
		{[]string{"force", "10"}, "version 10\n"},
	} {
		code, stdout, stderr := migrate(t, pg.OwnerDSN, step.args...)
		if code != exitOK || stdout != step.want {
			t.Fatalf("migrate %s = %d, %q (stderr %q); want %d, %q", strings.Join(step.args, " "), code, stdout, stderr, exitOK, step.want)
		}
	}
}

func TestRun_DatabaseFailure(t *testing.T) {
	code, stdout, stderr := migrate(t, "postgres://wallet_owner:wrong@127.0.0.1:1/wallet?connect_timeout=2", "up")
	if code != exitFailure {
		t.Fatalf("exit code = %d, want %d", code, exitFailure)
	}
	if stdout != "" || !strings.HasPrefix(stderr, "migrate: ") {
		t.Fatalf("stdout %q, stderr %q; want only an error on stderr", stdout, stderr)
	}
	if strings.Contains(stderr, "wrong") {
		t.Fatalf("stderr leaks the password: %q", stderr)
	}
}
