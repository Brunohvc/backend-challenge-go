//go:build integration

package postgres_test

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
)

const schemaVersionSince = 8

func appPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func asApp(t *testing.T, ownerDSN string) string {
	t.Helper()
	u, err := url.Parse(ownerDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	return withDatabase(t, pg.AppDSN, strings.TrimPrefix(u.Path, "/"))
}

func TestCheckSchema_CurrentSchemaPasses(t *testing.T) {
	if err := postgres.CheckSchema(t.Context(), appPool(t, pg.AppDSN)); err != nil {
		t.Fatalf("CheckSchema() error = %v", err)
	}
}

func TestCheckSchema_RefusesAnotherSchema(t *testing.T) {
	up := func(t *testing.T, dsn string) {
		t.Helper()
		if err := postgres.Migrate(t.Context(), dsn, postgres.DirectionUp, 0); err != nil {
			t.Fatalf("up: %v", err)
		}
	}
	force := func(t *testing.T, dsn string, version int) {
		t.Helper()
		if err := postgres.Migrate(t.Context(), dsn, postgres.DirectionForce, version); err != nil {
			t.Fatalf("force %d: %v", version, err)
		}
	}
	tests := []struct {
		name    string
		prepare func(t *testing.T, ownerDSN string)
		want    string
	}{
		{
			name:    "never migrated",
			prepare: func(*testing.T, string) {},
			want:    fmt.Sprintf("not migrated to version %d", latestVersion),
		},
		{
			name: "migrated short of the version that records it",
			prepare: func(t *testing.T, dsn string) {
				if err := postgres.Migrate(t.Context(), dsn, postgres.DirectionUp, schemaVersionSince-1); err != nil {
					t.Fatalf("up %d: %v", schemaVersionSince-1, err)
				}
			},
			want: fmt.Sprintf("not migrated to version %d", latestVersion),
		},
		{
			name:    "at an older version",
			prepare: func(t *testing.T, dsn string) { up(t, dsn); force(t, dsn, latestVersion-1) },
			want:    fmt.Sprintf("at version %d, this build expects %d", latestVersion-1, latestVersion),
		},
		{
			name:    "at a newer version",
			prepare: func(t *testing.T, dsn string) { up(t, dsn); force(t, dsn, latestVersion+1) },
			want:    fmt.Sprintf("at version %d, this build expects %d", latestVersion+1, latestVersion),
		},
		{
			name: "dirty",
			prepare: func(t *testing.T, dsn string) {
				up(t, dsn)
				mustExec(t, connect(t, dsn), "UPDATE schema_migrations SET dirty = true")
			},
			want: fmt.Sprintf("version %d is dirty", latestVersion),
		},
		{
			name:    "no version recorded",
			prepare: func(t *testing.T, dsn string) { up(t, dsn); force(t, dsn, -1) },
			want:    "no migration is recorded",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dsn := scratchDatabase(t)
			tc.prepare(t, dsn)

			err := postgres.CheckSchema(t.Context(), appPool(t, asApp(t, dsn)))

			if !errors.Is(err, postgres.ErrSchemaMismatch) {
				t.Fatalf("CheckSchema() error = %v, want postgres.ErrSchemaMismatch", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckSchema() error = %q, want it to say %q", err, tc.want)
			}
		})
	}
}
