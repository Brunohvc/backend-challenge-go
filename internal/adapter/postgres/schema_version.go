package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrSchemaMismatch marks a database whose schema is not the one this build embeds.
var ErrSchemaMismatch = errors.New("postgres: the database schema does not match this build")

// CheckSchema verifies that the database is at the last migration this build embeds.
func CheckSchema(ctx context.Context, pool *pgxpool.Pool) error {
	want, err := latestMigration()
	if err != nil {
		return err
	}
	var (
		version uint
		dirty   bool
	)
	err = pool.QueryRow(ctx, "SELECT version, dirty FROM public.schema_version()").Scan(&version, &dirty)
	var pgErr *pgconn.PgError
	switch {
	case errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UndefinedFunction:
		return fmt.Errorf("%w: the database is not migrated to version %d", ErrSchemaMismatch, want)
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("%w: no migration is recorded, this build expects version %d", ErrSchemaMismatch, want)
	case err != nil:
		return fmt.Errorf("postgres: read the schema version: %w", classify(err))
	case dirty:
		return fmt.Errorf("%w: version %d is dirty, a migration failed halfway; "+
			"repair the database and force the version it is at", ErrSchemaMismatch, version)
	case version != want:
		return fmt.Errorf("%w: the database is at version %d, this build expects %d", ErrSchemaMismatch, version, want)
	}
	return nil
}
