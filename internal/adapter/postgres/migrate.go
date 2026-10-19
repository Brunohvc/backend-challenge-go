// Package postgres is the PostgreSQL adapter of the wallet service.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Brunohvc/backend-challenge-go/migrations"
)

// The directions Migrate takes.
const (
	DirectionUp    = "up"
	DirectionDown  = "down"
	DirectionForce = "force"
)

// ErrInvalidMigrationArgs is returned for a direction or a number of steps Migrate cannot take.
var ErrInvalidMigrationArgs = errors.New("postgres: invalid migration arguments")

// Migrate moves the schema of the database at dsn to another migration version.
func Migrate(ctx context.Context, dsn string, direction string, steps int) error {
	apply, err := plan(direction, steps)
	if err != nil {
		return err
	}
	err = withMigrator(ctx, dsn, func(m *migrate.Migrate) error {
		if err := apply(m); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("postgres: migrate %s: %w", direction, err)
	}
	return nil
}

// MigrationVersion returns the version of the database at dsn and whether it is dirty.
func MigrationVersion(ctx context.Context, dsn string) (version uint, dirty bool, err error) {
	err = withMigrator(ctx, dsn, func(m *migrate.Migrate) error {
		v, d, err := m.Version()
		if errors.Is(err, migrate.ErrNilVersion) {
			return nil
		}
		version, dirty = v, d
		return err
	})
	if err != nil {
		return 0, false, fmt.Errorf("postgres: read migration version: %w", err)
	}
	return version, dirty, nil
}

func latestMigration() (uint, error) {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return 0, fmt.Errorf("postgres: read the embedded migrations: %w", err)
	}
	defer func() { _ = src.Close() }()

	version, err := src.First()
	if err != nil {
		return 0, fmt.Errorf("postgres: read the embedded migrations: %w", err)
	}
	for {
		next, err := src.Next(version)
		if errors.Is(err, fs.ErrNotExist) {
			return version, nil
		}
		if err != nil {
			return 0, fmt.Errorf("postgres: read the embedded migrations: %w", err)
		}
		version = next
	}
}

func plan(direction string, steps int) (func(*migrate.Migrate) error, error) {
	switch direction {
	case DirectionUp:
		switch {
		case steps < 0:
			return nil, fmt.Errorf("%w: up takes 0 (all) or a positive number of steps, got %d", ErrInvalidMigrationArgs, steps)
		case steps == 0:
			return (*migrate.Migrate).Up, nil
		}
		return func(m *migrate.Migrate) error { return m.Steps(steps) }, nil
	case DirectionDown:
		if steps < 1 {
			return nil, fmt.Errorf("%w: down takes a positive number of steps, got %d", ErrInvalidMigrationArgs, steps)
		}
		return func(m *migrate.Migrate) error { return m.Steps(-steps) }, nil
	case DirectionForce:
		if steps < -1 {
			return nil, fmt.Errorf("%w: force takes a version, or -1 for none, got %d", ErrInvalidMigrationArgs, steps)
		}
		return func(m *migrate.Migrate) error { return m.Force(steps) }, nil
	}
	return nil, fmt.Errorf("%w: unknown direction %q", ErrInvalidMigrationArgs, direction)
}

func withMigrator(ctx context.Context, dsn string, fn func(*migrate.Migrate) error) (err error) {
	db, err := open(ctx, dsn)
	if err != nil {
		return err
	}
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("prepare the migrations table: %w", err)
	}
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		_ = driver.Close()
		return fmt.Errorf("read the embedded migrations: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		_ = src.Close()
		_ = driver.Close()
		return fmt.Errorf("create the migrator: %w", err)
	}
	defer func() {
		srcErr, dbErr := m.Close()
		if closeErr := errors.Join(srcErr, dbErr); closeErr != nil && err == nil {
			err = fmt.Errorf("close the migrator: %w", closeErr)
		}
	}()

	stop := context.AfterFunc(ctx, func() { m.GracefulStop <- true })
	err = fn(m)
	if !stop() {
		return errors.Join(err, context.Cause(ctx))
	}
	return err
}

func open(ctx context.Context, dsn string) (*sql.DB, error) {
	if rest, ok := strings.CutPrefix(dsn, "pgx5://"); ok {
		dsn = "postgres://" + rest
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse the database url: %w", err)
	}
	db := stdlib.OpenDB(*cfg)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}
	return db, nil
}
