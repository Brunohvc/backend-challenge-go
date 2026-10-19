// Command migrate applies and reverts the versioned migrations of the wallet database.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
)

const (
	exitOK      = 0
	exitFailure = 1
	exitUsage   = 2
)

const timeout = 5 * time.Minute

const usage = `usage: migrate up | down N | version | force V
  MIGRATIONS_DATABASE_URL must be set to a DSN of the schema owner
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	cancel()
	stop()
	os.Exit(code)
}

type command struct {
	direction string
	steps     int
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	cmd, err := parse(args)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "migrate: %v\n%s", err, usage)
		return exitUsage
	}
	dsn := getenv("MIGRATIONS_DATABASE_URL")
	if dsn == "" {
		_, _ = fmt.Fprintf(stderr, "migrate: MIGRATIONS_DATABASE_URL is not set\n%s", usage)
		return exitUsage
	}

	if cmd.direction != "" {
		if err := postgres.Migrate(ctx, dsn, cmd.direction, cmd.steps); err != nil {
			_, _ = fmt.Fprintf(stderr, "migrate: %v\n", err)
			if errors.Is(err, postgres.ErrInvalidMigrationArgs) {
				return exitUsage
			}
			return exitFailure
		}
	}
	version, dirty, err := postgres.MigrationVersion(ctx, dsn)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "migrate: %v\n", err)
		return exitFailure
	}
	if _, err := fmt.Fprintln(stdout, describe(version, dirty)); err != nil {
		_, _ = fmt.Fprintf(stderr, "migrate: print the version: %v\n", err)
		return exitFailure
	}
	return exitOK
}

func parse(args []string) (command, error) {
	if len(args) == 0 {
		return command{}, errors.New("no command")
	}
	name, rest := args[0], args[1:]
	switch name {
	case "up", "version":
		if len(rest) != 0 {
			return command{}, fmt.Errorf("%s takes no argument", name)
		}
		if name == "version" {
			return command{}, nil
		}
		return command{direction: postgres.DirectionUp}, nil
	case "down", "force":
		if len(rest) != 1 {
			return command{}, fmt.Errorf("%s takes exactly one number", name)
		}
		n, err := strconv.Atoi(rest[0])
		if err != nil {
			return command{}, fmt.Errorf("%s: %q is not a number", name, rest[0])
		}
		if name == "down" {
			if n < 1 {
				return command{}, fmt.Errorf("down: the number of migrations to revert must be at least 1, got %d", n)
			}
			return command{direction: postgres.DirectionDown, steps: n}, nil
		}
		if n < -1 {
			return command{}, fmt.Errorf("force: the version must be -1 (none) or more, got %d", n)
		}
		return command{direction: postgres.DirectionForce, steps: n}, nil
	}
	return command{}, fmt.Errorf("unknown command %q", name)
}

func describe(version uint, dirty bool) string {
	switch {
	case version == 0 && !dirty:
		return "version none"
	case dirty:
		return fmt.Sprintf("version %d (dirty)", version)
	}
	return fmt.Sprintf("version %d", version)
}
