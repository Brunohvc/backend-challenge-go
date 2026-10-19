//go:build integration || e2e

// Package testenv starts the containers the integration and e2e tests need.
package testenv

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
)

const (
	postgresImage = "postgres:18.6-alpine"
	postgresPort  = "5432/tcp"
	databaseName  = "wallet"

	superuser = "postgres"
	ownerRole = "wallet_owner"
	appRole   = "wallet_app"

	startTimeout = 3 * time.Minute
)

// Postgres is a PostgreSQL server in a container, bootstrapped like the compose service.
type Postgres struct {
	SuperDSN string
	OwnerDSN string
	AppDSN   string

	Container testcontainers.Container
}

// StartPostgres starts the container and applies every migration as wallet_owner.
func StartPostgres(ctx context.Context) (*Postgres, error) {
	files, err := initdbFiles()
	if err != nil {
		return nil, err
	}

	ctr, err := testcontainers.Run(ctx, postgresImage,
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_USER":     superuser,
			"POSTGRES_PASSWORD": superuser,
			"POSTGRES_DB":       databaseName,
		}),
		testcontainers.WithExposedPorts(postgresPort),
		testcontainers.WithFiles(files...),
		testcontainers.WithWaitStrategyAndDeadline(startTimeout,
			wait.ForExec([]string{"pg_isready", "-h", "127.0.0.1", "-U", superuser, "-d", databaseName}).
				WithStartupTimeout(startTimeout),
			wait.ForListeningPort(postgresPort).WithStartupTimeout(startTimeout),
		),
	)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("testenv: start postgres: %w", err), testcontainers.TerminateContainer(ctr))
	}

	endpoint, err := ctr.PortEndpoint(ctx, postgresPort, "")
	if err != nil {
		return nil, errors.Join(fmt.Errorf("testenv: postgres endpoint: %w", err), testcontainers.TerminateContainer(ctr))
	}
	p := &Postgres{
		SuperDSN:  dsn(superuser, endpoint),
		OwnerDSN:  dsn(ownerRole, endpoint),
		AppDSN:    dsn(appRole, endpoint),
		Container: ctr,
	}
	if err := postgres.Migrate(ctx, p.OwnerDSN, postgres.DirectionUp, 0); err != nil {
		return nil, errors.Join(fmt.Errorf("testenv: migrate: %w", err), testcontainers.TerminateContainer(ctr))
	}
	return p, nil
}

// Terminate stops and removes the container.
func (p *Postgres) Terminate(ctx context.Context) error {
	if err := p.Container.Terminate(ctx); err != nil {
		return fmt.Errorf("testenv: terminate postgres: %w", err)
	}
	return nil
}

// Pause freezes every process of the server until Unpause.
func (p *Postgres) Pause(ctx context.Context) error {
	return pause(ctx, "postgres", p.Container)
}

// Unpause lets a server that Pause froze run again.
func (p *Postgres) Unpause(ctx context.Context) error {
	return unpause(ctx, "postgres", p.Container)
}

func pause(ctx context.Context, name string, c testcontainers.Container) error {
	return withDocker(ctx, "pause "+name, c, func(cli *testcontainers.DockerClient, id string) error {
		_, err := cli.ContainerPause(ctx, id, client.ContainerPauseOptions{})
		return err
	})
}

func unpause(ctx context.Context, name string, c testcontainers.Container) error {
	return withDocker(ctx, "unpause "+name, c, func(cli *testcontainers.DockerClient, id string) error {
		_, err := cli.ContainerUnpause(ctx, id, client.ContainerUnpauseOptions{})
		return err
	})
}

func withDocker(ctx context.Context, what string, c testcontainers.Container, fn func(cli *testcontainers.DockerClient, id string) error) error {
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return fmt.Errorf("testenv: %s: docker client: %w", what, err)
	}
	defer func() { _ = cli.Close() }()
	if err := fn(cli, c.GetContainerID()); err != nil {
		return fmt.Errorf("testenv: %s: %w", what, err)
	}
	return nil
}

// Reset empties every table of the schema and restarts the identity columns.
func (p *Postgres) Reset(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, p.SuperDSN)
	if err != nil {
		return fmt.Errorf("testenv: reset: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DO $$
DECLARE
    tables text;
BEGIN
    SELECT string_agg(format('%I.%I', schemaname, tablename), ', ')
      INTO tables
      FROM pg_tables
     WHERE schemaname = 'public' AND tablename <> 'schema_migrations';
    IF tables IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE ' || tables || ' RESTART IDENTITY';
    END IF;
END $$`)
		return err
	})
	if err != nil {
		return fmt.Errorf("testenv: reset: %w", err)
	}
	return nil
}

// RunWithPostgres runs a test binary against one PostgreSQL and returns the exit code.
func RunWithPostgres(m *testing.M, use func(*Postgres)) int {
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	p, err := StartPostgres(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() {
		if err := p.Terminate(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()

	use(p)
	return m.Run()
}

func dsn(role, endpoint string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(role, role),
		Host:     endpoint,
		Path:     "/" + databaseName,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

func initdbFiles() ([]testcontainers.ContainerFile, error) {
	root, err := repoRoot()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(root, "deploy", "postgres", "initdb")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("testenv: read initdb scripts: %w", err)
	}
	var files []testcontainers.ContainerFile
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		files = append(files, testcontainers.ContainerFile{
			HostFilePath:      filepath.Join(dir, e.Name()),
			ContainerFilePath: "/docker-entrypoint-initdb.d/" + e.Name(),
			FileMode:          0o644,
		})
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("testenv: no initdb scripts in %s", dir)
	}
	return files, nil
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("testenv: working directory: %w", err)
	}
	for {
		_, err := os.Stat(filepath.Join(dir, "go.mod"))
		switch {
		case err == nil:
			return dir, nil
		case !errors.Is(err, fs.ErrNotExist):
			return "", fmt.Errorf("testenv: look for go.mod: %w", err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("testenv: no go.mod above the working directory")
		}
		dir = parent
	}
}
