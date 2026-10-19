package bootstrap

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/health"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

var postgresModule = fx.Module("postgres",
	fx.Provide(fx.Annotate(newDatabase, fx.As(fx.Self()), fx.As(new(app.UnitOfWork)))),
	fx.Invoke(registerDatabase),
)

var errDatabaseNotStarted = errors.New("bootstrap: database not started")

type database struct {
	cfg        config.DBConfig
	instanceID string
	tracer     tracing.Tracer
	running    atomic.Pointer[runningDatabase]
}

type runningDatabase struct {
	pool *pgxpool.Pool
	uow  *postgres.UnitOfWork
}

func newDatabase(cfg config.Config, tr tracing.Tracer) *database {
	return &database{cfg: cfg.DB, instanceID: cfg.InstanceID, tracer: tr}
}

func (d *database) start(ctx context.Context) error {
	pool, err := postgres.NewPool(ctx, d.cfg, d.instanceID, postgres.WithTracing(d.tracer))
	if err != nil {
		return err
	}
	if err := postgres.CheckSchema(ctx, pool); err != nil {
		pool.Close()
		return err
	}
	d.running.Store(&runningDatabase{pool: pool, uow: postgres.NewUnitOfWork(pool, d.tracer)})
	return nil
}

func (d *database) stop(context.Context) error {
	if r := d.running.Swap(nil); r != nil {
		r.pool.Close()
	}
	return nil
}

func (d *database) started() (*runningDatabase, error) {
	r := d.running.Load()
	if r == nil {
		return nil, errDatabaseNotStarted
	}
	return r, nil
}

func (d *database) ping(ctx context.Context) error {
	r, err := d.started()
	if err != nil {
		return err
	}
	return r.pool.Ping(ctx)
}

func (d *database) Do(ctx context.Context, fn func(ctx context.Context, r app.Repos) error) error {
	r, err := d.started()
	if err != nil {
		return err
	}
	return r.uow.Do(ctx, fn)
}

func (d *database) Snapshot(ctx context.Context, fn func(ctx context.Context, r app.Repos) error) error {
	r, err := d.started()
	if err != nil {
		return err
	}
	return r.uow.Snapshot(ctx, fn)
}

func registerDatabase(l lifecycle, d *database, checker *health.Checker) {
	checker.Add("postgres", d.ping)
	l.add(component{name: "postgres", start: d.start, stop: d.stop})
}
