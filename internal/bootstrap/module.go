// Package bootstrap composes the wallet service with Uber Fx and runs its lifecycle.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.uber.org/fx"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/health"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
)

const startTimeout = 30 * time.Second

// Module is the whole service process for cfg.
func Module(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		fx.StartTimeout(startTimeout),
		fx.StopTimeout(cfg.ShutdownTimeout),
		fx.WithLogger(newEventLogger),
		platformModule,
		postgresModule,
		awsModule,
		authModule,
		appModule,
		httpModule(cfg.HTTP),
		workersModule(cfg),
		drainOption(cfg.HTTP),
	)
}

func drainOption(cfg config.HTTPConfig) fx.Option {
	if !cfg.Enabled {
		return fx.Options()
	}
	return fx.Invoke(registerDrain)
}

type lifecycle struct {
	fx.In

	Lifecycle  fx.Lifecycle
	Log        *slog.Logger
	Shutdowner fx.Shutdowner
	Running    *runningHook
}

func (l lifecycle) add(c component) {
	l.Lifecycle.Append(c.hook(l))
}

type runningHook struct {
	name atomic.Pointer[string]
}

func newRunningHook() *runningHook {
	return &runningHook{}
}

func (r *runningHook) begin(name string) {
	r.name.Store(&name)
}

func (r *runningHook) take() string {
	if name := r.name.Swap(nil); name != nil {
		return *name
	}
	return ""
}

type component struct {
	name  string
	start func(context.Context) error
	stop  func(context.Context) error
	addr  func() string
	ends  ender
}

type ender interface {
	Done() <-chan struct{}
	Err() error
}

type server interface {
	Start(context.Context) error
	Stop(context.Context) error
	Addr() string
	ender
}

func serverComponent(name string, s server) component {
	return component{name: name, start: s.Start, stop: s.Stop, addr: s.Addr, ends: s}
}

func (c component) hook(l lifecycle) fx.Hook {
	return fx.Hook{
		OnStart: func(ctx context.Context) error {
			if c.start == nil {
				return nil
			}
			l.Running.begin(c.name)
			if err := c.start(ctx); err != nil {
				return fmt.Errorf("%s: %w", c.name, err)
			}
			attrs := []any{slog.String("component", c.name)}
			if c.addr != nil {
				attrs = append(attrs, slog.String("addr", c.addr()))
			}
			l.Log.InfoContext(ctx, "component started", attrs...)
			if c.ends != nil {
				go c.watch(context.WithoutCancel(ctx), l.Log, l.Shutdowner)
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if c.stop == nil {
				return nil
			}
			l.Running.begin(c.name)
			if err := c.stop(ctx); err != nil {
				return fmt.Errorf("%s: %w", c.name, err)
			}
			l.Log.InfoContext(ctx, "component stopped", slog.String("component", c.name))
			return nil
		},
	}
}

func (c component) watch(ctx context.Context, log *slog.Logger, shutdown fx.Shutdowner) {
	<-c.ends.Done()
	err := c.ends.Err()
	if err == nil {
		return
	}
	log.ErrorContext(ctx, "component failed, shutting down",
		slog.String("component", c.name), slog.Any("error", err))
	_ = shutdown.Shutdown(fx.ExitCode(1))
}

func registerDrain(l lifecycle, cfg config.Config, checker *health.Checker) {
	l.add(component{
		name: "drain",
		stop: func(ctx context.Context) error {
			checker.SetShuttingDown()
			l.Log.InfoContext(ctx, "shutting down: reporting not ready",
				slog.Duration("drainDelay", cfg.ShutdownDrainDelay))
			if err := retry.Wait(ctx, cfg.ShutdownDrainDelay); err != nil {
				return fmt.Errorf("bootstrap: drain cut short: %w", err)
			}
			return nil
		},
	})
}
