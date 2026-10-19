package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/health"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/otlp"
)

const tracingName = "tracing"

var platformModule = fx.Module("platform",
	fx.Provide(
		newLogger,
		newRunningHook,
		newTracing,
		newTracer,
		metrics.New,
		newAdminServer,
		health.NewChecker,
		newClock,
		newIDGenerator,
	),
	fx.Invoke(registerTracing, registerAdmin),
)

func newLogger(cfg config.Config) *slog.Logger {
	return logging.New(os.Stdout, cfg.LogLevel, cfg.InstanceID)
}

func newTracing(cfg config.Config, log *slog.Logger) (*otlp.Provider, error) {
	return otlp.New(cfg.Tracing, cfg.InstanceID, log)
}

func newTracer(p *otlp.Provider) tracing.Tracer {
	return p.Tracer()
}

func registerTracing(l lifecycle, cfg config.Config, p *otlp.Provider) {
	if !cfg.Tracing.Enabled() {
		return
	}
	l.add(component{name: tracingName, stop: flushSpans(p, l.Log)})
}

func flushSpans(p *otlp.Provider, log *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, config.SpanFlushTimeout)
		defer cancel()
		if err := p.Shutdown(ctx); err != nil {
			log.WarnContext(ctx, "spans still queued were not exported",
				slog.String("component", tracingName), slog.Any("error", err))
		}
		return nil
	}
}

func newEventLogger(log *slog.Logger, running *runningHook) fxevent.Logger {
	l := &fxevent.SlogLogger{Logger: log}
	l.UseLogLevel(slog.LevelDebug)
	return onceLogger{Logger: l, running: running}
}

type onceLogger struct {
	fxevent.Logger
	running *runningHook
}

func (l onceLogger) LogEvent(event fxevent.Event) {
	switch e := event.(type) {
	case *fxevent.OnStartExecuted, *fxevent.OnStopExecuted:
		l.Logger.LogEvent(event)
		l.running.take()
		return
	case *fxevent.Started:
		if e.Err != nil {
			if !isContextError(e.Err) {
				return
			}
			event = &fxevent.Started{Err: l.waitedOn(e.Err)}
		}
	case *fxevent.Stopped:
		if e.Err != nil {
			if !isContextError(e.Err) {
				return
			}
			event = &fxevent.Stopped{Err: l.waitedOn(e.Err)}
		}
	case *fxevent.RollingBack, *fxevent.RolledBack:
		return
	}
	l.Logger.LogEvent(event)
}

func (l onceLogger) waitedOn(err error) error {
	if name := l.running.take(); name != "" {
		return fmt.Errorf("%s: %w", name, err)
	}
	return err
}

func isContextError(err error) bool {
	return err == context.DeadlineExceeded || err == context.Canceled //nolint:errorlint // identity, not wrapped errors
}

func newAdminServer(cfg config.Config, reg *prometheus.Registry) *metrics.AdminServer {
	return metrics.NewAdminServer(cfg.Admin.Addr, reg)
}

func newClock() app.Clock {
	return clock.System{}
}

func newIDGenerator() app.IDGenerator {
	return idgen.V7{}
}

func registerAdmin(l lifecycle, s *metrics.AdminServer) {
	l.add(serverComponent("admin", s))
}
