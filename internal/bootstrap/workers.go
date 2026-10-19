package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.uber.org/fx"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsout"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/worker"
	"github.com/Brunohvc/backend-challenge-go/internal/worker/outboxrelay"
	"github.com/Brunohvc/backend-challenge-go/internal/worker/resolver"
)

const (
	consumerName = "consumer"
	relayName    = "outbox-relay"
	statsName    = "outbox-stats"
	resolverName = "resolver"
)

const outboxStatsInterval = 5 * time.Second

func workersModule(cfg config.Config) fx.Option {
	if !cfg.Consumer.Enabled && !cfg.Outbox.Enabled && !cfg.Resolver.Enabled {
		return fx.Options()
	}
	return fx.Module("workers", fx.Invoke(registerWorkers))
}

type workerDeps struct {
	fx.In

	Config   config.Config
	Queues   *queues
	Database *database
	Wagering *wagering.Service
	Verifier *oidc.Verifier
	Metrics  *metrics.Metrics
	Tracer   tracing.Tracer
	Log      *slog.Logger
}

func registerWorkers(l lifecycle, d workerDeps) {
	addWorkers(l, d.build)
}

type loop struct {
	name   string
	start  func(context.Context) error
	stop   func(context.Context) error
	budget time.Duration
}

func (d workerDeps) build() ([]loop, error) {
	var s *queueState
	if d.Config.Consumer.Enabled || d.Config.Outbox.Enabled {
		var err error
		if s, err = d.Queues.started(); err != nil {
			return nil, err
		}
	}
	var loops []loop
	if d.Config.Consumer.Enabled {
		loops = append(loops, d.consumer(s))
	}
	if d.Config.Outbox.Enabled {
		loops = append(loops, d.outbox(s)...)
	}
	if d.Config.Resolver.Enabled {
		loops = append(loops, d.resolver())
	}
	return loops, nil
}

func (d workerDeps) consumer(s *queueState) loop {
	cfg := d.Config.Consumer
	c := sqsin.NewConsumer(cfg, s.urls, sqsin.Deps{
		Client:    s.client,
		Identity:  sqsin.TokenAttributeIdentity{Verifier: d.Verifier},
		DLQ:       sqsin.NewDLQMover(s.client, s.urls.Input, s.urls.InputDLQ),
		Submitter: d.Wagering,
		Ping:      d.Database.ping,
		Metrics:   d.Metrics,
		Tracer:    d.Tracer,
		Log:       d.Log,
	})
	return loop{name: consumerName, start: c.Start, stop: c.Stop, budget: cfg.StopBudget()}
}

func (d workerDeps) outbox(s *queueState) []loop {
	cfg := d.Config.Outbox
	pub := sqsout.NewPublisher(s.client, s.urls, d.Tracer, d.Log)
	relay := outboxrelay.New(d.Database, pub, d.Config.InstanceID, cfg, d.Metrics, d.Tracer, d.Log)
	return []loop{
		relayLoop(worker.NewRunner(relayName, cfg.PollInterval, relay.RunOnce, d.Log), relay.Release, cfg, d.Log),
		runnerLoop(statsName, worker.NewRunner(statsName, outboxStatsInterval, relay.StatsOnce, d.Log), cfg.Lease),
	}
}

func (d workerDeps) resolver() loop {
	cfg := d.Config.Resolver
	r := resolver.New(d.Wagering, cfg, d.Metrics, d.Tracer, d.Log)
	return runnerLoop(resolverName, worker.NewRunner(resolverName, cfg.PollInterval, r.RunOnce, d.Log), cfg.Lease)
}

func runnerLoop(name string, r *worker.Runner, budget time.Duration) loop {
	return loop{name: name, start: r.Start, stop: r.Stop, budget: budget}
}

func relayLoop(r *worker.Runner, release func(context.Context) error, cfg config.OutboxConfig, log *slog.Logger) loop {
	l := runnerLoop(relayName, r, cfg.StopBudget())
	l.stop = func(ctx context.Context) error {
		runnerCtx, cancel := context.WithTimeout(ctx, cfg.Lease)
		defer cancel()
		if err := r.Stop(runnerCtx); err != nil {
			return err
		}

		releaseCtx, cancel := context.WithTimeout(ctx, config.OutboxReleaseTimeout)
		defer cancel()
		err := release(releaseCtx)
		if errors.Is(err, context.DeadlineExceeded) {
			log.WarnContext(ctx, "outbox relay could not hand its leases back in time; they run out on their own",
				slog.String("component", relayName), slog.Any("error", err))
			return nil
		}
		return err
	}
	return l
}

func addWorkers(l lifecycle, build func() ([]loop, error)) {
	g := &workerGroup{log: l.Log, build: build}
	l.add(component{name: "workers", start: g.start, stop: g.stop})
}

type workerGroup struct {
	log     *slog.Logger
	build   func() ([]loop, error)
	running []loop
}

func (g *workerGroup) start(ctx context.Context) error {
	loops, err := g.build()
	if err != nil {
		return err
	}
	for _, l := range loops {
		if err := l.start(ctx); err != nil {
			return errors.Join(err, g.stop(context.WithoutCancel(ctx)))
		}
		g.running = append(g.running, l)
		g.log.InfoContext(ctx, "component started", slog.String("component", l.name))
	}
	return nil
}

func (g *workerGroup) stop(ctx context.Context) error {
	running := g.running
	g.running = nil
	errs := make([]error, len(running))
	var wg sync.WaitGroup
	for i, l := range running {
		wg.Go(func() { errs[i] = g.stopOne(ctx, l) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (g *workerGroup) stopOne(ctx context.Context, l loop) error {
	if l.budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, l.budget)
		defer cancel()
	}
	if err := l.stop(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%s did not stop in time: %w", l.name, context.DeadlineExceeded)
		}
		return err
	}
	g.log.InfoContext(ctx, "component stopped", slog.String("component", l.name))
	return nil
}
