package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx/fxtest"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/awsclient"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/worker"
)

func (b *logBuffer) components(t *testing.T, msg string) []string {
	t.Helper()
	b.mu.Lock()
	written := bytes.NewBuffer(bytes.Clone(b.buf.Bytes()))
	b.mu.Unlock()
	var names []string
	for _, r := range records(t, written) {
		if r["msg"] == msg {
			name, _ := r["component"].(string)
			names = append(names, name)
		}
	}
	return names
}

func workersLifecycle(t *testing.T, out *logBuffer, loops ...loop) *fxtest.Lifecycle {
	t.Helper()
	lc := fxtest.NewLifecycle(t)
	addWorkers(lifecycle{Lifecycle: lc, Running: newRunningHook(), Log: slog.New(slog.NewJSONHandler(out, nil))},
		func() ([]loop, error) { return loops, nil })
	return lc
}

func TestWorkers_StartOneAfterAnotherAndStopTogether(t *testing.T) {
	names := []string{"consumer", "outbox-relay", "resolver"}
	var begun atomic.Int32
	everyStopBegun := make(chan struct{})
	loops := make([]loop, len(names))
	for i, name := range names {
		loops[i] = loop{
			name:  name,
			start: func(context.Context) error { return nil },
			stop: func(ctx context.Context) error {
				if begun.Add(1) == int32(len(names)) {
					close(everyStopBegun)
				}
				select {
				case <-everyStopBegun:
					return nil
				case <-ctx.Done():
					return fmt.Errorf("%s stopped alone: %w", name, ctx.Err())
				}
			},
		}
	}
	var out logBuffer
	lc := workersLifecycle(t, &out, loops...)

	lc.RequireStart()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := lc.Stop(ctx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if got, want := out.components(t, "component started"), append(slices.Clone(names), "workers"); !slices.Equal(got, want) {
		t.Errorf("components started = %v, want %v", got, want)
	}
	stopped := out.components(t, "component stopped")
	if len(stopped) != len(names)+1 || stopped[len(names)] != "workers" {
		t.Fatalf("components stopped = %v, want every worker, then workers", stopped)
	}
	slices.Sort(stopped[:len(names)])
	if want := []string{"consumer", "outbox-relay", "resolver"}; !slices.Equal(stopped[:len(names)], want) {
		t.Errorf("workers stopped = %v, want %v in any order", stopped[:len(names)], want)
	}
}

func TestWorkers_FailedStartStopsTheWorkersStarted(t *testing.T) {
	ctx, endStart := context.WithCancel(t.Context())
	defer endStart()
	var started []string
	var stoppedOnLiveContext bool
	loops := []loop{
		{
			name: "consumer",
			start: func(context.Context) error {
				started = append(started, "consumer")
				endStart()
				return nil
			},
			stop: func(ctx context.Context) error {
				stoppedOnLiveContext = ctx.Err() == nil
				return nil
			},
		},
		{
			name: "outbox-relay",
			start: func(ctx context.Context) error {
				started = append(started, "outbox-relay")
				return fmt.Errorf("refused: %w", ctx.Err())
			},
			stop: func(context.Context) error {
				t.Error("stopped outbox-relay, which never started")
				return nil
			},
		},
		{
			name: "resolver",
			start: func(context.Context) error {
				started = append(started, "resolver")
				return nil
			},
		},
	}
	var out logBuffer
	lc := workersLifecycle(t, &out, loops...)

	err := lc.Start(ctx)

	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("Start() error = %v, want the refusal of outbox-relay", err)
	}
	if want := []string{"consumer", "outbox-relay"}; !slices.Equal(started, want) {
		t.Errorf("starts = %v, want %v", started, want)
	}
	if !stoppedOnLiveContext {
		t.Error("consumer was stopped on the ended context of the start, which leaves it no time to stop")
	}
	if got := out.components(t, "component started"); !slices.Equal(got, []string{"consumer"}) {
		t.Errorf("components started = %v, want [consumer]", got)
	}
	if got := out.components(t, "component stopped"); !slices.Equal(got, []string{"consumer"}) {
		t.Errorf("components stopped = %v, want [consumer]", got)
	}
}

func TestWorkers_UnbuiltWorkersFailTheStart(t *testing.T) {
	errUnbuilt := errors.New("queues not started")
	lc := fxtest.NewLifecycle(t)
	addWorkers(lifecycle{Lifecycle: lc, Running: newRunningHook(), Log: slog.New(slog.DiscardHandler)},
		func() ([]loop, error) { return nil, errUnbuilt })

	if err := lc.Start(t.Context()); !errors.Is(err, errUnbuilt) {
		t.Fatalf("Start() error = %v, want %v", err, errUnbuilt)
	}
}

func TestWorkers_StopEachWithinItsBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stopDeadline, _ := ctx.Deadline()
	var consumerDeadline atomic.Pointer[time.Time]
	loops := []loop{
		{
			name:   "outbox-relay",
			start:  func(context.Context) error { return nil },
			budget: 50 * time.Millisecond,
			stop: func(ctx context.Context) error {
				<-ctx.Done()
				return fmt.Errorf("stuck: %w", ctx.Err())
			},
		},
		{
			name:  "consumer",
			start: func(context.Context) error { return nil },
			stop: func(ctx context.Context) error {
				deadline, _ := ctx.Deadline()
				consumerDeadline.Store(&deadline)
				return nil
			},
		},
	}
	var out logBuffer
	lc := workersLifecycle(t, &out, loops...)
	lc.RequireStart()

	began := time.Now()
	err := lc.Stop(ctx)

	if want := "workers: outbox-relay did not stop in time: context deadline exceeded"; err == nil || err.Error() != want {
		t.Fatalf("Stop() error = %v, want %s", err, want)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stop() error = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(began); elapsed > time.Second {
		t.Errorf("Stop() took %s, want about the 50ms budget of the relay", elapsed)
	}
	if got := consumerDeadline.Load(); got == nil || !got.Equal(stopDeadline) {
		t.Errorf("consumer stopped with the deadline %v, want the stop's own, %v", got, stopDeadline)
	}
	if got := out.components(t, "component stopped"); !slices.Equal(got, []string{"consumer"}) {
		t.Errorf("components stopped = %v, want [consumer] alone", got)
	}
}

func TestWorkers_StopTimeoutNamesTheWorkerOnce(t *testing.T) {
	inPass := make(chan struct{})
	endPass := make(chan struct{})
	r := worker.NewRunner("outbox-relay", time.Millisecond, func(context.Context) (bool, error) {
		close(inPass)
		<-endPass
		return false, nil
	}, slog.New(slog.DiscardHandler))
	var out logBuffer
	lc := workersLifecycle(t, &out, runnerLoop("outbox-relay", r, 50*time.Millisecond))
	lc.RequireStart()
	t.Cleanup(func() {
		close(endPass)
		if err := r.Stop(context.Background()); err != nil {
			t.Errorf("final Stop() error = %v", err)
		}
	})
	<-inPass

	err := lc.Stop(t.Context())

	if want := "workers: outbox-relay did not stop in time: context deadline exceeded"; err == nil || err.Error() != want {
		t.Fatalf("Stop() error = %v, want %s", err, want)
	}
}

func TestRelayLoop_ReleasesOnlyOnceItsRunnerStopped(t *testing.T) {
	t.Run("runner stopped", func(t *testing.T) {
		var inPass atomic.Bool
		passed := make(chan struct{}, 1)
		r := worker.NewRunner("outbox-relay", time.Millisecond, func(context.Context) (bool, error) {
			inPass.Store(true)
			defer inPass.Store(false)
			select {
			case passed <- struct{}{}:
			default:
			}
			return false, nil
		}, slog.New(slog.DiscardHandler))
		var releases atomic.Int32
		l := relayLoop(r, func(context.Context) error {
			if inPass.Load() {
				t.Error("released while a pass was running")
			}
			releases.Add(1)
			return nil
		}, config.OutboxConfig{Lease: time.Second}, slog.New(slog.DiscardHandler))
		if err := l.start(t.Context()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		<-passed

		if err := l.stop(t.Context()); err != nil {
			t.Fatalf("stop() error = %v", err)
		}
		if n := releases.Load(); n != 1 {
			t.Errorf("released %d times, want once", n)
		}
	})

	t.Run("runner did not stop", func(t *testing.T) {
		inPass := make(chan struct{})
		endPass := make(chan struct{})
		r := worker.NewRunner("outbox-relay", time.Millisecond, func(context.Context) (bool, error) {
			close(inPass)
			<-endPass
			return false, nil
		}, slog.New(slog.DiscardHandler))
		l := relayLoop(r, func(context.Context) error {
			t.Error("released while the runner was still publishing")
			return nil
		}, config.OutboxConfig{Lease: time.Second}, slog.New(slog.DiscardHandler))
		if err := l.start(t.Context()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		t.Cleanup(func() {
			close(endPass)
			if err := r.Stop(context.Background()); err != nil {
				t.Errorf("final Stop() error = %v", err)
			}
		})
		<-inPass
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		if err := l.stop(ctx); !errors.Is(err, worker.ErrStopTimeout) {
			t.Fatalf("stop() error = %v, want %v", err, worker.ErrStopTimeout)
		}
	})
}

type releaseRecorder struct {
	calls atomic.Int32
	left  atomic.Int64
	err   error
	block bool
}

func (r *releaseRecorder) release(ctx context.Context) error {
	r.calls.Add(1)
	if deadline, ok := ctx.Deadline(); ok {
		r.left.Store(int64(time.Until(deadline)))
	}
	if r.block {
		<-ctx.Done()
		return fmt.Errorf("release: %w", ctx.Err())
	}
	return r.err
}

func idleRunner(t *testing.T) *worker.Runner {
	t.Helper()
	return worker.NewRunner("outbox-relay", time.Millisecond, func(context.Context) (bool, error) {
		return false, nil
	}, slog.New(slog.DiscardHandler))
}

func TestRelayLoop_ReleasesWithATimeOfItsOwn(t *testing.T) {
	t.Run("capped at its own timeout", func(t *testing.T) {
		rec := &releaseRecorder{}
		l := relayLoop(idleRunner(t), rec.release, config.OutboxConfig{Lease: time.Second}, slog.New(slog.DiscardHandler))
		if err := l.start(t.Context()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		if err := l.stop(ctx); err != nil {
			t.Fatalf("stop() error = %v", err)
		}

		left := time.Duration(rec.left.Load())
		if left <= 0 || left > config.OutboxReleaseTimeout {
			t.Errorf("the release had %s, want at most its own timeout, %s", left, config.OutboxReleaseTimeout)
		}
	})

	t.Run("after a pass that used the whole lease", func(t *testing.T) {
		cfg := config.OutboxConfig{Lease: 600 * time.Millisecond}
		inPass := make(chan struct{})
		r := worker.NewRunner("outbox-relay", time.Millisecond, func(context.Context) (bool, error) {
			close(inPass)
			time.Sleep(cfg.Lease - 200*time.Millisecond)
			return false, nil
		}, slog.New(slog.DiscardHandler))
		rec := &releaseRecorder{}
		l := relayLoop(r, rec.release, cfg, slog.New(slog.DiscardHandler))
		if err := l.start(t.Context()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		<-inPass
		ctx, cancel := context.WithTimeout(t.Context(), l.budget)
		defer cancel()

		if err := l.stop(ctx); err != nil {
			t.Fatalf("stop() error = %v, want the pass and the release to fit the budget", err)
		}

		if left := time.Duration(rec.left.Load()); left < config.OutboxReleaseTimeout*3/4 {
			t.Errorf("the release had %s after the pass, want about its own %s", left, config.OutboxReleaseTimeout)
		}
	})

	t.Run("the budget of the loop adds the release to the lease", func(t *testing.T) {
		cfg := config.OutboxConfig{Lease: 30 * time.Second}
		l := relayLoop(idleRunner(t), (&releaseRecorder{}).release, cfg, slog.New(slog.DiscardHandler))

		if want := cfg.Lease + config.OutboxReleaseTimeout; l.budget != want {
			t.Errorf("budget = %s, want %s", l.budget, want)
		}
	})
}

func TestRelayLoop_ReleaseTimeoutIsOnlyWarned(t *testing.T) {
	t.Run("timed out", func(t *testing.T) {
		var out bytes.Buffer
		rec := &releaseRecorder{block: true}
		l := relayLoop(idleRunner(t), rec.release, config.OutboxConfig{Lease: time.Second}, slog.New(slog.NewJSONHandler(&out, nil)))
		if err := l.start(t.Context()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		if err := l.stop(ctx); err != nil {
			t.Fatalf("stop() error = %v, want nil", err)
		}

		if n := rec.calls.Load(); n != 1 {
			t.Errorf("released %d times, want once", n)
		}
		var warned int
		for _, r := range records(t, &out) {
			if r["level"] == slog.LevelWarn.String() {
				warned++
				if r["component"] != "outbox-relay" || !strings.Contains(fmt.Sprint(r["error"]), "context deadline exceeded") {
					t.Errorf("warning = %v, want it to name the relay and the deadline", r)
				}
				continue
			}
			t.Errorf("log line %v, want only the warning", r)
		}
		if warned != 1 {
			t.Errorf("%d warnings, want one", warned)
		}
	})

	t.Run("failed", func(t *testing.T) {
		var out bytes.Buffer
		errDown := errors.New("database down")
		rec := &releaseRecorder{err: errDown}
		l := relayLoop(idleRunner(t), rec.release, config.OutboxConfig{Lease: time.Second}, slog.New(slog.NewJSONHandler(&out, nil)))
		if err := l.start(t.Context()); err != nil {
			t.Fatalf("start() error = %v", err)
		}

		if err := l.stop(t.Context()); !errors.Is(err, errDown) {
			t.Fatalf("stop() error = %v, want %v", err, errDown)
		}
		if out.Len() != 0 {
			t.Errorf("logged %q, want nothing: the error is returned, to be logged once", out.String())
		}
	})
}

func workersConfig(on ...string) config.Config {
	return config.Config{
		Consumer: config.ConsumerConfig{Enabled: slices.Contains(on, "consumer"), DrainTimeout: 20 * time.Second},
		Outbox:   config.OutboxConfig{Enabled: slices.Contains(on, "outbox"), Lease: 30 * time.Second, PollInterval: time.Second},
		Resolver: config.ResolverConfig{Enabled: slices.Contains(on, "resolver"), Lease: 40 * time.Second, PollInterval: time.Second},
	}
}

func startedQueues() *queues {
	q := &queues{}
	q.state.Store(&queueState{
		client: sqs.New(sqs.Options{Region: "us-east-1"}),
		urls:   awsclient.Queues{Input: "http://sqs/input", InputDLQ: "http://sqs/input-dlq", Events: "http://sqs/events"},
	})
	return q
}

func TestWorkerDeps_BuildsTheWorkersSwitchedOn(t *testing.T) {
	tests := []struct {
		on      []string
		names   []string
		budgets []time.Duration
	}{
		{
			on:      []string{"consumer", "outbox", "resolver"},
			names:   []string{"consumer", "outbox-relay", "outbox-stats", "resolver"},
			budgets: []time.Duration{25 * time.Second, 32 * time.Second, 30 * time.Second, 40 * time.Second},
		},
		{on: []string{"consumer"}, names: []string{"consumer"}, budgets: []time.Duration{25 * time.Second}},
		{
			on:      []string{"outbox"},
			names:   []string{"outbox-relay", "outbox-stats"},
			budgets: []time.Duration{32 * time.Second, 30 * time.Second},
		},
		{on: []string{"resolver"}, names: []string{"resolver"}, budgets: []time.Duration{40 * time.Second}},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.on), func(t *testing.T) {
			m, _ := metrics.New()
			cfg := workersConfig(tc.on...)
			d := workerDeps{Config: cfg, Queues: startedQueues(), Database: newDatabase(cfg, tracing.Tracer{}), Metrics: m, Log: slog.New(slog.DiscardHandler)}

			loops, err := d.build()
			if err != nil {
				t.Fatalf("build() error = %v", err)
			}

			var names []string
			var budgets []time.Duration
			for _, l := range loops {
				names = append(names, l.name)
				budgets = append(budgets, l.budget)
			}
			if !slices.Equal(names, tc.names) || !slices.Equal(budgets, tc.budgets) {
				t.Errorf("workers = %v with budgets %v, want %v with %v", names, budgets, tc.names, tc.budgets)
			}
		})
	}
}

func TestWorkerDeps_NeedTheQueuesOnlyForSQS(t *testing.T) {
	tests := map[string]error{"consumer": errQueuesNotStarted, "outbox": errQueuesNotStarted, "resolver": nil}
	for on, want := range tests {
		t.Run(on, func(t *testing.T) {
			m, _ := metrics.New()
			cfg := workersConfig(on)
			d := workerDeps{Config: cfg, Queues: newQueues(cfg), Database: newDatabase(cfg, tracing.Tracer{}), Metrics: m, Log: slog.New(slog.DiscardHandler)}

			if _, err := d.build(); !errors.Is(err, want) {
				t.Errorf("build() error = %v, want %v", err, want)
			}
		})
	}
}
