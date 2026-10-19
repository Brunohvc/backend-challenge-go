package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/worker"
)

const (
	runnerName    = "test-worker"
	waitFor       = 5 * time.Second
	shortInterval = 5 * time.Millisecond
	longInterval  = time.Hour
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) lines(t *testing.T, level slog.Level) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for raw := range strings.SplitSeq(b.buf.String(), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line %q is not JSON: %v", raw, err)
		}
		if line["level"] == level.String() {
			out = append(out, line)
		}
	}
	return out
}

func newLogger() (*slog.Logger, *logBuffer) {
	buf := &logBuffer{}
	return logging.New(buf, slog.LevelDebug, "test"), buf
}

func stopCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	t.Cleanup(cancel)
	return ctx
}

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func mustStop(t *testing.T, r *worker.Runner) {
	t.Helper()
	if err := r.Stop(stopCtx(t)); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(waitFor):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

type step struct {
	didWork bool
	err     error
	panic   any
}

func scripted(steps ...step) (worker.Task, <-chan time.Time) {
	calls := make(chan time.Time, len(steps)+64)
	var n atomic.Int64
	task := func(context.Context) (bool, error) {
		calls <- time.Now()
		i := int(n.Add(1)) - 1
		if i >= len(steps) {
			return false, nil
		}
		if steps[i].panic != nil {
			panic(steps[i].panic)
		}
		return steps[i].didWork, steps[i].err
	}
	return task, calls
}

func TestRunner_StopWaitsForTask(t *testing.T) {
	log, _ := newLogger()
	started, release := make(chan struct{}), make(chan struct{})
	var finished atomic.Bool
	r := worker.NewRunner(runnerName, shortInterval, func(context.Context) (bool, error) {
		close(started)
		<-release
		finished.Store(true)
		return false, nil
	}, log)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	recv(t, started, "task to start")

	stopped := make(chan error, 1)
	go func() { stopped <- r.Stop(stopCtx(t)) }()

	select {
	case err := <-stopped:
		t.Fatalf("Stop returned %v while the task was still running", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := recv(t, stopped, "Stop to return"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !finished.Load() {
		t.Error("Stop returned before the in-flight task did")
	}
}

func TestRunner_StopCancelsTask(t *testing.T) {
	log, logs := newLogger()
	started := make(chan struct{})
	r := worker.NewRunner(runnerName, shortInterval, func(ctx context.Context) (bool, error) {
		close(started)
		<-ctx.Done()
		return false, ctx.Err()
	}, log)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	recv(t, started, "task to start")
	mustStop(t, r)

	if got := logs.lines(t, slog.LevelWarn); len(got) != 0 {
		t.Errorf("the cancellation Stop caused was logged as a failure: %v", got)
	}
}

func TestRunner_StopInterruptsBusyLoop(t *testing.T) {
	log, _ := newLogger()
	started := make(chan struct{})
	var once sync.Once
	r := worker.NewRunner(runnerName, longInterval, func(context.Context) (bool, error) {
		once.Do(func() { close(started) })
		return true, nil
	}, log)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	recv(t, started, "task to start")
	mustStop(t, r)
}

func TestRunner_StopTimeout(t *testing.T) {
	log, _ := newLogger()
	started, release := make(chan struct{}), make(chan struct{})
	r := worker.NewRunner(runnerName, shortInterval, func(context.Context) (bool, error) {
		close(started)
		<-release
		return false, nil
	}, log)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	recv(t, started, "task to start")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := r.Stop(ctx)
	if !errors.Is(err, worker.ErrStopTimeout) {
		t.Fatalf("Stop = %v, want ErrStopTimeout", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stop = %v, want it to wrap the context error", err)
	}

	if err := r.Stop(ctx); !errors.Is(err, worker.ErrStopTimeout) {
		t.Errorf("second Stop = %v, want ErrStopTimeout", err)
	}

	close(release)
	mustStop(t, r)
}

func TestRunner_NoGoroutineLeak(t *testing.T) {
	log, _ := newLogger()
	task, calls := scripted(step{didWork: true}, step{didWork: true})
	r := worker.NewRunner(runnerName, shortInterval, task, log)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for range 3 {
		recv(t, calls, "task call")
	}
	mustStop(t, r)

	goleak.VerifyNone(t)
}

func TestRunner_DidWorkRunsImmediately(t *testing.T) {
	log, _ := newLogger()
	const busyPasses = 5
	steps := make([]step, busyPasses)
	for i := range steps {
		steps[i] = step{didWork: true}
	}
	task, calls := scripted(steps...)
	r := worker.NewRunner(runnerName, longInterval, task, log)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for i := range busyPasses + 1 {
		recv(t, calls, fmt.Sprintf("back-to-back task call %d", i+1))
	}
	mustStop(t, r)
}

func TestRunner_IdleWaitsInterval(t *testing.T) {
	log, _ := newLogger()
	const interval = 30 * time.Millisecond
	task, calls := scripted()
	r := worker.NewRunner(runnerName, interval, task, log)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	prev := recv(t, calls, "first call")
	for i := range 3 {
		next := recv(t, calls, "next call")
		if gap := next.Sub(prev); gap < interval {
			t.Errorf("idle gap %d = %s, want at least %s", i, gap, interval)
		}
		prev = next
	}
	mustStop(t, r)
}

func TestRunner_ErrorBacksOff(t *testing.T) {
	log, logs := newLogger()
	boom := errors.New("boom")
	const interval = shortInterval
	task, calls := scripted(step{err: boom}, step{err: boom}, step{}, step{err: boom}, step{})
	r := worker.NewRunner(runnerName, interval, task, log)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	var at []time.Time
	for range 5 {
		at = append(at, recv(t, calls, "task call"))
	}
	mustStop(t, r)

	for i, lower := range []time.Duration{2 * interval, 4 * interval, interval, 2 * interval} {
		if gap := at[i+1].Sub(at[i]); gap < lower {
			t.Errorf("gap after call %d = %s, want at least %s", i+1, gap, lower)
		}
	}

	warns := logs.lines(t, slog.LevelWarn)
	if len(warns) != 3 {
		t.Fatalf("got %d warnings, want one per failure (3): %v", len(warns), warns)
	}
	wantRetry := []time.Duration{2 * interval, 4 * interval, 2 * interval}
	wantStreak := []float64{1, 2, 1}
	for i, line := range warns {
		if line["worker"] != runnerName {
			t.Errorf("warning %d: worker = %v, want %q", i, line["worker"], runnerName)
		}
		if line["error"] != "boom" {
			t.Errorf("warning %d: error = %v, want boom", i, line["error"])
		}
		if got := line["retryIn"]; got != wantRetry[i].String() {
			t.Errorf("warning %d: retryIn = %v, want %s", i, got, wantRetry[i])
		}
		if got := line["consecutiveErrors"]; got != wantStreak[i] {
			t.Errorf("warning %d: consecutiveErrors = %v, want %v", i, got, wantStreak[i])
		}
	}
}

func TestRunner_PanicIsRecovered(t *testing.T) {
	log, logs := newLogger()
	task, calls := scripted(step{panic: "kaboom"}, step{didWork: true}, step{})
	r := worker.NewRunner(runnerName, shortInterval, task, log)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for range 3 {
		recv(t, calls, "task call")
	}
	mustStop(t, r)

	errs := logs.lines(t, slog.LevelError)
	if len(errs) != 1 {
		t.Fatalf("got %d error lines, want exactly one for the panic: %v", len(errs), errs)
	}
	line := errs[0]
	if line["worker"] != runnerName {
		t.Errorf("worker = %v, want %q", line["worker"], runnerName)
	}
	if line["panic"] != "kaboom" {
		t.Errorf("panic = %v, want kaboom", line["panic"])
	}
	if stack, _ := line["stack"].(string); !strings.Contains(stack, "scripted") {
		t.Errorf("stack does not point at the panicking task:\n%v", line["stack"])
	}
	if got := line["retryIn"]; got != (2 * shortInterval).String() {
		t.Errorf("retryIn = %v, want %s", got, 2*shortInterval)
	}
	if warns := logs.lines(t, slog.LevelWarn); len(warns) != 0 {
		t.Errorf("the panic was logged a second time as a warning: %v", warns)
	}
}

func TestRunner_StartTwice(t *testing.T) {
	log, _ := newLogger()
	task, calls := scripted()
	r := worker.NewRunner(runnerName, shortInterval, task, log)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	recv(t, calls, "task call")

	if err := r.Start(context.Background()); !errors.Is(err, worker.ErrAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrAlreadyStarted", err)
	}
	recv(t, calls, "task call after the rejected Start")
	mustStop(t, r)

	if err := r.Start(context.Background()); !errors.Is(err, worker.ErrAlreadyStarted) {
		t.Fatalf("Start after Stop = %v, want ErrAlreadyStarted", err)
	}
}

func TestRunner_StopIdempotent(t *testing.T) {
	log, _ := newLogger()

	t.Run("before start", func(t *testing.T) {
		r := worker.NewRunner(runnerName, shortInterval, func(context.Context) (bool, error) { return false, nil }, log)
		if err := r.Stop(canceledCtx()); err != nil {
			t.Errorf("Stop before Start = %v, want nil", err)
		}
	})

	t.Run("after stop", func(t *testing.T) {
		task, calls := scripted()
		r := worker.NewRunner(runnerName, shortInterval, task, log)
		if err := r.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		recv(t, calls, "task call")
		mustStop(t, r)

		for range 2 {
			if err := r.Stop(canceledCtx()); err != nil {
				t.Errorf("repeated Stop = %v, want nil", err)
			}
		}
	})
}

func TestRunner_SurvivesStartContext(t *testing.T) {
	log, _ := newLogger()
	type key struct{}
	got := make(chan any, 8)
	var n atomic.Int64
	r := worker.NewRunner(runnerName, shortInterval, func(ctx context.Context) (bool, error) {
		got <- ctx.Value(key{})
		return n.Add(1) < 3, nil
	}, log)

	startCtx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "from-start"))
	if err := r.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancel()

	for range 3 {
		if v := recv(t, got, "task call after the start context was canceled"); v != "from-start" {
			t.Errorf("task context value = %v, want the Start context's values kept", v)
		}
	}
	mustStop(t, r)
}

func TestRunner_StartRejects(t *testing.T) {
	log, _ := newLogger()
	idle := func(context.Context) (bool, error) { return false, nil }

	tests := []struct {
		name string
		r    *worker.Runner
		ctx  context.Context
		want error
	}{
		{"zero interval", worker.NewRunner(runnerName, 0, idle, log), context.Background(), nil},
		{"negative interval", worker.NewRunner(runnerName, -time.Second, idle, log), context.Background(), nil},
		{"nil task", worker.NewRunner(runnerName, shortInterval, nil, log), context.Background(), nil},
		{"canceled context", worker.NewRunner(runnerName, shortInterval, idle, log), canceledCtx(), context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.r.Start(tt.ctx)
			if err == nil {
				mustStop(t, tt.r)
				t.Fatal("Start = nil, want an error")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Errorf("Start = %v, want it to wrap %v", err, tt.want)
			}
			if err := tt.r.Stop(stopCtx(t)); err != nil {
				t.Errorf("Stop after a rejected Start = %v, want nil", err)
			}
		})
	}
}
