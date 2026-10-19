package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/health"
)

func records(t *testing.T, out *bytes.Buffer) []map[string]any {
	t.Helper()
	var recs []map[string]any
	dec := json.NewDecoder(out)
	for dec.More() {
		var r map[string]any
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		recs = append(recs, r)
	}
	return recs
}

func TestComponent_LogsEachTransition(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&out, nil))
	lc := fxtest.NewLifecycle(t)
	noop := func(context.Context) error { return nil }
	lifecycle{Lifecycle: lc, Running: newRunningHook(), Log: log}.add(component{
		name: "thing", start: noop, stop: noop, addr: func() string { return "127.0.0.1:9" },
	})

	lc.RequireStart().RequireStop()

	recs := records(t, &out)
	if len(recs) != 2 {
		t.Fatalf("logged %d lines, want 2: %v", len(recs), recs)
	}
	started, stopped := recs[0], recs[1]
	if started["msg"] != "component started" || started["component"] != "thing" || started["addr"] != "127.0.0.1:9" {
		t.Errorf("start line = %v, want component started, thing, 127.0.0.1:9", started)
	}
	if stopped["msg"] != "component stopped" || stopped["component"] != "thing" {
		t.Errorf("stop line = %v, want component stopped, thing", stopped)
	}
	if _, ok := stopped["addr"]; ok {
		t.Errorf("stop line = %v, want no address", stopped)
	}
}

func TestComponent_FailedStartIsReturned(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&out, nil))
	errRefused := errors.New("refused")
	lc := fxtest.NewLifecycle(t)
	lifecycle{Lifecycle: lc, Running: newRunningHook(), Log: log}.add(component{name: "thing", start: func(context.Context) error { return errRefused }})

	err := lc.Start(t.Context())

	if !errors.Is(err, errRefused) || err.Error() != "thing: refused" {
		t.Fatalf("Start() error = %v, want thing: refused, wrapping %v", err, errRefused)
	}
	if out.Len() != 0 {
		t.Fatalf("logged %q, want nothing", out.String())
	}
}

func TestComponent_FailedStopIsReturned(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&out, nil))
	errStuck := errors.New("stuck")
	lc := fxtest.NewLifecycle(t)
	lifecycle{Lifecycle: lc, Running: newRunningHook(), Log: log}.add(component{name: "thing", stop: func(context.Context) error { return errStuck }})
	lc.RequireStart()

	err := lc.Stop(t.Context())

	if !errors.Is(err, errStuck) || err.Error() != "thing: stuck" {
		t.Fatalf("Stop() error = %v, want thing: stuck, wrapping %v", err, errStuck)
	}
	if out.Len() != 0 {
		t.Fatalf("logged %q, want nothing", out.String())
	}
}

type fakeServer struct {
	done chan struct{}
	once sync.Once
	err  error
}

func newFakeServer() *fakeServer {
	return &fakeServer{done: make(chan struct{})}
}

func (s *fakeServer) Start(context.Context) error { return nil }
func (s *fakeServer) Addr() string                { return "127.0.0.1:9" }
func (s *fakeServer) Done() <-chan struct{}       { return s.done }

func (s *fakeServer) Stop(context.Context) error {
	s.end(nil)
	return nil
}

func (s *fakeServer) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

func (s *fakeServer) end(err error) {
	s.once.Do(func() {
		s.err = err
		close(s.done)
	})
}

type shutdownRecorder struct {
	calls atomic.Int32
}

func (s *shutdownRecorder) Shutdown(...fx.ShutdownOption) error {
	s.calls.Add(1)
	return nil
}

func errorLines(recs []map[string]any) []map[string]any {
	var errs []map[string]any
	for _, r := range recs {
		if r["level"] == slog.LevelError.String() {
			errs = append(errs, r)
		}
	}
	return errs
}

func TestServerComponent_ServingEndedOnItsOwnShutsDown(t *testing.T) {
	var out bytes.Buffer
	srv := newFakeServer()
	app := fxtest.New(t,
		fx.Supply(slog.New(slog.NewJSONHandler(&out, nil))),
		fx.Provide(newRunningHook),
		fx.Invoke(func(l lifecycle) { l.add(serverComponent("thing", srv)) }),
	)
	app.RequireStart()

	srv.end(errors.New("listener broken"))

	select {
	case sig := <-app.Wait():
		if sig.ExitCode != 1 {
			t.Errorf("shutdown exit code = %d, want 1", sig.ExitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no shutdown 5s after the server stopped serving")
	}
	app.RequireStop()
	errs := errorLines(records(t, &out))
	if len(errs) != 1 || errs[0]["component"] != "thing" || errs[0]["error"] != "listener broken" {
		t.Errorf("error lines = %v, want one naming thing and listener broken", errs)
	}
}

func TestServerComponent_StopIsNotAFailure(t *testing.T) {
	before := goleak.IgnoreCurrent()
	var out bytes.Buffer
	sd := &shutdownRecorder{}
	lc := fxtest.NewLifecycle(t)
	lifecycle{Lifecycle: lc, Running: newRunningHook(), Log: slog.New(slog.NewJSONHandler(&out, nil)), Shutdowner: sd}.
		add(serverComponent("thing", newFakeServer()))

	lc.RequireStart().RequireStop()

	goleak.VerifyNone(t, before)
	if n := sd.calls.Load(); n != 0 {
		t.Errorf("Shutdown called %d times, want none", n)
	}
	if errs := errorLines(records(t, &out)); len(errs) != 0 {
		t.Errorf("error lines = %v, want none", errs)
	}
}

func TestDrain_ReportsNotReadyThenWaits(t *testing.T) {
	const delay = 500 * time.Millisecond
	checker := health.NewChecker()
	lc := fxtest.NewLifecycle(t)
	registerDrain(lifecycle{Lifecycle: lc, Running: newRunningHook(), Log: slog.New(slog.DiscardHandler)}, config.Config{ShutdownDrainDelay: delay}, checker)
	lc.RequireStart()
	if ready, _ := checker.Ready(t.Context()); !ready {
		t.Fatal("not ready before the drain")
	}

	began := time.Now()
	stopped := make(chan error, 1)
	go func() { stopped <- lc.Stop(context.WithoutCancel(t.Context())) }()

	for {
		if ready, _ := checker.Ready(t.Context()); !ready {
			break
		}
		select {
		case err := <-stopped:
			t.Fatalf("Stop() returned %v while still ready", err)
		case <-time.After(time.Millisecond):
		}
	}
	if err := <-stopped; err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if elapsed := time.Since(began); elapsed < delay {
		t.Fatalf("drain took %s, want at least %s", elapsed, delay)
	}
}

func TestDrain_GivesUpAtTheDeadline(t *testing.T) {
	lc := fxtest.NewLifecycle(t)
	registerDrain(lifecycle{Lifecycle: lc, Running: newRunningHook(), Log: slog.New(slog.DiscardHandler)},
		config.Config{ShutdownDrainDelay: time.Hour}, health.NewChecker())
	lc.RequireStart()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := lc.Stop(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() error = %v, want context.DeadlineExceeded", err)
	}
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

func (b *logBuffer) errorLog(t *testing.T) []string {
	t.Helper()
	b.mu.Lock()
	written := bytes.NewBuffer(bytes.Clone(b.buf.Bytes()))
	b.mu.Unlock()
	var lines []string
	for _, r := range errorLines(records(t, written)) {
		msg, _ := r["msg"].(string)
		cause, _ := r["error"].(string)
		lines = append(lines, msg+": "+cause)
	}
	return lines
}

func newLoggedApp(out *logBuffer, opts ...fx.Option) *fx.App {
	return fx.New(append([]fx.Option{
		fx.Supply(slog.New(slog.NewJSONHandler(out, nil))),
		fx.Provide(newRunningHook),
		fx.WithLogger(newEventLogger),
	}, opts...)...)
}

func failWith(msg string) func(context.Context) error {
	return func(context.Context) error { return errors.New(msg) }
}

func TestEventLogger_LogsAFailedStartOnce(t *testing.T) {
	var out logBuffer
	app := newLoggedApp(&out, fx.Invoke(func(l lifecycle) {
		l.add(component{name: "admin", stop: failWith("stuck")})
		l.add(component{name: "postgres", start: failWith("refused")})
	}))

	if err := app.Start(t.Context()); err == nil {
		t.Fatal("Start() succeeded")
	}

	want := []string{"OnStart hook failed: postgres: refused", "OnStop hook failed: admin: stuck"}
	if got := out.errorLog(t); !slices.Equal(got, want) {
		t.Errorf("error lines = %q, want %q", got, want)
	}
}

func TestEventLogger_LogsAFailedStopOnce(t *testing.T) {
	var out logBuffer
	app := newLoggedApp(&out, fx.Invoke(func(l lifecycle) {
		l.add(component{name: "postgres", stop: failWith("stuck")})
		l.add(component{name: "http", stop: failWith("jammed")})
	}))
	if err := app.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := app.Stop(t.Context()); err == nil {
		t.Fatal("Stop() succeeded")
	}

	want := []string{"OnStop hook failed: http: jammed", "OnStop hook failed: postgres: stuck"}
	if got := out.errorLog(t); !slices.Equal(got, want) {
		t.Errorf("error lines = %q, want %q", got, want)
	}
}

func TestEventLogger_KeepsTheErrorsOfAStopPastItsDeadline(t *testing.T) {
	var out logBuffer
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	app := newLoggedApp(&out, fx.Invoke(func(l lifecycle) {
		l.add(component{name: "postgres", stop: func(context.Context) error {
			<-release
			return nil
		}})
		l.add(component{name: "workers", stop: func(context.Context) error {
			return errors.Join(errors.New("consumer did not stop in time"), errors.New("relay did not stop in time"))
		}})
	}))
	if err := app.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	if err := app.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() error = %v, want context.DeadlineExceeded", err)
	}

	want := []string{
		"OnStop hook failed: workers: consumer did not stop in time\nrelay did not stop in time",
		"stop failed: postgres: context deadline exceeded",
	}
	if got := out.errorLog(t); !slices.Equal(got, want) {
		t.Errorf("error lines = %q, want %q", got, want)
	}
}

func TestEventLogger_LogsAComponentsOwnTimeoutOnce(t *testing.T) {
	var out logBuffer
	app := newLoggedApp(&out, fx.Invoke(func(l lifecycle) {
		l.add(component{name: "workers", stop: func(context.Context) error {
			return fmt.Errorf("stop timed out: %w", context.DeadlineExceeded)
		}})
	}))
	if err := app.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := app.Stop(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() error = %v, want one wrapping context.DeadlineExceeded", err)
	}

	want := []string{"OnStop hook failed: workers: stop timed out: context deadline exceeded"}
	if got := out.errorLog(t); !slices.Equal(got, want) {
		t.Errorf("error lines = %q, want %q", got, want)
	}
}

func TestEventLogger_NamesTheComponentAStartTimedOutOn(t *testing.T) {
	var out logBuffer
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	app := newLoggedApp(&out, fx.Invoke(func(l lifecycle) {
		l.add(component{name: "admin", start: func(context.Context) error { return nil }})
		l.add(component{name: "postgres", start: func(context.Context) error {
			<-release
			return nil
		}})
	}))
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	if err := app.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want context.DeadlineExceeded", err)
	}

	want := []string{"start failed: postgres: context deadline exceeded"}
	if got := out.errorLog(t); !slices.Equal(got, want) {
		t.Errorf("error lines = %q, want %q", got, want)
	}
}

func TestEventLogger_NamesNoComponentOnceItsHookEnded(t *testing.T) {
	var out logBuffer
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	app := newLoggedApp(&out, fx.Invoke(func(l lifecycle) {
		l.Lifecycle.Append(fx.StopHook(cancel))
		l.add(component{name: "workers", stop: func(context.Context) error { return nil }})
	}))
	if err := app.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := app.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop() error = %v, want context.Canceled", err)
	}

	want := []string{"stop failed: context canceled"}
	if got := out.errorLog(t); !slices.Equal(got, want) {
		t.Errorf("error lines = %q, want %q", got, want)
	}
}

func TestEventLogger_LogsAFailedConstructor(t *testing.T) {
	var out logBuffer
	app := newLoggedApp(&out,
		fx.Provide(func() (*health.Checker, error) { return nil, errors.New("broken") }),
		fx.Invoke(func(*health.Checker) {}),
	)

	if err := app.Start(t.Context()); err == nil {
		t.Fatal("Start() succeeded")
	}

	got := out.errorLog(t)
	if !slices.ContainsFunc(got, func(line string) bool { return strings.HasSuffix(line, "broken") }) {
		t.Errorf("error lines = %q, want one with the constructor's error", got)
	}
}
