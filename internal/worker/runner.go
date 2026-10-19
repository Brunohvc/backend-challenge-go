// Package worker runs the service's background loops.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
)

const maxBackoff = 30 * time.Second

// The errors a Runner returns from Stop and Start.
var (
	ErrStopTimeout    = errors.New("worker: stop timed out")
	ErrAlreadyStarted = errors.New("worker: already started")
)

// Task is one pass of a loop, reporting whether it found work.
type Task func(ctx context.Context) (didWork bool, err error)

// Runner calls a Task repeatedly in its own goroutine until stopped.
type Runner struct {
	name     string
	interval time.Duration
	task     Task
	log      *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewRunner returns a Runner that calls task, waiting interval between idle passes.
func NewRunner(name string, interval time.Duration, task Task, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.Default()
	}
	return &Runner{
		name:     name,
		interval: interval,
		task:     task,
		log:      log.With("worker", name),
	}
}

// Start launches the loop in the background and returns at once.
func (r *Runner) Start(ctx context.Context) error {
	if r.interval <= 0 {
		return fmt.Errorf("worker: %s: interval must be positive, got %s", r.name, r.interval)
	}
	if r.task == nil {
		return fmt.Errorf("worker: %s: task is nil", r.name)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("worker: %s: start: %w", r.name, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done != nil {
		return fmt.Errorf("%w: %s", ErrAlreadyStarted, r.name)
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	r.cancel, r.done = cancel, done

	r.log.InfoContext(runCtx, "worker started", "interval", r.interval)
	go r.run(runCtx, done)
	return nil
}

// Stop cancels the loop and waits for it to end, for at most as long as ctx allows.
func (r *Runner) Stop(ctx context.Context) error {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if done == nil {
		return nil
	}
	cancel()

	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: %s: %w", ErrStopTimeout, r.name, ctx.Err())
	}
}

func (r *Runner) run(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	defer r.log.InfoContext(ctx, "worker stopped")

	failures := 0
	for ctx.Err() == nil {
		didWork, err := r.runTask(ctx)

		var wait time.Duration
		switch {
		case err != nil:
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				return
			}
			failures++
			wait = retry.Backoff(failures, r.interval, maxBackoff)
			r.logFailure(ctx, err, failures, wait)
		case didWork:
			failures = 0
			continue
		default:
			failures = 0
			wait = r.interval
		}

		if retry.Wait(ctx, wait) != nil {
			return
		}
	}
}

func (r *Runner) runTask(ctx context.Context) (didWork bool, err error) {
	defer func() {
		if v := recover(); v != nil {
			didWork, err = false, &panicError{value: v, stack: string(debug.Stack())}
		}
	}()
	return r.task(ctx)
}

func (r *Runner) logFailure(ctx context.Context, err error, failures int, retryIn time.Duration) {
	var p *panicError
	if errors.As(err, &p) {
		r.log.ErrorContext(ctx, "worker task panicked",
			"panic", fmt.Sprint(p.value),
			"stack", p.stack,
			"consecutiveErrors", failures,
			"retryIn", retryIn)
		return
	}
	r.log.WarnContext(ctx, "worker task failed",
		"error", err,
		"consecutiveErrors", failures,
		"retryIn", retryIn)
}

type panicError struct {
	value any
	stack string
}

func (e *panicError) Error() string {
	return fmt.Sprintf("panic: %v", e.value)
}
