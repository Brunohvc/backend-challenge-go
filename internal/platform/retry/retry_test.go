package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var (
	errRetryable = errors.New("serialization failure")
	errFinal     = errors.New("constraint violation")
)

func isRetryable(err error) bool { return errors.Is(err, errRetryable) }

func noWait(time.Duration) time.Duration { return 0 }

func TestBackoff(t *testing.T) {
	const maxDuration = time.Duration(math.MaxInt64)
	tests := []struct {
		name      string
		attempt   int
		base, max time.Duration
		want      time.Duration
	}{
		{"first attempt is the base", 0, 10 * time.Millisecond, time.Second, 10 * time.Millisecond},
		{"doubles", 1, 10 * time.Millisecond, time.Second, 20 * time.Millisecond},
		{"doubles again", 3, 10 * time.Millisecond, time.Second, 80 * time.Millisecond},
		{"capped at max", 10, 10 * time.Millisecond, time.Second, time.Second},
		{"exactly max", 2, 250 * time.Millisecond, time.Second, time.Second},
		{"huge attempt does not overflow", math.MaxInt, 10 * time.Millisecond, time.Second, time.Second},
		{"overflow near the int64 limit", 100, time.Hour, maxDuration, maxDuration},
		{"negative attempt is the base", -5, 10 * time.Millisecond, time.Second, 10 * time.Millisecond},
		{"base above max is max", 0, 2 * time.Second, time.Second, time.Second},
		{"zero base never waits", math.MaxInt, 0, time.Second, 0},
		{"odd limit is exact", 1, time.Nanosecond, 3 * time.Nanosecond, 2 * time.Nanosecond},
		{"odd limit reached", 2, time.Nanosecond, 3 * time.Nanosecond, 3 * time.Nanosecond},
		{"non-positive max never waits", 3, 10 * time.Millisecond, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Backoff(tt.attempt, tt.base, tt.max); got != tt.want {
				t.Errorf("Backoff(%d, %v, %v) = %v, want %v", tt.attempt, tt.base, tt.max, got, tt.want)
			}
		})
	}
}

func TestDo_RetriesOnlyRetryable(t *testing.T) {
	tests := []struct {
		name      string
		attempts  int
		results   []error
		wantCalls int
		wantErr   error
	}{
		{"succeeds after retryable failures", 3, []error{errRetryable, errRetryable, nil}, 3, nil},
		{"non-retryable stops at once", 5, []error{errFinal}, 1, errFinal},
		{"retryable then non-retryable", 5, []error{errRetryable, errFinal}, 2, errFinal},
		{"attempts exhausted", 3, []error{errRetryable}, 3, errRetryable},
		{"zero attempts is one", 0, []error{errRetryable}, 1, errRetryable},
		{"negative attempts is one", -2, []error{errRetryable}, 1, errRetryable},
		{"success first time", 3, []error{nil}, 1, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			p := Policy{Attempts: tt.attempts, Base: time.Millisecond, Max: time.Millisecond, Jitter: noWait}

			err := Do(t.Context(), p, isRetryable, nil, func(context.Context) error {
				err := tt.results[min(calls, len(tt.results)-1)]
				calls++
				return err
			})

			if calls != tt.wantCalls {
				t.Errorf("calls = %d, want %d", calls, tt.wantCalls)
			}
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil) != (err == nil) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestDo_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	calls := 0
	p := Policy{Attempts: 5, Base: time.Hour, Max: time.Hour, Jitter: func(d time.Duration) time.Duration { return d }}

	start := time.Now()
	err := Do(ctx, p, isRetryable, nil, func(context.Context) error {
		calls++
		return errRetryable
	})

	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("Do took %v, want it to stop at the 50ms deadline", elapsed)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1: no attempt after the deadline", calls)
	}
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errRetryable) {
		t.Errorf("err = %v, want it to match both context.DeadlineExceeded and the last error", err)
	}
}

func TestDo_CanceledFromOnRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	p := Policy{Attempts: 5, Base: time.Hour, Max: time.Hour, Jitter: func(d time.Duration) time.Duration { return d }}

	err := Do(ctx, p, isRetryable, func(error) { cancel() }, func(context.Context) error {
		calls++
		return errRetryable
	})

	if calls != 1 || !errors.Is(err, context.Canceled) || !errors.Is(err, errRetryable) {
		t.Errorf("calls = %d, err = %v; want 1 call and both context.Canceled and the last error", calls, err)
	}
}

func TestDo_DefaultJitter(t *testing.T) {
	calls := 0
	p := Policy{Attempts: 5, Base: time.Millisecond, Max: 2 * time.Millisecond}

	start := time.Now()
	err := Do(t.Context(), p, isRetryable, nil, func(context.Context) error {
		calls++
		if calls < 3 {
			return errRetryable
		}
		return nil
	})

	if err != nil || calls != 3 {
		t.Errorf("err = %v, calls = %d; want success on the third call", err, calls)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Do took %v, want two waits of at most 2ms", elapsed)
	}
}

func TestDo_StopsWhenContextAlreadyDone(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	p := Policy{Attempts: 5, Base: time.Millisecond, Max: time.Millisecond, Jitter: noWait}

	err := Do(ctx, p, isRetryable, nil, func(context.Context) error {
		calls++
		return errRetryable
	})

	if calls != 1 || !errors.Is(err, context.Canceled) {
		t.Errorf("calls = %d, err = %v; want 1 call and context.Canceled", calls, err)
	}
}

func TestDo_UsesJitterWithinBound(t *testing.T) {
	var bounds []time.Duration
	p := Policy{Attempts: 4, Base: 10 * time.Millisecond, Max: 25 * time.Millisecond, Jitter: func(d time.Duration) time.Duration {
		bounds = append(bounds, d)
		return 0
	}}

	_ = Do(t.Context(), p, isRetryable, nil, func(context.Context) error { return errRetryable })

	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond}
	if !slices.Equal(bounds, want) {
		t.Errorf("jitter bounds = %v, want %v", bounds, want)
	}
}

func TestFullJitter_WithinBound(t *testing.T) {
	const bound = 50 * time.Millisecond
	seen := map[bool]bool{}
	for range 2000 {
		d := fullJitter(bound)
		if d < 0 || d > bound {
			t.Fatalf("fullJitter(%v) = %v, want within [0, %v]", bound, d, bound)
		}
		seen[d < bound/2] = true
	}
	if !seen[true] || !seen[false] {
		t.Error("fullJitter never landed in one half of its range; it is not spread over [0, bound]")
	}
	for _, d := range []time.Duration{0, -time.Second} {
		if got := fullJitter(d); got != 0 {
			t.Errorf("fullJitter(%v) = %v, want 0", d, got)
		}
	}
	if got := fullJitter(time.Duration(math.MaxInt64)); got < 0 {
		t.Errorf("fullJitter(max) = %v, want non-negative", got)
	}
}

func TestFullJitter_UpperBoundInclusive(t *testing.T) {
	seen := map[time.Duration]bool{}
	for range 200 {
		seen[fullJitter(1)] = true
	}
	if !seen[0] || !seen[1] || len(seen) != 2 {
		t.Errorf("fullJitter(1) produced %v, want exactly 0 and 1", seen)
	}
}

func TestEqualJitter_WithinUpperHalf(t *testing.T) {
	const bound = 50 * time.Millisecond
	seen := map[bool]bool{}
	for range 2000 {
		d := EqualJitter(bound)
		if d < bound/2 || d > bound {
			t.Fatalf("EqualJitter(%v) = %v, want within [%v, %v]", bound, d, bound/2, bound)
		}
		seen[d < bound*3/4] = true
	}
	if !seen[true] || !seen[false] {
		t.Error("EqualJitter never landed in one half of its range; it is not spread over [bound/2, bound]")
	}
}

func TestEqualJitter_Edges(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second, time.Duration(math.MinInt64)} {
		if got := EqualJitter(d); got != 0 {
			t.Errorf("EqualJitter(%v) = %v, want 0", d, got)
		}
	}
	seen := map[time.Duration]bool{}
	for range 300 {
		seen[EqualJitter(3)] = true
	}
	if !seen[1] || !seen[2] || !seen[3] || len(seen) != 3 {
		t.Errorf("EqualJitter(3) produced %v, want exactly 1, 2 and 3", seen)
	}
	const maxDuration = time.Duration(math.MaxInt64)
	for range 100 {
		if got := EqualJitter(maxDuration); got < maxDuration/2 {
			t.Fatalf("EqualJitter(max) = %v, want at least %v", got, maxDuration/2)
		}
	}
}

func TestPositiveJitter_NeverZeroForTinyBounds(t *testing.T) {
	for _, bound := range []time.Duration{1, 2, 3} {
		for range 300 {
			if got := PositiveJitter(bound); got <= 0 || got < bound-bound/2 || got > bound {
				t.Fatalf("PositiveJitter(%v) = %v, want within [%v, %v] and above 0", bound, got, bound-bound/2, bound)
			}
		}
	}
	if got := PositiveJitter(1); got != 1 {
		t.Errorf("PositiveJitter(1) = %v, want 1", got)
	}
}

func TestPositiveJitter_WithinUpperHalf(t *testing.T) {
	for _, bound := range []time.Duration{time.Second, time.Minute, time.Duration(math.MaxInt64)} {
		seen := map[time.Duration]bool{}
		for range 1000 {
			got := PositiveJitter(bound)
			if got < bound-bound/2 || got > bound {
				t.Fatalf("PositiveJitter(%v) = %v, want within [%v, %v]", bound, got, bound-bound/2, bound)
			}
			seen[got] = true
		}
		if len(seen) < 2 {
			t.Errorf("PositiveJitter(%v) always returned %v, want spread values", bound, seen)
		}
	}
}

func TestPositiveJitter_NonPositiveBoundUnchanged(t *testing.T) {
	for _, bound := range []time.Duration{0, -time.Second} {
		if got := PositiveJitter(bound); got != bound {
			t.Errorf("PositiveJitter(%v) = %v, want %v", bound, got, bound)
		}
	}
}

func TestDo_ReturnsLastError(t *testing.T) {
	errs := []error{
		fmt.Errorf("first: %w", errRetryable),
		fmt.Errorf("second: %w", errRetryable),
		fmt.Errorf("third: %w", errRetryable),
	}
	calls := 0
	p := Policy{Attempts: 3, Base: time.Millisecond, Max: time.Millisecond, Jitter: noWait}

	err := Do(t.Context(), p, isRetryable, nil, func(context.Context) error {
		calls++
		return errs[calls-1]
	})

	if err != errs[2] { //nolint:errorlint // asserting identity, not wrapping
		t.Errorf("err = %v, want exactly the last error %v", err, errs[2])
	}
}

func TestDo_OnRetryCalledBetweenAttempts(t *testing.T) {
	var events []string
	calls := 0
	p := Policy{Attempts: 5, Base: time.Millisecond, Max: time.Millisecond, Jitter: noWait}

	err := Do(t.Context(), p, isRetryable, func(err error) {
		events = append(events, "retry after "+err.Error())
	}, func(context.Context) error {
		calls++
		events = append(events, fmt.Sprintf("attempt %d", calls))
		if calls < 3 {
			return fmt.Errorf("e%d: %w", calls, errRetryable)
		}
		return nil
	})

	want := []string{
		"attempt 1", "retry after e1: serialization failure",
		"attempt 2", "retry after e2: serialization failure",
		"attempt 3",
	}
	if err != nil || !slices.Equal(events, want) {
		t.Errorf("err = %v, events = %q; want nil and %q", err, events, want)
	}
}

func TestDo_NoRetryCallbackAfterLastAttempt(t *testing.T) {
	retries := 0
	p := Policy{Attempts: 2, Base: time.Millisecond, Max: time.Millisecond, Jitter: noWait}

	_ = Do(t.Context(), p, isRetryable, func(error) { retries++ }, func(context.Context) error { return errRetryable })

	if retries != 1 {
		t.Errorf("onRetry calls = %d, want 1: only between the two attempts", retries)
	}
}

func TestWait_RunsItsCourse(t *testing.T) {
	start := time.Now()

	if err := Wait(t.Context(), 5*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v, want nil", err)
	}
	if waited := time.Since(start); waited < 5*time.Millisecond {
		t.Errorf("Wait() returned after %s, want at least 5ms", waited)
	}
}

func TestWait_StopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(5*time.Millisecond, cancel)

	if err := Wait(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context.Canceled", err)
	}
}

func TestWait_DoneContextWinsOverZeroWait(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for range 100 {
		if err := Wait(ctx, 0); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait() error = %v, want context.Canceled", err)
		}
	}
}
