// Package retry repeats an operation that failed for a retryable reason, with bounded backoff.
package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Policy bounds the retries of an operation.
type Policy struct {
	Attempts  int
	Base, Max time.Duration
	Jitter    func(bound time.Duration) time.Duration
}

// Do runs fn until it succeeds, fails with a non-retryable error or exhausts p.Attempts.
func Do(ctx context.Context, p Policy, retryable func(error) bool, onRetry func(error), fn func(context.Context) error) error {
	jitter := p.Jitter
	if jitter == nil {
		jitter = fullJitter
	}
	attempts := max(p.Attempts, 1)
	for attempt := 0; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if attempt == attempts-1 || !retryable(err) {
			return err
		}
		if onRetry != nil {
			onRetry(err)
		}
		if waitErr := Wait(ctx, jitter(Backoff(attempt, p.Base, p.Max))); waitErr != nil {
			return fmt.Errorf("retry: stopped waiting to retry (%w) after: %w", waitErr, err)
		}
	}
}

// Wait blocks for d or until ctx ends, returning ctx.Err() in the second case.
func Wait(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Backoff returns the upper bound of the wait before retry number attempt, capped at limit.
func Backoff(attempt int, base, limit time.Duration) time.Duration {
	if base <= 0 || limit <= 0 {
		return 0
	}
	d := base
	for range attempt {
		if d > limit/2 {
			return limit
		}
		d *= 2
	}
	return min(d, limit)
}

// EqualJitter returns a random duration in [bound/2, bound], or 0 for a non-positive bound.
func EqualJitter(bound time.Duration) time.Duration {
	if bound <= 0 {
		return 0
	}
	floor := bound / 2
	return floor + fullJitter(bound-floor)
}

// PositiveJitter returns a random duration in [bound-bound/2, bound], never 0 for a positive bound.
func PositiveJitter(bound time.Duration) time.Duration {
	if bound <= 0 {
		return bound
	}
	half := bound / 2
	return bound - half + fullJitter(half)
}

func fullJitter(bound time.Duration) time.Duration {
	if bound <= 0 {
		return 0
	}
	if bound == math.MaxInt64 {
		return time.Duration(rand.Int64N(int64(bound))) //nolint:gosec // jitter, not a secret
	}
	return time.Duration(rand.Int64N(int64(bound) + 1)) //nolint:gosec // jitter, not a secret
}
