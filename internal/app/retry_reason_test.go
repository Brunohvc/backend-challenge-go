package app_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
)

func TestRetryable(t *testing.T) {
	cause := errors.New("serialization failure")

	err := fmt.Errorf("postgres: update: %w", app.Retryable(app.RetrySerialization, cause))

	if !errors.Is(err, app.ErrRetryable) {
		t.Error("errors.Is(err, ErrRetryable) = false")
	}
	if !errors.Is(err, cause) {
		t.Error("the cause is lost")
	}
	for _, other := range []error{app.ErrNotFound, app.ErrAlreadyExists, app.ErrUnavailable, app.ErrPermanent} {
		if errors.Is(err, other) {
			t.Errorf("err matches %v too", other)
		}
	}
	if got := app.RetryReasonOf(err); got != app.RetrySerialization {
		t.Errorf("RetryReasonOf = %q, want %q", got, app.RetrySerialization)
	}
	if want := "postgres: update: app: retryable conflict: serialization failure"; err.Error() != want {
		t.Errorf("Error() = %q, want %q", err, want)
	}
}

func TestRetryReasonOf(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"each known reason", app.Retryable(app.RetryDeadlock, nil), app.RetryDeadlock},
		{"lock timeout", app.Retryable(app.RetryLockTimeout, nil), app.RetryLockTimeout},
		{"version", app.Retryable(app.RetryVersion, nil), app.RetryVersion},
		{"unique race", app.Retryable(app.RetryUniqueRace, nil), app.RetryUniqueRace},
		{"unknown reason", app.Retryable("connection reset by 10.0.0.7", nil), app.RetryOther},
		{"bare sentinel", fmt.Errorf("x: %w", app.ErrRetryable), app.RetryOther},
		{"not retryable", errors.New("boom"), app.RetryOther},
		{"nil", nil, app.RetryOther},
		{"outermost reason wins", app.Retryable(app.RetryVersion, app.Retryable(app.RetryDeadlock, nil)), app.RetryVersion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := app.RetryReasonOf(tt.err); got != tt.want {
				t.Errorf("RetryReasonOf = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRetryable_NilCause(t *testing.T) {
	err := app.Retryable(app.RetryVersion, nil)

	if !errors.Is(err, app.ErrRetryable) || err.Error() != app.ErrRetryable.Error() {
		t.Errorf("err = %q, want just the sentinel's text and errors.Is ErrRetryable", err)
	}
}

func TestErrorType(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"not found", fmt.Errorf("x: %w", app.ErrNotFound), "not_found"},
		{"already exists", fmt.Errorf("x: %w", app.ErrAlreadyExists), "already_exists"},
		{"permanent", fmt.Errorf("x: %w", app.ErrPermanent), "permanent"},
		{"unavailable", fmt.Errorf("x: %w", app.ErrUnavailable), "unavailable"},
		{"retryable names its reason", fmt.Errorf("x: %w", app.Retryable(app.RetryLockTimeout, nil)), app.RetryLockTimeout},
		{"retryable without a known reason", fmt.Errorf("x: %w", app.ErrRetryable), app.RetryOther},
		{"retries ran out", fmt.Errorf("x: %w: %w", app.ErrUnavailable, app.Retryable(app.RetryDeadlock, nil)), "unavailable"},
		{"canceled", fmt.Errorf("x: %w: %w", app.ErrUnavailable, context.Canceled), "canceled"},
		{"deadline", fmt.Errorf("x: %w: %w", app.ErrUnavailable, context.DeadlineExceeded), "deadline_exceeded"},
		{"anything else", errors.New("wallet 0192f291 failed at 25.00"), "other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := app.ErrorType(tt.err); got != tt.want {
				t.Errorf("ErrorType() = %q, want %q", got, tt.want)
			}
		})
	}
}
