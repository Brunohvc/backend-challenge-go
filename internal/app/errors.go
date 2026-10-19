package app

import (
	"context"
	"errors"
)

// The infrastructure errors adapters report, matched with errors.Is.
var (
	ErrNotFound      = errors.New("app: not found")
	ErrAlreadyExists = errors.New("app: already exists")
	ErrRetryable     = errors.New("app: retryable conflict")
	ErrUnavailable   = errors.New("app: temporarily unavailable")
	ErrPermanent     = errors.New("app: permanent failure")
)

// RetryReason values label db_tx_retries_total.
const (
	RetrySerialization = "serialization"
	RetryDeadlock      = "deadlock"
	RetryLockTimeout   = "lock_timeout"
	RetryVersion       = "version"
	RetryUniqueRace    = "unique_race"
	RetryOther         = "retryable"
)

var knownRetryReasons = map[string]bool{
	RetrySerialization: true, RetryDeadlock: true, RetryLockTimeout: true,
	RetryVersion: true, RetryUniqueRace: true,
}

type retryableError struct {
	reason string
	cause  error
}

func (e *retryableError) Error() string {
	if e.cause == nil {
		return ErrRetryable.Error()
	}
	return ErrRetryable.Error() + ": " + e.cause.Error()
}

func (e *retryableError) Is(target error) bool { return target == ErrRetryable }

func (e *retryableError) Unwrap() error { return e.cause }

// Retryable returns an ErrRetryable error for reason, wrapping cause.
func Retryable(reason string, cause error) error {
	if !knownRetryReasons[reason] {
		reason = RetryOther
	}
	return &retryableError{reason: reason, cause: cause}
}

// RetryReasonOf returns the reason of the outermost Retryable error in err, or RetryOther.
func RetryReasonOf(err error) string {
	var re *retryableError
	if errors.As(err, &re) {
		return re.reason
	}
	return RetryOther
}

// ErrorType names the class of err with a value of a closed set, for telemetry; it is "" for nil.
func ErrorType(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, ErrUnavailable):
		return "unavailable"
	case errors.Is(err, ErrRetryable):
		return RetryReasonOf(err)
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrAlreadyExists):
		return "already_exists"
	case errors.Is(err, ErrPermanent):
		return "permanent"
	}
	return "other"
}
