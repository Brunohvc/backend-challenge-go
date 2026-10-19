package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
)

func classify(err error) error {
	if err == nil || classified(err) {
		return err
	}
	err = withoutServerMessage(err)
	class := classOf(err)
	if errors.Is(class, app.ErrRetryable) {
		return app.Retryable(retryReason(err), err)
	}
	return fmt.Errorf("%w: %w", class, err)
}

const (
	codeLedgerMismatch = "WL001"
	codeForbiddenWrite = "WL002"
)

var integrityDescriptions = map[string]string{
	codeLedgerMismatch: "the ledger does not explain a balance or a transaction",
	codeForbiddenWrite: "a write the schema's guards forbid",
}

type integrityError struct {
	pg *pgconn.PgError
}

func (e *integrityError) Error() string {
	return fmt.Sprintf("%s (SQLSTATE %s, %s)", integrityDescriptions[e.pg.Code], e.pg.Code, e.pg.ConstraintName)
}

func (e *integrityError) Unwrap() error { return e.pg }

func withoutServerMessage(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == codeLedgerMismatch || pgErr.Code == codeForbiddenWrite) {
		return &integrityError{pg: pgErr}
	}
	return err
}

func retryReason(err error) string {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return app.RetryOther
	}
	switch pgErr.Code {
	case pgerrcode.SerializationFailure:
		return app.RetrySerialization
	case pgerrcode.DeadlockDetected:
		return app.RetryDeadlock
	case pgerrcode.LockNotAvailable:
		return app.RetryLockTimeout
	case pgerrcode.UniqueViolation:
		return app.RetryUniqueRace
	}
	return app.RetryOther
}

func classified(err error) bool {
	return errors.Is(err, app.ErrNotFound) || errors.Is(err, app.ErrAlreadyExists) ||
		errors.Is(err, app.ErrRetryable) || errors.Is(err, app.ErrUnavailable) || errors.Is(err, app.ErrPermanent)
}

func classOf(err error) error {
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return app.ErrUnavailable
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return classOfServerError(pgErr)
	}
	if unreachable(err) {
		return app.ErrUnavailable
	}
	return app.ErrPermanent
}

func classOfServerError(e *pgconn.PgError) error {
	switch {
	case e.Code == pgerrcode.SerializationFailure,
		e.Code == pgerrcode.DeadlockDetected,
		e.Code == pgerrcode.LockNotAvailable:
		return app.ErrRetryable
	case e.Code == pgerrcode.UniqueViolation:
		return classOfUniqueViolation(e.ConstraintName)
	case pgerrcode.IsConnectionException(e.Code),
		pgerrcode.IsInsufficientResources(e.Code),
		pgerrcode.IsOperatorIntervention(e.Code):
		return app.ErrUnavailable
	}
	return app.ErrPermanent
}

func classOfUniqueViolation(constraint string) error {
	switch constraint {
	case "ux_wallets_player_currency":
		return app.ErrAlreadyExists
	case "ux_tx_provider_key", "ux_tx_provider_external", "ux_tx_single_reversal":
		return app.ErrRetryable
	}
	return app.ErrPermanent
}

func unreachable(err error) bool {
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.As(err, &netErr) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, pgconn.ErrConnClosed) ||
		errors.Is(err, puddle.ErrClosedPool)
}
