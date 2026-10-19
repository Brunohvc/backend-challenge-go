package wagering

import (
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const (
	spanSubmit = "wagering.submit"
	spanResume = "wagering.resume"
)

const errorTypePanicked = "panicked"

const (
	attrChannel          = attribute.Key("wagering.channel")
	attrKind             = attribute.Key("wagering.kind")
	attrStatus           = attribute.Key("wagering.status")
	attrFailureCode      = attribute.Key("wagering.failure_code")
	attrProviderID       = attribute.Key("wagering.provider_id")
	attrTransactionID    = attribute.Key("wagering.transaction_id")
	attrReplay           = attribute.Key("wagering.replay")
	attrDuplicateMessage = attribute.Key("wagering.duplicate_message")
	attrResumeOutcome    = attribute.Key("wagering.resume_outcome")
	attrWalletID         = attribute.Key("wallet.id")
)

var errorTypes = []struct {
	err  error
	name string
}{
	{ErrIdempotencyKeyReused, "idempotency_key_reused"},
	{ErrDuplicateExternalTransaction, "duplicate_external_transaction"},
	{ErrWalletNotFound, "wallet_not_found"},
	{ErrTransactionNotFound, "transaction_not_found"},
	{ErrMessageIDReused, "message_id_reused"},
}

func errorType(err error) string {
	for _, e := range errorTypes {
		if errors.Is(err, e.err) {
			return e.name
		}
	}
	return app.ErrorType(err)
}

func endSubmit(span trace.Span, cmd wager.Command, src Source, res Result, err error, panicked bool) {
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(
		attrChannel.String(string(src.Channel)),
		attrKind.String(string(cmd.Kind)),
		attrProviderID.String(cmd.ProviderID),
		attrWalletID.String(cmd.WalletID.String()),
	)
	if res.Replay {
		span.SetAttributes(attrReplay.Bool(true))
	}
	if res.DuplicateMessage {
		span.SetAttributes(attrDuplicateMessage.Bool(true))
	}
	if tx := res.Transaction; tx.Status != "" {
		setOutcome(span, tx.ID.String(), tx.Status, tx.FailureCode)
	}
	end(span, err, panicked)
}

func endResume(span trace.Span, due app.DueTransaction, tx *wager.Transaction, outcome ResumeOutcome, err error, panicked bool) {
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(
		attrChannel.String(resolverChannel),
		attrTransactionID.String(due.ID.String()),
		attrWalletID.String(due.WalletID.String()),
	)
	if outcome != "" {
		span.SetAttributes(attrResumeOutcome.String(string(outcome)))
	}
	if tx != nil {
		span.SetAttributes(attrKind.String(string(tx.Kind())), attrProviderID.String(tx.ProviderID()))
		setOutcome(span, tx.ID().String(), tx.Status(), tx.FailureCode())
	}
	end(span, err, panicked)
}

func setOutcome(span trace.Span, txID string, status wager.Status, code wager.FailureCode) {
	span.SetAttributes(attrTransactionID.String(txID), attrStatus.String(string(status)))
	if code != "" {
		span.SetAttributes(attrFailureCode.String(string(code)))
	}
}

func end(span trace.Span, err error, panicked bool) {
	switch {
	case panicked:
		tracing.Fail(span, errorTypePanicked)
	case err != nil:
		tracing.Fail(span, errorType(err))
	}
	span.End()
}
