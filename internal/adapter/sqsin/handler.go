package sqsin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/failpoint"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
)

const maxClaimedProviderBytes = 128

var errProviderMismatch = errors.New("sqsin: data.providerId is not the provider whose token sent the message")

var permanentErrors = []struct {
	err          error
	reason, code string
}{
	{ErrMissingProviderToken, ReasonUnauthenticated, "MISSING_PROVIDER_TOKEN"},
	{ErrInvalidProviderToken, ReasonUnauthenticated, "INVALID_PROVIDER_TOKEN"},
	{ErrNotProvider, ReasonForbidden, "NOT_PROVIDER"},
	{wagering.ErrMessageIDReused, ReasonMessageIDReused, ""},
	{wagering.ErrIdempotencyKeyReused, ReasonIdempotencyKeyReused, ""},
	{wagering.ErrDuplicateExternalTransaction, ReasonDuplicateExternalTransaction, ""},
	{wagering.ErrWalletNotFound, ReasonWalletNotFound, ""},
	{app.ErrPermanent, ReasonProcessingFailed, ""},
}

type outcomeKind int

const (
	outcomeProcessed outcomeKind = iota
	outcomeDuplicate
	outcomeDeadLetter
	outcomeRetry
	outcomeCanceled
)

type outcome struct {
	kind            outcomeKind
	reason, code    string
	err             error
	status          wager.Status
	claimedProvider string
	fields          logging.Fields
}

func failed(f logging.Fields, err error) outcome {
	for _, p := range permanentErrors {
		if errors.Is(err, p.err) {
			return outcome{kind: outcomeDeadLetter, reason: p.reason, code: p.code, err: err, fields: f}
		}
	}
	return outcome{kind: outcomeRetry, err: err, fields: f}
}

func identityFailed(ctx context.Context, f logging.Fields, err error) outcome {
	o := failed(f, err)
	if o.kind != outcomeDeadLetter || ctx.Err() == nil {
		return o
	}
	return outcome{
		kind:   outcomeRetry,
		err:    fmt.Errorf("sqsin: identify the sender: processing ended (%w) before the verdict %q could be trusted", ctx.Err(), detachedText(err)),
		fields: f,
	}
}

func detachedText(err error) string {
	return err.Error()
}

func invalidMessage(f logging.Fields, code wager.ValidationCode, err error) outcome {
	return outcome{kind: outcomeDeadLetter, reason: ReasonInvalidMessage, code: string(code), err: err, fields: f}
}

func (c *Consumer) process(work context.Context, groupID string, h *heldMessage) bool {
	ctx, cancel := context.WithTimeout(work, c.cfg.ProcessingTimeout)
	defer cancel()
	ctx, span := c.startProcess(ctx, h.msg)
	defer span.End()
	o := c.handle(ctx, h.msg)
	if o.kind == outcomeRetry && work.Err() != nil {
		o.kind = outcomeCanceled
	}
	return c.settle(ctx, groupID, h, o)
}

func (c *Consumer) handle(ctx context.Context, msg types.Message) (o outcome) {
	var f logging.Fields
	defer func() {
		if v := recover(); v != nil {
			o = outcome{kind: outcomeRetry, err: &panicError{value: v, stack: string(debug.Stack())}, fields: f}
		}
	}()

	provider, err := c.identity.ProviderFor(ctx, msg)
	if err != nil {
		return identityFailed(ctx, f, err)
	}
	f.ProviderID = provider

	env, in, err := ParseEnvelope([]byte(aws.ToString(msg.Body)))
	if err != nil {
		return invalidMessage(f, wager.ValidationInvalidRequest, err)
	}
	f.MessageID = env.MessageID
	f.CorrelationID = correlationID(msg, env.MessageID)
	ctx = logging.With(ctx, f)

	if claimsAnotherProvider(in.ProviderID, provider) {
		return outcome{
			kind:            outcomeDeadLetter,
			reason:          ReasonProviderIdentityMismatch,
			err:             errProviderMismatch,
			claimedProvider: truncate(in.ProviderID, maxClaimedProviderBytes),
			fields:          f,
		}
	}
	cmd, err := wager.NewCommand(in)
	if err != nil {
		code := wager.ValidationInvalidRequest
		var verr *wager.ValidationError
		if errors.As(err, &verr) {
			code = verr.Code
		}
		return invalidMessage(f, code, err)
	}
	f.WalletID = cmd.WalletID.String()
	ctx = logging.With(ctx, f)

	hash, err := InboxHash(env)
	if err != nil {
		return invalidMessage(f, wager.ValidationInvalidRequest, err)
	}
	res, err := c.sub.Submit(ctx, cmd, wagering.Source{
		Channel:       wagering.ChannelSQS,
		CorrelationID: f.CorrelationID,
		CausationID:   env.MessageID,
		Inbox:         &wagering.InboxMessage{Consumer: c.cfg.Name, MessageID: env.MessageID, Hash: hash},
	})
	if err != nil {
		return failed(f, err)
	}
	failpoint.Inject(failpoint.ConsumerAfterCommitBeforeDelete)

	f.TransactionID = res.Transaction.ID.String()
	kind := outcomeProcessed
	if res.Replay || res.DuplicateMessage {
		kind = outcomeDuplicate
	}
	return outcome{kind: kind, status: res.Transaction.Status, fields: f}
}

func (c *Consumer) settle(ctx context.Context, groupID string, h *heldMessage, o outcome) bool {
	c.held.settle(h)
	ctx = logging.With(ctx, o.fields)
	log := c.log.With("sqsMessageId", aws.ToString(h.msg.MessageId))

	switch o.kind {
	case outcomeProcessed, outcomeDuplicate:
		result := resultProcessed
		if o.kind == outcomeDuplicate {
			result = resultDuplicate
		}
		if err := callSQS(ctx, func(ctx context.Context) error { return c.deleteMessage(ctx, h.msg) }); err != nil {
			c.record(ctx, resultRetry)
			log.WarnContext(ctx, "message handled but not deleted; it comes back when its visibility runs out and is recognized then",
				"result", result, "error", err)
			return false
		}
		c.record(ctx, result)
		log.InfoContext(ctx, "message handled", "result", result, "status", string(o.status))
		return true

	case outcomeDeadLetter:
		return c.deadLetter(ctx, log, groupID, h.msg, o)

	case outcomeCanceled:
		c.noteResult(ctx, resultCanceled)
		if err := callSQS(ctx, func(ctx context.Context) error { return c.changeVisibility(ctx, h.msg, 0) }); err != nil {
			log.WarnContext(ctx, "could not give back a message whose processing shutdown canceled; it returns when its visibility runs out",
				"error", err)
			return false
		}
		log.InfoContext(ctx, "message processing canceled by shutdown; given back", "error", o.err)
		return false

	default:
		return c.retry(ctx, log, groupID, h.msg, o)
	}
}

func (c *Consumer) deadLetter(ctx context.Context, log *slog.Logger, groupID string, msg types.Message, o outcome) bool {
	err := callSQS(ctx, func(ctx context.Context) error { return c.dlq.Move(ctx, msg, groupID, o.reason, o.code) })
	if err != nil {
		c.retryLater(ctx, log, msg, fmt.Errorf("sqsin: dead-letter the message as %s, for %s: %w", o.reason, detachedText(o.err), err))
		return false
	}
	c.record(ctx, resultDLQ)
	c.m.DLQ(o.reason)
	c.noteDeadLetter(ctx, o.reason)
	if o.reason == ReasonProviderIdentityMismatch {
		log.WarnContext(ctx, "message names another provider than the one that sent it; moved to the dead-letter queue",
			"security", true, "reason", o.reason, "claimedProviderId", o.claimedProvider)
		return true
	}
	var p *panicError
	if errors.As(o.err, &p) {
		log.ErrorContext(ctx, "message handling panicked; moved to the dead-letter queue",
			"reason", o.reason, "error", o.err, "panic", fmt.Sprint(p.value), "stack", p.stack)
		return true
	}
	log.WarnContext(ctx, "message moved to the dead-letter queue", "reason", o.reason, "code", o.code, "error", o.err)
	return true
}

func (c *Consumer) retry(ctx context.Context, log *slog.Logger, groupID string, msg types.Message, o outcome) bool {
	down := c.databaseDown(ctx, log, o.err)
	if n := receiveCount(msg); n >= c.cfg.MaxReceives && !down {
		o.kind, o.reason, o.code = outcomeDeadLetter, ReasonRetriesExhausted, ""
		o.err = fmt.Errorf("sqsin: attempt failed on receive %d of at most %d: %w", n, c.cfg.MaxReceives, o.err)
		return c.deadLetter(ctx, log, groupID, msg, o)
	}
	c.retryLater(ctx, log, msg, o.err)
	return false
}

func (c *Consumer) databaseDown(ctx context.Context, log *slog.Logger, cause error) bool {
	if !errors.Is(cause, app.ErrUnavailable) {
		return false
	}
	if c.unavailable.Load() {
		return true
	}
	pingCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.pingEvery)
	err := c.ping(pingCtx)
	cancel()
	if err == nil {
		return false
	}
	if c.unavailable.CompareAndSwap(false, true) {
		log.WarnContext(ctx, "database unavailable; polling paused until it answers", "error", cause, "pingError", err)
	}
	return true
}

func (c *Consumer) retryLater(ctx context.Context, log *slog.Logger, msg types.Message, cause error) {
	c.record(ctx, resultRetry)
	c.noteRetry(ctx, cause)
	n := receiveCount(msg)
	wait := retry.Backoff(n, retryBase, retryMax)
	var p *panicError
	if errors.As(cause, &p) {
		log.ErrorContext(ctx, "message handling panicked; it will be tried again",
			"panic", fmt.Sprint(p.value), "stack", p.stack, "receiveCount", n, "retryIn", wait)
	} else {
		log.WarnContext(ctx, "message will be tried again", "error", cause, "receiveCount", n, "retryIn", wait)
	}
	if err := callSQS(ctx, func(ctx context.Context) error { return c.changeVisibility(ctx, msg, wait) }); err != nil {
		log.WarnContext(ctx, "could not delay the retry; the message returns when its visibility runs out", "error", err)
	}
}

func claimsAnotherProvider(claimed, sender string) bool {
	return claimed != sender && wager.ValidTextID(claimed)
}

func correlationID(msg types.Message, messageID string) string {
	attr, ok := msg.MessageAttributes[CorrelationIDAttribute]
	if v := aws.ToString(attr.StringValue); ok && aws.ToString(attr.DataType) == "String" && wager.ValidTextID(v) {
		return v
	}
	return messageID
}

func receiveCount(msg types.Message) int {
	n, err := strconv.Atoi(msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

type panicError struct {
	value any
	stack string
}

func (e *panicError) Error() string {
	return fmt.Sprintf("sqsin: panic while handling a message: %v", e.value)
}
