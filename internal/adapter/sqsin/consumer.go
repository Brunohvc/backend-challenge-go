package sqsin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/awsclient"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

// CorrelationIDAttribute is the message attribute carrying the correlation id of a message.
const CorrelationIDAttribute = "correlationId"

// The reasons a message is moved to the dead-letter queue.
const (
	ReasonInvalidMessage               = "INVALID_MESSAGE"
	ReasonUnauthenticated              = "UNAUTHENTICATED"
	ReasonForbidden                    = "FORBIDDEN"
	ReasonProviderIdentityMismatch     = "PROVIDER_IDENTITY_MISMATCH"
	ReasonMessageIDReused              = "MESSAGE_ID_REUSED"
	ReasonIdempotencyKeyReused         = "IDEMPOTENCY_KEY_REUSED"
	ReasonDuplicateExternalTransaction = "DUPLICATE_EXTERNAL_TRANSACTION"
	ReasonWalletNotFound               = "WALLET_NOT_FOUND"
	ReasonProcessingFailed             = "PROCESSING_FAILED"
	ReasonRetriesExhausted             = "RETRIES_EXHAUSTED"
)

const (
	resultProcessed = "processed"
	resultDuplicate = "duplicate"
	resultRetry     = "retry"
	resultDLQ       = "dlq"
	resultCanceled  = "canceled"
)

const (
	receiveBatchSize = 10
	maxVisibility    = 12 * time.Hour

	retryBase = time.Second
	retryMax  = 5 * time.Minute

	pingInterval = 2 * time.Second

	receiveRetryBase = time.Second
	receiveRetryMax  = 30 * time.Second
)

// SQSAPI is the part of the SQS client the consumer uses.
type SQSAPI interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
}

// DLQ moves a message to the dead-letter queue.
type DLQ interface {
	Move(ctx context.Context, msg types.Message, groupID, reason, code string) error
}

// Submitter decides a wagering command.
type Submitter interface {
	Submit(ctx context.Context, cmd wager.Command, src wagering.Source) (wagering.Result, error)
}

// Metrics records what the consumer did with each message it settled.
type Metrics interface {
	Message(result string)
	DLQ(reason string)
}

// Consumer reads the wagering commands of the input queue and submits them.
type Consumer struct {
	cfg         config.ConsumerConfig
	client      SQSAPI
	queueURL    string
	identity    IdentityResolver
	dlq         DLQ
	sub         Submitter
	ping        func(context.Context) error
	m           Metrics
	tracer      tracing.Tracer
	destination string
	log         *slog.Logger

	pingEvery    time.Duration
	receiveRetry time.Duration

	held        heldSet
	slots       chan struct{}
	unavailable atomic.Bool

	mu  sync.Mutex
	run *run
}

type run struct {
	stopPolling context.CancelFunc
	cancelWork  context.CancelFunc
	polled      chan struct{}
	drained     chan struct{}
	extended    chan struct{}
	groups      sync.WaitGroup
}

// Deps are the collaborators of a Consumer.
type Deps struct {
	Client    SQSAPI
	Identity  IdentityResolver
	DLQ       DLQ
	Submitter Submitter
	Ping      func(context.Context) error
	Metrics   Metrics
	Tracer    tracing.Tracer
	Log       *slog.Logger
}

// NewConsumer returns a consumer of the input queue of q, reading through d.Client.
func NewConsumer(cfg config.ConsumerConfig, q awsclient.Queues, d Deps) *Consumer {
	log := d.Log
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		cfg:          cfg,
		client:       d.Client,
		queueURL:     q.Input,
		identity:     d.Identity,
		dlq:          d.DLQ,
		sub:          d.Submitter,
		ping:         d.Ping,
		m:            d.Metrics,
		tracer:       d.Tracer,
		destination:  path.Base(q.Input),
		log:          log.With("consumer", cfg.Name),
		pingEvery:    pingInterval,
		receiveRetry: receiveRetryBase,
		held:         heldSet{msgs: make(map[*heldMessage]struct{})},
	}
}

// Start launches the poll loop and the visibility extender, and returns at once.
func (c *Consumer) Start(ctx context.Context) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sqsin: start consumer %s: %w", c.cfg.Name, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.run != nil {
		return fmt.Errorf("sqsin: consumer %s already started", c.cfg.Name)
	}
	base := context.WithoutCancel(ctx)
	pollCtx, stopPolling := context.WithCancel(base)
	workCtx, cancelWork := context.WithCancel(base)
	extendCtx, stopExtender := context.WithCancel(base)
	r := &run{
		stopPolling: stopPolling,
		cancelWork:  cancelWork,
		polled:      make(chan struct{}),
		drained:     make(chan struct{}),
		extended:    make(chan struct{}),
	}
	c.run = r
	c.slots = make(chan struct{}, c.cfg.MaxGroups)

	go c.extend(extendCtx, r.extended)
	go c.poll(pollCtx, workCtx, r)
	go func() {
		<-r.polled
		r.groups.Wait()
		close(r.drained)
		stopExtender()
	}()
	c.log.InfoContext(base, "consumer started",
		"maxGroups", c.cfg.MaxGroups, "visibilityTimeout", c.cfg.VisibilityTimeout, "processingTimeout", c.cfg.ProcessingTimeout,
		"maxReceives", c.cfg.MaxReceives)
	return nil
}

func (c *Consumer) validate() error {
	var errs []error
	if c.cfg.Name == "" {
		errs = append(errs, errors.New("name is empty: it keys the inbox"))
	}
	if c.cfg.MaxGroups < 1 {
		errs = append(errs, fmt.Errorf("max groups must be at least 1, got %d", c.cfg.MaxGroups))
	}
	if c.cfg.VisibilityTimeout < time.Second || c.cfg.VisibilityTimeout > maxVisibility {
		errs = append(errs, fmt.Errorf("visibility timeout must be between 1s and %s, got %s", maxVisibility, c.cfg.VisibilityTimeout))
	}
	if c.cfg.ProcessingTimeout <= 0 || c.cfg.ProcessingTimeout >= c.cfg.VisibilityTimeout {
		errs = append(errs, fmt.Errorf("processing timeout must be positive and shorter than the visibility timeout (%s), got %s",
			c.cfg.VisibilityTimeout, c.cfg.ProcessingTimeout))
	}
	if c.cfg.WaitTime < time.Second || c.cfg.WaitTime > config.MaxConsumerWaitTime {
		errs = append(errs, fmt.Errorf("wait time must be between 1s and %s, got %s", config.MaxConsumerWaitTime, c.cfg.WaitTime))
	}
	if c.cfg.MaxReceives < 1 {
		errs = append(errs, fmt.Errorf("max receives must be at least 1, got %d", c.cfg.MaxReceives))
	}
	if c.cfg.DrainTimeout < 0 {
		errs = append(errs, fmt.Errorf("drain timeout must not be negative, got %s", c.cfg.DrainTimeout))
	}
	if c.ping == nil {
		errs = append(errs, errors.New("no database ping"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("sqsin: consumer %s: %w", c.cfg.Name, err)
	}
	return nil
}

// Stop ends the consumer without losing a message or leaving one hidden longer than needed.
func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	r := c.run
	c.mu.Unlock()
	if r == nil {
		return nil
	}

	r.stopPolling()
	if err := waitClosed(ctx, r.polled); err != nil {
		r.cancelWork()
		return c.stopFailed(err)
	}

	drain := time.NewTimer(c.cfg.DrainTimeout)
	defer drain.Stop()
	select {
	case <-r.drained:
	case <-drain.C:
		c.log.WarnContext(ctx, "drain timeout reached; canceling the messages still in process",
			"drainTimeout", c.cfg.DrainTimeout)
		r.cancelWork()
	case <-ctx.Done():
		r.cancelWork()
	}
	if err := waitClosed(ctx, r.drained); err != nil {
		return c.stopFailed(err)
	}

	r.cancelWork()
	if err := waitClosed(ctx, r.extended); err != nil {
		return c.stopFailed(err)
	}
	c.log.InfoContext(ctx, "consumer stopped")
	return nil
}

func (c *Consumer) stopFailed(err error) error {
	return fmt.Errorf("sqsin: stop consumer %s: %w", c.cfg.Name, err)
}

func (c *Consumer) poll(ctx, work context.Context, r *run) {
	defer close(r.polled)
	failures := 0
	for {
		if !c.acquireSlot(ctx) {
			return
		}
		if c.unavailable.Load() {
			c.releaseSlot()
			c.awaitDatabase(ctx)
			continue
		}

		msgs, err := c.receive(ctx)
		if err != nil {
			c.releaseSlot()
			if ctx.Err() != nil {
				return
			}
			failures++
			wait := retry.Backoff(failures-1, c.receiveRetry, receiveRetryMax)
			c.log.WarnContext(ctx, "receive failed", "error", err, "consecutiveErrors", failures, "retryIn", wait)
			if retry.Wait(ctx, wait) != nil {
				return
			}
			continue
		}
		failures = 0
		if len(msgs) == 0 {
			c.releaseSlot()
			continue
		}
		c.dispatch(ctx, work, r, msgs)
	}
}

func (c *Consumer) receive(ctx context.Context) ([]types.Message, error) {
	out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(c.queueURL),
		MaxNumberOfMessages:         receiveBatchSize,
		WaitTimeSeconds:             wholeSeconds(c.cfg.WaitTime),
		VisibilityTimeout:           wholeSeconds(c.cfg.VisibilityTimeout),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		MessageAttributeNames:       []string{"All"},
		ReceiveRequestAttemptId:     aws.String(attemptID()),
	})
	if err != nil {
		return nil, fmt.Errorf("sqsin: receive: %w", err)
	}
	return out.Messages, nil
}

func (c *Consumer) awaitDatabase(ctx context.Context) {
	t := time.NewTicker(c.pingEvery)
	defer t.Stop()
	for pings := 1; ; pings++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		pingCtx, cancel := context.WithTimeout(ctx, c.pingEvery)
		err := c.ping(pingCtx)
		cancel()
		if err == nil {
			c.unavailable.Store(false)
			c.log.InfoContext(ctx, "database answers again; polling resumed", "pings", pings)
			return
		}
		c.log.DebugContext(ctx, "database still unavailable", "error", err)
	}
}

func attemptID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func waitClosed(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
