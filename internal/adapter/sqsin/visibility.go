package sqsin

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

const renewBatchSize = 10

type heldMessage struct {
	msg types.Message

	mu       sync.Mutex
	released bool
}

type heldSet struct {
	mu   sync.Mutex
	msgs map[*heldMessage]struct{}
}

func (s *heldSet) hold(msgs []types.Message) []*heldMessage {
	held := make([]*heldMessage, len(msgs))
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, msg := range msgs {
		held[i] = &heldMessage{msg: msg}
		s.msgs[held[i]] = struct{}{}
	}
	return held
}

func (s *heldSet) settle(h *heldMessage) {
	h.mu.Lock()
	h.released = true
	h.mu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.msgs, h)
}

func (s *heldSet) inBatches(n int, fn func(batch []*heldMessage)) {
	s.mu.Lock()
	held := make([]*heldMessage, 0, len(s.msgs))
	for h := range s.msgs {
		held = append(held, h)
	}
	s.mu.Unlock()

	batch := make([]*heldMessage, 0, n)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		fn(batch)
		for _, h := range batch {
			h.mu.Unlock()
		}
		batch = batch[:0]
	}
	for _, h := range held {
		h.mu.Lock()
		if h.released {
			h.mu.Unlock()
			continue
		}
		if batch = append(batch, h); len(batch) == n {
			flush()
		}
	}
	flush()
}

func (c *Consumer) extend(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(c.cfg.VisibilityTimeout / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.held.inBatches(renewBatchSize, func(batch []*heldMessage) { c.renew(ctx, batch) })
	}
}

func (c *Consumer) renew(ctx context.Context, batch []*heldMessage) {
	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(batch))
	for i, h := range batch {
		entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(strconv.Itoa(i)),
			ReceiptHandle:     h.msg.ReceiptHandle,
			VisibilityTimeout: wholeSeconds(c.cfg.VisibilityTimeout),
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, config.SQSCallTimeout)
	defer cancel()
	out, err := c.client.ChangeMessageVisibilityBatch(callCtx, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(c.queueURL),
		Entries:  entries,
	})
	if err != nil {
		c.log.WarnContext(ctx, "could not renew the visibility of held messages; trying again at the next renewal",
			"messages", len(batch), "error", err)
		return
	}
	for _, f := range out.Failed {
		i, err := strconv.Atoi(aws.ToString(f.Id))
		if err != nil || i < 0 || i >= len(batch) {
			continue
		}
		h, code := batch[i], aws.ToString(f.Code)
		if lostHandle(code, aws.ToString(f.Message)) {
			h.released = true
			c.log.WarnContext(ctx, "a held message is no longer in flight for this consumer; no longer renewing it",
				"sqsMessageId", aws.ToString(h.msg.MessageId), "code", code)
			continue
		}
		c.log.WarnContext(ctx, "could not renew the visibility of a held message; trying again at the next renewal",
			"sqsMessageId", aws.ToString(h.msg.MessageId), "code", code, "error", aws.ToString(f.Message))
	}
}

const staleHandleReason = "message does not exist or is not available for visibility timeout change"

func lostHandle(code, message string) bool {
	switch strings.TrimPrefix(code, "AWS.SimpleQueueService.") {
	case "ReceiptHandleIsInvalid", "MessageNotInflight":
		return true
	case "InvalidParameterValue":
		return strings.Contains(strings.ToLower(message), staleHandleReason)
	}
	return false
}

func (c *Consumer) changeVisibility(ctx context.Context, msg types.Message, d time.Duration) error {
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.queueURL),
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: wholeSeconds(d),
	})
	if err != nil {
		return fmt.Errorf("sqsin: change the visibility of message %s: %w", aws.ToString(msg.MessageId), err)
	}
	return nil
}

func (c *Consumer) deleteMessage(ctx context.Context, msg types.Message) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		return fmt.Errorf("sqsin: delete message %s: %w", aws.ToString(msg.MessageId), err)
	}
	return nil
}

func callSQS(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.SQSCallTimeout)
	defer cancel()
	return fn(ctx)
}

func wholeSeconds(d time.Duration) int32 {
	d = min(max(d, 0), maxVisibility)
	return int32((d + time.Second - 1) / time.Second)
}
