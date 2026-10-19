package sqsin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// The attributes a moved message carries in the dead-letter queue.
const (
	FailureReasonAttribute   = "FailureReason"
	FailureCodeAttribute     = "FailureCode"
	SourceMessageIDAttribute = "SourceMessageId"
)

const maxAttributeBytes = 256

// DLQClient is the part of the SQS client the mover uses.
type DLQClient interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// DLQMover moves a message from the input queue to its dead-letter queue.
type DLQMover struct {
	client   DLQClient
	inputURL string
	dlqURL   string
}

// NewDLQMover returns a mover between the queues at inputURL and dlqURL.
func NewDLQMover(c DLQClient, inputURL, dlqURL string) *DLQMover {
	return &DLQMover{client: c, inputURL: inputURL, dlqURL: dlqURL}
}

// Move sends a copy of msg to the dead-letter queue, then deletes msg from the input queue.
func (d *DLQMover) Move(ctx context.Context, msg types.Message, groupID, reason, code string) error {
	if groupID == "" {
		return errors.New("sqsin: move to DLQ: empty message group")
	}
	attrs := map[string]types.MessageAttributeValue{
		SourceMessageIDAttribute: stringAttribute(aws.ToString(msg.MessageId)),
	}
	if reason = sanitizeAttribute(reason); reason != "" {
		attrs[FailureReasonAttribute] = stringAttribute(reason)
	}
	if code = sanitizeAttribute(code); code != "" {
		attrs[FailureCodeAttribute] = stringAttribute(code)
	}
	if correlation, ok := msg.MessageAttributes[CorrelationIDAttribute]; ok {
		attrs[CorrelationIDAttribute] = correlation
	}

	if _, err := d.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(d.dlqURL),
		MessageBody:            msg.Body,
		MessageGroupId:         aws.String(groupID),
		MessageDeduplicationId: msg.MessageId,
		MessageAttributes:      attrs,
	}); err != nil {
		return fmt.Errorf("sqsin: send message %s to DLQ: %w", aws.ToString(msg.MessageId), err)
	}
	if _, err := d.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(d.inputURL),
		ReceiptHandle: msg.ReceiptHandle,
	}); err != nil {
		return fmt.Errorf("sqsin: delete message %s after moving it to DLQ: %w", aws.ToString(msg.MessageId), err)
	}
	return nil
}

func sanitizeAttribute(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == utf8.RuneError || r == 0xFFFE || r == 0xFFFF || unicode.IsControl(r) {
			r = '?'
		}
		if b.Len()+utf8.RuneLen(r) > maxAttributeBytes {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

func stringAttribute(v string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
}
