package sqsin_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
)

type fakeQueues struct {
	calls     []string
	sent      *sqs.SendMessageInput
	deleted   *sqs.DeleteMessageInput
	sendErr   error
	deleteErr error
}

func (f *fakeQueues) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.calls = append(f.calls, "send")
	f.sent = in
	return &sqs.SendMessageOutput{}, f.sendErr
}

func (f *fakeQueues) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.calls = append(f.calls, "delete")
	f.deleted = in
	return &sqs.DeleteMessageOutput{}, f.deleteErr
}

func received() types.Message {
	return types.Message{
		MessageId:     aws.String("1e7058fc-0000-4000-8000-000000000001"),
		ReceiptHandle: aws.String("handle-1"),
		Body:          aws.String(exampleBody),
	}
}

func TestDLQMover_SendsThenDeletes(t *testing.T) {
	q := &fakeQueues{}
	d := sqsin.NewDLQMover(q, "input-url", "dlq-url")

	if err := d.Move(t.Context(), received(), "wallet-1", "invalid message", "INVALID_MESSAGE"); err != nil {
		t.Fatalf("Move: %v", err)
	}

	if len(q.calls) != 2 || q.calls[0] != "send" || q.calls[1] != "delete" {
		t.Fatalf("calls = %v, want [send delete]", q.calls)
	}
	s := q.sent
	if aws.ToString(s.QueueUrl) != "dlq-url" || aws.ToString(s.MessageBody) != exampleBody ||
		aws.ToString(s.MessageGroupId) != "wallet-1" || aws.ToString(s.MessageDeduplicationId) != aws.ToString(received().MessageId) {
		t.Errorf("SendMessage = url %s, group %s, dedup %s; want dlq-url, the same body, group wallet-1 and dedup = MessageId",
			aws.ToString(s.QueueUrl), aws.ToString(s.MessageGroupId), aws.ToString(s.MessageDeduplicationId))
	}
	for name, want := range map[string]string{
		sqsin.FailureReasonAttribute:   "invalid message",
		sqsin.FailureCodeAttribute:     "INVALID_MESSAGE",
		sqsin.SourceMessageIDAttribute: aws.ToString(received().MessageId),
	} {
		if got := aws.ToString(s.MessageAttributes[name].StringValue); got != want || aws.ToString(s.MessageAttributes[name].DataType) != "String" {
			t.Errorf("attribute %s = %q, want String %q", name, got, want)
		}
	}
	if aws.ToString(q.deleted.QueueUrl) != "input-url" || aws.ToString(q.deleted.ReceiptHandle) != "handle-1" {
		t.Errorf("DeleteMessage = %s %s, want input-url handle-1", aws.ToString(q.deleted.QueueUrl), aws.ToString(q.deleted.ReceiptHandle))
	}
}

func TestDLQMover_KeepsCorrelationButNotToken(t *testing.T) {
	q := &fakeQueues{}
	d := sqsin.NewDLQMover(q, "input-url", "dlq-url")
	msg := received()
	correlation := types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("corr-1")}
	msg.MessageAttributes = map[string]types.MessageAttributeValue{
		sqsin.CorrelationIDAttribute: correlation,
		sqsin.ProviderTokenAttribute: {DataType: aws.String("String"), StringValue: aws.String("secret-token")},
	}

	if err := d.Move(t.Context(), msg, "wallet-1", "UNAUTHENTICATED", "INVALID_PROVIDER_TOKEN"); err != nil {
		t.Fatalf("Move: %v", err)
	}

	attrs := q.sent.MessageAttributes
	if got := attrs[sqsin.CorrelationIDAttribute]; aws.ToString(got.DataType) != "String" || aws.ToString(got.StringValue) != "corr-1" {
		t.Errorf("correlation attribute = %s %q, want String corr-1", aws.ToString(got.DataType), aws.ToString(got.StringValue))
	}
	if _, ok := attrs[sqsin.ProviderTokenAttribute]; ok {
		t.Error("the provider token was copied to the DLQ")
	}
}

func TestDLQMover_KeepsMessageWhenSendFails(t *testing.T) {
	q := &fakeQueues{sendErr: errors.New("throttled")}
	d := sqsin.NewDLQMover(q, "input-url", "dlq-url")

	err := d.Move(t.Context(), received(), "wallet-1", "invalid message", "INVALID_MESSAGE")

	if err == nil {
		t.Fatal("Move succeeded although SendMessage failed")
	}
	if len(q.calls) != 1 {
		t.Errorf("calls = %v, want only the send: a message not in the DLQ must stay in the input queue", q.calls)
	}
}

func TestDLQMover_RequiresGroup(t *testing.T) {
	q := &fakeQueues{}
	d := sqsin.NewDLQMover(q, "input-url", "dlq-url")

	if err := d.Move(t.Context(), received(), "", "r", "C"); err == nil || len(q.calls) != 0 {
		t.Errorf("Move with no group: err = %v, calls = %v; want an error and no call", err, q.calls)
	}
}

func TestDLQMover_DeleteFailureIsReported(t *testing.T) {
	q := &fakeQueues{deleteErr: errors.New("receipt handle expired")}
	d := sqsin.NewDLQMover(q, "input-url", "dlq-url")

	err := d.Move(t.Context(), received(), "wallet-1", "invalid message", "INVALID_MESSAGE")

	if !errors.Is(err, q.deleteErr) {
		t.Errorf("err = %v, want the delete error", err)
	}
	if len(q.calls) != 2 || q.sent == nil {
		t.Errorf("calls = %v, want [send delete]", q.calls)
	}
}

func TestDLQMover_OmitsEmptyReasonAndCode(t *testing.T) {
	q := &fakeQueues{}
	d := sqsin.NewDLQMover(q, "input-url", "dlq-url")

	if err := d.Move(t.Context(), received(), "wallet-1", "", ""); err != nil {
		t.Fatalf("Move: %v", err)
	}

	attrs := q.sent.MessageAttributes
	if _, ok := attrs[sqsin.FailureReasonAttribute]; ok {
		t.Error("empty FailureReason was sent; SQS rejects empty attribute values")
	}
	if _, ok := attrs[sqsin.FailureCodeAttribute]; ok {
		t.Error("empty FailureCode was sent; SQS rejects empty attribute values")
	}
	if _, ok := attrs[sqsin.SourceMessageIDAttribute]; !ok {
		t.Error("SourceMessageId missing")
	}
}

func TestDLQMover_SanitizesReasonAndCode(t *testing.T) {
	q := &fakeQueues{}
	d := sqsin.NewDLQMover(q, "input-url", "dlq-url")
	dirty := "bad\x00key\x1b[31m\xff" + strings.Repeat("é", 2_000)

	if err := d.Move(t.Context(), received(), "wallet-1", dirty, dirty); err != nil {
		t.Fatalf("Move: %v", err)
	}

	for _, name := range []string{sqsin.FailureReasonAttribute, sqsin.FailureCodeAttribute} {
		v := aws.ToString(q.sent.MessageAttributes[name].StringValue)
		if len(v) > 256 {
			t.Errorf("%s is %d bytes, want at most 256", name, len(v))
		}
		if !utf8.ValidString(v) {
			t.Errorf("%s = %q is not valid UTF-8", name, v)
		}
		for _, r := range v {
			if !sqsAccepts(r) {
				t.Errorf("%s contains %U, which SQS rejects", name, r)
			}
			if r < 0x20 {
				t.Errorf("%s contains control character %U", name, r)
			}
		}
		if !strings.HasPrefix(v, "bad") {
			t.Errorf("%s = %q, want the readable part kept", name, v)
		}
	}
}

func sqsAccepts(r rune) bool {
	return r == 0x9 || r == 0xA || r == 0xD || (r >= 0x20 && r <= 0xD7FF) || (r >= 0xE000 && r <= 0xFFFD) || r >= 0x10000
}
