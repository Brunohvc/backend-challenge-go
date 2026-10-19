//go:build integration

package sqsin_test

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

func TestDLQMover_MiniStack(t *testing.T) {
	dir, err := os.MkdirTemp("", "sqsin-it-")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	ms, err := testenv.StartMiniStack(t.Context(), dir)
	if err != nil {
		t.Fatalf("StartMiniStack: %v", err)
	}
	t.Cleanup(func() { _ = ms.Terminate(t.Context()) })
	provider, err := ms.Client(t.Context(), "provider-a")
	if err != nil {
		t.Fatalf("provider client: %v", err)
	}
	app, err := ms.Client(t.Context(), "wallet-app")
	if err != nil {
		t.Fatalf("wallet-app client: %v", err)
	}
	if _, err := provider.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(ms.Queues.Input),
		MessageBody:            aws.String(exampleBody),
		MessageGroupId:         aws.String("wallet-1"),
		MessageDeduplicationId: aws.String("provider-a:transaction-123"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			sqsin.CorrelationIDAttribute: {DataType: aws.String("String"), StringValue: aws.String("corr-1")},
			sqsin.ProviderTokenAttribute: {DataType: aws.String("String"), StringValue: aws.String("a-token")},
		},
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	got, err := app.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(ms.Queues.Input),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		MessageAttributeNames:       []string{"All"},
		WaitTimeSeconds:             1,
	})
	if err != nil || len(got.Messages) != 1 {
		t.Fatalf("ReceiveMessage = %v, %v; want one message", got, err)
	}
	msg := got.Messages[0]

	err = sqsin.NewDLQMover(app, ms.Queues.Input, ms.Queues.InputDLQ).Move(t.Context(), msg, "wallet-1", "invalid message", "INVALID_MESSAGE")
	if err != nil {
		t.Fatalf("Move: %v", err)
	}

	moved, err := ms.RootClient().ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(ms.Queues.InputDLQ),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		MessageAttributeNames:       []string{"All"},
		WaitTimeSeconds:             1,
	})
	if err != nil || len(moved.Messages) != 1 {
		t.Fatalf("DLQ ReceiveMessage = %v, %v; want one message", moved, err)
	}
	m := moved.Messages[0]
	if aws.ToString(m.Body) != exampleBody {
		t.Errorf("DLQ body = %q, want the original body", aws.ToString(m.Body))
	}
	if g := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]; g != "wallet-1" {
		t.Errorf("DLQ MessageGroupId = %q, want wallet-1", g)
	}
	if d := m.Attributes[string(types.MessageSystemAttributeNameMessageDeduplicationId)]; d != aws.ToString(msg.MessageId) {
		t.Errorf("DLQ MessageDeduplicationId = %q, want the source MessageId %s", d, aws.ToString(msg.MessageId))
	}
	for name, want := range map[string]string{
		sqsin.FailureReasonAttribute:   "invalid message",
		sqsin.FailureCodeAttribute:     "INVALID_MESSAGE",
		sqsin.SourceMessageIDAttribute: aws.ToString(msg.MessageId),
		sqsin.CorrelationIDAttribute:   "corr-1",
	} {
		if v := aws.ToString(m.MessageAttributes[name].StringValue); v != want {
			t.Errorf("DLQ attribute %s = %q, want %q", name, v, want)
		}
	}
	if _, ok := m.MessageAttributes[sqsin.ProviderTokenAttribute]; ok {
		t.Error("the provider token was copied to the DLQ")
	}
	attrs, err := app.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(ms.Queues.Input),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes: %v", err)
	}
	for _, n := range []types.QueueAttributeName{
		types.QueueAttributeNameApproximateNumberOfMessages,
		types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
	} {
		if v := attrs.Attributes[string(n)]; v != "0" {
			t.Errorf("input queue %s = %s, want 0", n, v)
		}
	}
}
