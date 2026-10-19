//go:build integration

package testenv_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

func TestMiniStack_PauseAndUnpause(t *testing.T) {
	ctx := t.Context()
	m, err := testenv.StartMiniStack(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("StartMiniStack: %v", err)
	}
	t.Cleanup(func() { _ = m.Terminate(context.WithoutCancel(ctx)) })
	client := m.RootClient()
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(m.Queues.Input),
		MessageBody:            aws.String("before the pause"),
		MessageGroupId:         aws.String("g"),
		MessageDeduplicationId: aws.String("m-1"),
	})
	if err != nil {
		t.Fatalf("send before the pause: %v", err)
	}

	if err := m.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	paused := true
	t.Cleanup(func() {
		if paused {
			_ = m.Unpause(context.WithoutCancel(ctx))
		}
	})
	const deadline = time.Second
	callCtx, cancel := context.WithTimeout(ctx, deadline)
	started := time.Now()
	_, err = client.GetQueueAttributes(callCtx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(m.Queues.Input)})
	elapsed := time.Since(started)
	cancel()
	if err == nil {
		t.Fatal("a paused MiniStack answered, want the call to hang until its deadline")
	}
	if elapsed < deadline {
		t.Fatalf("the call failed after %s with %v, before its %s deadline: MiniStack refused it instead of hanging", elapsed, err, deadline)
	}

	if err := m.Unpause(ctx); err != nil {
		t.Fatalf("Unpause: %v", err)
	}
	paused = false
	out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(m.Queues.Input),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		t.Fatalf("receive after the pause: %v", err)
	}
	if len(out.Messages) != 1 || aws.ToString(out.Messages[0].Body) != "before the pause" {
		t.Fatalf("received %d messages after the pause, want the one sent before it", len(out.Messages))
	}
}
