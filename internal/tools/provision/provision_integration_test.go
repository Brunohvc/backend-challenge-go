//go:build integration

package provision_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	awshttp "github.com/aws/smithy-go/transport/http"

	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var ms *testenv.MiniStack

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "provision-it-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "temp dir: %v\n", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	ms, err = testenv.StartMiniStack(ctx, dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start MiniStack: %v\n", err)
		return 1
	}
	defer func() { _ = ms.Terminate(ctx) }()
	return m.Run()
}

func TestProvision_Idempotent(t *testing.T) {
	first, err := ms.Provision(t.Context())
	if err != nil {
		t.Fatalf("first Provision: %v", err)
	}
	before, err := os.ReadFile(ms.CredentialsFile)
	if err != nil {
		t.Fatalf("read credentials file: %v", err)
	}

	second, err := ms.Provision(t.Context())
	if err != nil {
		t.Fatalf("second Provision: %v", err)
	}

	for name, url := range first.QueueURLs {
		if second.QueueURLs[name] != url {
			t.Errorf("queue %s URL = %q, want %q", name, second.QueueURLs[name], url)
		}
	}
	for name, creds := range first.Profiles {
		if second.Profiles[name].AccessKeyID != creds.AccessKeyID {
			t.Errorf("profile %s key = %s, want the reused %s", name, second.Profiles[name].AccessKeyID, creds.AccessKeyID)
		}
	}
	after, err := os.ReadFile(ms.CredentialsFile)
	if err != nil {
		t.Fatalf("read credentials file: %v", err)
	}
	if string(after) != string(before) {
		t.Error("credentials file changed on a run that should reuse every key")
	}
	for profile, queue := range map[string]string{
		"wallet-app":    "wager-transactions.fifo",
		"events-reader": "wallet-events.fifo",
		"provider-a":    "wager-transactions.fifo",
		"provider-b":    "wager-transactions.fifo",
	} {
		c, err := ms.Client(t.Context(), profile)
		if err != nil {
			t.Fatalf("client %s: %v", profile, err)
		}
		if _, err := c.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String(queue)}); err != nil {
			t.Errorf("profile %s GetQueueUrl %s: %v", profile, queue, err)
		}
	}
}

func TestProvision_RedriveConfigured(t *testing.T) {
	root := ms.RootClient()

	tests := []struct{ source, dlq string }{
		{"wager-transactions.fifo", "wager-transactions-dlq.fifo"},
		{"wallet-events.fifo", "wallet-events-dlq.fifo"},
	}
	for _, tt := range tests {
		t.Run(tt.source, func(t *testing.T) {
			url, err := root.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String(tt.source)})
			if err != nil {
				t.Fatalf("GetQueueUrl: %v", err)
			}
			got, err := root.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
				QueueUrl:       url.QueueUrl,
				AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameRedrivePolicy},
			})
			if err != nil {
				t.Fatalf("GetQueueAttributes: %v", err)
			}
			var policy struct {
				DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
				MaxReceiveCount     json.Number `json:"maxReceiveCount"`
			}
			if err := json.Unmarshal([]byte(got.Attributes[string(sqstypes.QueueAttributeNameRedrivePolicy)]), &policy); err != nil {
				t.Fatalf("decode RedrivePolicy %q: %v", got.Attributes[string(sqstypes.QueueAttributeNameRedrivePolicy)], err)
			}

			if policy.MaxReceiveCount.String() != "30" {
				t.Errorf("maxReceiveCount = %s, want 30", policy.MaxReceiveCount)
			}
			if !strings.HasSuffix(policy.DeadLetterTargetArn, ":"+tt.dlq) {
				t.Errorf("deadLetterTargetArn = %s, want the ARN of %s", policy.DeadLetterTargetArn, tt.dlq)
			}
		})
	}
}

func TestSQS_UnauthorizedPrincipalDenied(t *testing.T) {
	provider, err := ms.Client(t.Context(), "provider-a")
	if err != nil {
		t.Fatalf("client provider-a: %v", err)
	}
	input := aws.String(ms.Queues.Input)

	if _, err := provider.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:               input,
		MessageBody:            aws.String(`{"probe":"aut-10"}`),
		MessageGroupId:         aws.String("aut-10"),
		MessageDeduplicationId: aws.String("aut-10-send"),
	}); err != nil {
		t.Fatalf("provider SendMessage: %v", err)
	}

	t.Run("provider cannot receive", func(t *testing.T) {
		_, err := provider.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: input})

		assertDenied(t, err, "AccessDeniedException", "AccessDenied")
	})

	t.Run("bogus key cannot send", func(t *testing.T) {
		bogus := sqs.New(sqs.Options{
			Region:       "us-east-1",
			BaseEndpoint: aws.String(ms.Endpoint),
			Credentials:  credentials.NewStaticCredentialsProvider("AKIABOGUSBOGUSBOGUS0", "bogus-secret", ""),
		})

		_, err := bogus.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl:               input,
			MessageBody:            aws.String(`{"probe":"aut-10"}`),
			MessageGroupId:         aws.String("aut-10"),
			MessageDeduplicationId: aws.String("aut-10-bogus"),
		})

		assertDenied(t, err, "InvalidClientTokenId")
	})
}

func TestSQS_WalletAppPublishesEventBatches(t *testing.T) {
	walletApp, err := ms.Client(t.Context(), "wallet-app")
	if err != nil {
		t.Fatalf("client wallet-app: %v", err)
	}

	out, err := walletApp.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(ms.Queues.Events),
		Entries: []sqstypes.SendMessageBatchRequestEntry{{
			Id:                     aws.String("probe"),
			MessageBody:            aws.String(`{"probe":"event-batch"}`),
			MessageGroupId:         aws.String("event-batch"),
			MessageDeduplicationId: aws.String("event-batch"),
		}},
	})

	if err != nil {
		t.Fatalf("wallet-app SendMessageBatch to the events queue: %v", err)
	}
	if len(out.Failed) != 0 || len(out.Successful) != 1 {
		t.Errorf("SendMessageBatch = %d sent, %d failed (%+v); want the one entry sent", len(out.Successful), len(out.Failed), out.Failed)
	}
}

func TestSQS_WalletAppSendsNoBatchToTheInputDLQ(t *testing.T) {
	walletApp, err := ms.Client(t.Context(), "wallet-app")
	if err != nil {
		t.Fatalf("client wallet-app: %v", err)
	}
	dlq := aws.String(ms.Queues.InputDLQ)

	if _, err := walletApp.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:               dlq,
		MessageBody:            aws.String(`{"probe":"dlq-single"}`),
		MessageGroupId:         aws.String("dlq-single"),
		MessageDeduplicationId: aws.String("dlq-single"),
	}); err != nil {
		t.Fatalf("wallet-app SendMessage to the input DLQ: %v", err)
	}

	_, err = walletApp.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{
		QueueUrl: dlq,
		Entries: []sqstypes.SendMessageBatchRequestEntry{{
			Id:                     aws.String("probe"),
			MessageBody:            aws.String(`{"probe":"dlq-batch"}`),
			MessageGroupId:         aws.String("dlq-batch"),
			MessageDeduplicationId: aws.String("dlq-batch"),
		}},
	})

	assertDenied(t, err, "AccessDeniedException", "AccessDenied")
}

func assertDenied(t *testing.T, err error, codes ...string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want an API error with code %v", err, codes)
	}
	var respErr *awshttp.ResponseError
	if !errors.As(err, &respErr) || respErr.HTTPStatusCode() != http.StatusForbidden {
		t.Errorf("err = %v, want HTTP 403", err)
	}
	for _, c := range codes {
		if apiErr.ErrorCode() == c {
			return
		}
	}
	t.Errorf("error code = %s, want one of %v", apiErr.ErrorCode(), codes)
}
