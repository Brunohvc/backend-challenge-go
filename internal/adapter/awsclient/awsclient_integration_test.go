//go:build integration

package awsclient_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/awsclient"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var ms *testenv.MiniStack

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "awsclient-it-")
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

var defaultQueues = config.SQSConfig{
	InputQueue:  "wager-transactions.fifo",
	InputDLQ:    "wager-transactions-dlq.fifo",
	EventsQueue: "wallet-events.fifo",
}

func TestLoadConfig_FromEnvironment(t *testing.T) {
	emptyConfig := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(emptyConfig, nil, 0o600); err != nil {
		t.Fatalf("write empty config: %v", err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_CONFIG_FILE", emptyConfig)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ENDPOINT_URL", ms.Endpoint)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", ms.CredentialsFile)
	t.Setenv("AWS_PROFILE", "wallet-app")

	cfg, err := awsclient.LoadConfig(t.Context())
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	got, err := awsclient.ResolveQueues(t.Context(), sqs.NewFromConfig(cfg), defaultQueues)
	if err != nil {
		t.Fatalf("ResolveQueues: %v", err)
	}

	if got != ms.Queues {
		t.Errorf("queues = %+v, want %+v", got, ms.Queues)
	}
	creds, err := cfg.Credentials.Retrieve(t.Context())
	if err != nil {
		t.Fatalf("retrieve credentials: %v", err)
	}
	if creds.AccessKeyID == "" || creds.AccessKeyID == "test" {
		t.Errorf("access key = %q, want the wallet-app key from the credentials file", creds.AccessKeyID)
	}
}

func TestResolveQueues(t *testing.T) {
	c, err := ms.Client(t.Context(), "wallet-app")
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	got, err := awsclient.ResolveQueues(t.Context(), c, defaultQueues)
	if err != nil {
		t.Fatalf("ResolveQueues: %v", err)
	}

	for name, url := range map[string]string{
		defaultQueues.InputQueue:  got.Input,
		defaultQueues.InputDLQ:    got.InputDLQ,
		defaultQueues.EventsQueue: got.Events,
	} {
		if !strings.HasSuffix(url, "/"+name) {
			t.Errorf("URL of %s = %q", name, url)
		}
	}
}

func TestResolveQueues_MissingQueueFails(t *testing.T) {
	c, err := ms.Client(t.Context(), "wallet-app")
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	cfg := defaultQueues
	cfg.InputDLQ = "missing-dlq.fifo"
	cfg.EventsQueue = "missing-events.fifo"

	_, err = awsclient.ResolveQueues(t.Context(), c, cfg)

	if err == nil {
		t.Fatal("ResolveQueues succeeded with missing queues, want an error")
	}
	for _, name := range []string{cfg.InputDLQ, cfg.EventsQueue} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
	if strings.Contains(err.Error(), cfg.InputQueue+":") {
		t.Errorf("error %q names %s, which exists", err, cfg.InputQueue)
	}
}
