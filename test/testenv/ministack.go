//go:build integration || e2e

package testenv

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/awsclient"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/tools/provision"
)

// AWSRegion is the region every client of the tests uses.
const AWSRegion = "us-east-1"

const (
	miniStackImage        = "ministackorg/ministack:1.5.27"
	miniStackPort         = "4566/tcp"
	miniStackStartTimeout = 2 * time.Minute
	rootKey, rootSecret   = "test", "test"
)

var queueNames = config.SQSConfig{
	InputQueue:  "wager-transactions.fifo",
	InputDLQ:    "wager-transactions-dlq.fifo",
	EventsQueue: "wallet-events.fifo",
}

// MiniStack is a running, provisioned MiniStack with IAM enforcement on.
type MiniStack struct {
	Endpoint        string
	CredentialsFile string
	Queues          awsclient.Queues
	Container       testcontainers.Container
}

// StartMiniStack starts and provisions MiniStack, writing the credentials file into dir.
func StartMiniStack(ctx context.Context, dir string) (*MiniStack, error) {
	c, err := testcontainers.Run(ctx, miniStackImage,
		testcontainers.WithEnv(map[string]string{"AUTH": "true"}),
		testcontainers.WithExposedPorts(miniStackPort),
		testcontainers.WithWaitStrategyAndDeadline(miniStackStartTimeout,
			wait.ForHTTP("/_ministack/health").WithPort(miniStackPort).WithStartupTimeout(miniStackStartTimeout),
		),
	)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("testenv: start MiniStack: %w", err), testcontainers.TerminateContainer(c))
	}
	m := &MiniStack{CredentialsFile: filepath.Join(dir, "credentials"), Container: c}
	if m.Endpoint, err = c.PortEndpoint(ctx, miniStackPort, "http"); err != nil {
		return nil, errors.Join(fmt.Errorf("testenv: MiniStack endpoint: %w", err), m.Terminate(ctx))
	}
	if _, err := m.Provision(ctx); err != nil {
		return nil, errors.Join(err, m.Terminate(ctx))
	}
	if m.Queues, err = awsclient.ResolveQueues(ctx, m.RootClient(), queueNames); err != nil {
		return nil, errors.Join(fmt.Errorf("testenv: %w", err), m.Terminate(ctx))
	}
	return m, nil
}

// Provision runs the provisioning program against MiniStack as root.
func (m *MiniStack) Provision(ctx context.Context) (provision.Result, error) {
	res, err := provision.Run(ctx, m.RootClient(), m.rootIAM(), provision.Options{
		InputQueue:      queueNames.InputQueue,
		InputDLQ:        queueNames.InputDLQ,
		EventsQueue:     queueNames.EventsQueue,
		EventsDLQ:       "wallet-events-dlq.fifo",
		MaxReceiveCount: provision.DefaultMaxReceiveCount,
		Providers:       []string{"provider-a", "provider-b"},
		CredentialsPath: m.CredentialsFile,
	})
	if err != nil {
		return provision.Result{}, fmt.Errorf("testenv: provision MiniStack: %w", err)
	}
	return res, nil
}

// RootClient returns an SQS client acting as MiniStack's root.
func (m *MiniStack) RootClient() *sqs.Client {
	return sqs.New(sqs.Options{
		Region:       AWSRegion,
		BaseEndpoint: aws.String(m.Endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(rootKey, rootSecret, ""),
	})
}

func (m *MiniStack) rootIAM() *iam.Client {
	return iam.New(iam.Options{
		Region:       AWSRegion,
		BaseEndpoint: aws.String(m.Endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(rootKey, rootSecret, ""),
	})
}

// Client returns an SQS client signing with the key of profile from the credentials file.
func (m *MiniStack) Client(ctx context.Context, profile string) (*sqs.Client, error) {
	shared, err := awsconfig.LoadSharedConfigProfile(ctx, profile, func(o *awsconfig.LoadSharedConfigOptions) {
		o.CredentialsFiles = []string{m.CredentialsFile}
		o.ConfigFiles = []string{}
	})
	if err != nil {
		return nil, fmt.Errorf("testenv: read profile %s: %w", profile, err)
	}
	return sqs.New(sqs.Options{
		Region:       AWSRegion,
		BaseEndpoint: aws.String(m.Endpoint),
		Credentials:  credentials.StaticCredentialsProvider{Value: shared.Credentials},
	}), nil
}

// Pause freezes MiniStack until Unpause.
func (m *MiniStack) Pause(ctx context.Context) error {
	return pause(ctx, "MiniStack", m.Container)
}

// Unpause lets a MiniStack that Pause froze run again.
func (m *MiniStack) Unpause(ctx context.Context) error {
	return unpause(ctx, "MiniStack", m.Container)
}

// Terminate stops and removes the container.
func (m *MiniStack) Terminate(context.Context) error {
	if err := testcontainers.TerminateContainer(m.Container); err != nil {
		return fmt.Errorf("testenv: terminate MiniStack: %w", err)
	}
	return nil
}
