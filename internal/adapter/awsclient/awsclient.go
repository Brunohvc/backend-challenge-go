// Package awsclient builds the AWS configuration and resolves the URLs of the SQS queues.
package awsclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

// LoadConfig loads the AWS configuration from the environment the SDK reads.
func LoadConfig(ctx context.Context) (aws.Config, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return aws.Config{}, fmt.Errorf("awsclient: load AWS configuration: %w", err)
	}
	return cfg, nil
}

// Queues holds the URLs of the queues the service uses.
type Queues struct {
	Input    string
	InputDLQ string
	Events   string
}

// ResolveQueues looks up the URL of every queue in cfg.
func ResolveQueues(ctx context.Context, c *sqs.Client, cfg config.SQSConfig) (Queues, error) {
	var errs []error
	resolve := func(name string) string {
		out, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			return ""
		}
		return aws.ToString(out.QueueUrl)
	}
	q := Queues{
		Input:    resolve(cfg.InputQueue),
		InputDLQ: resolve(cfg.InputDLQ),
		Events:   resolve(cfg.EventsQueue),
	}
	if err := errors.Join(errs...); err != nil {
		return Queues{}, fmt.Errorf("awsclient: resolve queues: %w", err)
	}
	return q, nil
}
