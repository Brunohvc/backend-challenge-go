package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/awsclient"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/health"
)

var awsModule = fx.Module("aws",
	fx.Provide(newQueues),
	fx.Invoke(registerQueues),
)

var errQueuesNotStarted = errors.New("bootstrap: sqs queues not started")

type queues struct {
	names config.SQSConfig
	state atomic.Pointer[queueState]
}

type queueState struct {
	client *sqs.Client
	urls   awsclient.Queues
}

func newQueues(cfg config.Config) *queues {
	return &queues{names: cfg.SQS}
}

func (q *queues) start(ctx context.Context) error {
	awsCfg, err := awsclient.LoadConfig(ctx)
	if err != nil {
		return err
	}
	client := sqs.NewFromConfig(awsCfg)
	urls, err := awsclient.ResolveQueues(ctx, client, q.names)
	if err != nil {
		return err
	}
	q.state.Store(&queueState{client: client, urls: urls})
	return nil
}

func (q *queues) stop(context.Context) error {
	q.state.Store(nil)
	return nil
}

func (q *queues) started() (*queueState, error) {
	s := q.state.Load()
	if s == nil {
		return nil, errQueuesNotStarted
	}
	return s, nil
}

func (q *queues) checkInput(ctx context.Context) error {
	s, err := q.started()
	if err != nil {
		return err
	}
	_, err = s.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(s.urls.Input)})
	if err != nil {
		return fmt.Errorf("sqs: input queue: %w", err)
	}
	return nil
}

func registerQueues(l lifecycle, q *queues, checker *health.Checker) {
	checker.Add("sqs", q.checkInput)
	l.add(component{name: "sqs", start: q.start, stop: q.stop})
}
