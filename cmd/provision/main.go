// Command provision creates the SQS queues and IAM principals of the wallet service.
package main

import (
	"context"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Brunohvc/backend-challenge-go/internal/tools/provision"
)

const timeout = 2 * time.Minute

func main() {
	opt := provision.Options{Providers: []string{"provider-a", "provider-b"}}
	flag.StringVar(&opt.CredentialsPath, "credentials", "", "AWS shared credentials file to write (required)")
	flag.StringVar(&opt.InputQueue, "input-queue", "wager-transactions.fifo", "queue providers send wager transactions to")
	flag.StringVar(&opt.InputDLQ, "input-dlq", "wager-transactions-dlq.fifo", "dead-letter queue of the input queue")
	flag.StringVar(&opt.EventsQueue, "events-queue", "wallet-events.fifo", "queue the wallet service publishes events to")
	flag.StringVar(&opt.EventsDLQ, "events-dlq", "wallet-events-dlq.fifo", "dead-letter queue of the events queue")
	flag.IntVar(&opt.MaxReceiveCount, "max-receive-count", provision.DefaultMaxReceiveCount,
		"receives before SQS moves a message to its dead-letter queue, a net under the consumer's own limit")
	flag.Parse()

	if opt.CredentialsPath == "" || flag.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "provision: -credentials is required and no arguments are accepted")
		flag.Usage()
		os.Exit(2)
	}

	if err := run(opt); err != nil {
		fmt.Fprintf(os.Stderr, "provision: %v\n", err)
		os.Exit(1)
	}
}

func run(opt provision.Options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS configuration: %w", err)
	}
	res, err := provision.Run(ctx, sqs.NewFromConfig(cfg), iam.NewFromConfig(cfg), opt)
	if err != nil {
		return err
	}

	for _, name := range slices.Sorted(maps.Keys(res.QueueURLs)) {
		fmt.Printf("queue   %-28s %s\n", name, res.QueueURLs[name])
	}
	for _, name := range slices.Sorted(maps.Keys(res.Profiles)) {
		fmt.Printf("profile %-28s %s\n", name, res.Profiles[name].AccessKeyID)
	}
	fmt.Printf("wrote %s\n", opt.CredentialsPath)
	return nil
}
