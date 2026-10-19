//go:build load

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const emulatorRootKey = "test"

const (
	messageType            = "WagerTransactionRequested"
	providerTokenAttribute = "ProviderToken"
	correlationIDAttribute = "correlationId"
	timestampLayout        = "2006-01-02T15:04:05.000Z"
)

const (
	eventProcessed      = "WagerTransactionProcessed"
	eventRejected       = "WagerTransactionRejected"
	eventPending        = "WagerTransactionPendingReference"
	eventBalanceChanged = "WalletBalanceChanged"
)

func sqsClient(ctx context.Context, profile string) (*sqs.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithSharedConfigProfile(profile))
	if err != nil {
		return nil, fmt.Errorf("AWS configuration of profile %s: %w", profile, err)
	}
	return sqs.NewFromConfig(cfg), nil
}

func rootSQSClient(ctx context.Context) (*sqs.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(emulatorRootKey, emulatorRootKey, "")))
	if err != nil {
		return nil, fmt.Errorf("AWS configuration of the emulator root: %w", err)
	}
	return sqs.NewFromConfig(cfg), nil
}

func queueURL(ctx context.Context, c *sqs.Client, name string) (string, error) {
	out, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", fmt.Errorf("queue %s: %w", name, err)
	}
	return aws.ToString(out.QueueUrl), nil
}

type producer struct {
	queueURL string
	clients  map[string]*sqs.Client
	tokens   map[string]*tokenSource
	sent     atomic.Int64
}

type inputMessage struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt string    `json:"occurredAt"`
	Data       inputData `json:"data"`
}

type inputData struct {
	txBody
	IdempotencyKey string `json:"idempotencyKey"`
}

func stringAttribute(v string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
}

func (p *producer) send(ctx context.Context, body txBody, key, messageID string) (time.Duration, error) {
	token, err := p.tokens[body.ProviderID].Token(ctx)
	if err != nil {
		return 0, err
	}
	msg, err := json.Marshal(inputMessage{
		MessageID:  messageID,
		Type:       messageType,
		OccurredAt: time.Now().UTC().Format(timestampLayout),
		Data:       inputData{txBody: body, IdempotencyKey: key},
	})
	if err != nil {
		return 0, fmt.Errorf("encode message %s: %w", messageID, err)
	}
	started := time.Now()
	_, err = p.clients[body.ProviderID].SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(msg)),
		MessageGroupId:         aws.String(body.WalletID),
		MessageDeduplicationId: aws.String(messageID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			providerTokenAttribute: stringAttribute(token),
			correlationIDAttribute: stringAttribute(messageID),
		},
	})
	took := time.Since(started)
	if err != nil {
		return took, fmt.Errorf("send message %s: %w", messageID, err)
	}
	p.sent.Add(1)
	return took, nil
}

type outcomeEvent struct {
	status        string
	failureCode   string
	transactionID string
	occurredMs    int64
	receivedUs    int64
	events        int
}

type eventTiming struct {
	occurredMs int64
	sentMs     int64
	receivedUs int64
}

type walletEvents struct {
	openings int
	timings  []eventTiming
}

type collector struct {
	client   *sqs.Client
	queueURL string

	mu          sync.Mutex
	seen        map[string]struct{}
	outcomes    map[string]*outcomeEvent
	balances    map[string]int
	pendings    map[string]int
	wallets     map[string]*walletEvents
	redelivered int64
	malformed   int64
	errors      int64
}

func newCollector(client *sqs.Client, url string) *collector {
	return &collector{
		client:   client,
		queueURL: url,
		seen:     map[string]struct{}{},
		outcomes: map[string]*outcomeEvent{},
		balances: map[string]int{},
		pendings: map[string]int{},
		wallets:  map[string]*walletEvents{},
	}
}

type eventEnvelope struct {
	EventID     string `json:"eventId"`
	EventType   string `json:"eventType"`
	AggregateID string `json:"aggregateId"`
	OccurredAt  string `json:"occurredAt"`
	Data        struct {
		TransactionID         string `json:"transactionId"`
		ProviderID            string `json:"providerId"`
		ExternalTransactionID string `json:"externalTransactionId"`
		Kind                  string `json:"kind"`
		FailureCode           string `json:"failureCode"`
	} `json:"data"`
}

func (c *collector) run(ctx context.Context) {
	for ctx.Err() == nil {
		out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(c.queueURL),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             1,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameSentTimestamp},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.countError()
			sleep(ctx, 200*time.Millisecond)
			continue
		}
		receivedUs := time.Now().UnixMicro()
		for _, m := range out.Messages {
			sentMs, _ := strconv.ParseInt(m.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)], 10, 64)
			c.record(aws.ToString(m.Body), sentMs, receivedUs)
		}
		if len(out.Messages) == 0 {
			continue
		}
		entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(out.Messages))
		for i, m := range out.Messages {
			entries = append(entries, types.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: m.ReceiptHandle})
		}
		res, err := c.client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(c.queueURL), Entries: entries})
		if (err != nil && ctx.Err() == nil) || (err == nil && len(res.Failed) > 0) {
			c.countError()
		}
	}
}

func (c *collector) countError() {
	c.mu.Lock()
	c.errors++
	c.mu.Unlock()
}

func (c *collector) record(body string, sentMs, receivedUs int64) {
	var env eventEnvelope
	err := json.Unmarshal([]byte(body), &env)
	occurred, perr := time.Parse(timestampLayout, env.OccurredAt)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil || perr != nil || env.EventID == "" {
		c.malformed++
		return
	}
	if _, ok := c.seen[env.EventID]; ok {
		c.redelivered++
		return
	}
	c.seen[env.EventID] = struct{}{}

	w := c.wallets[env.AggregateID]
	if w == nil {
		w = &walletEvents{}
		c.wallets[env.AggregateID] = w
	}
	occurredMs := occurred.UnixMilli()
	w.timings = append(w.timings, eventTiming{occurredMs: occurredMs, sentMs: sentMs, receivedUs: receivedUs})

	key := idempotencyKey(env.Data.ProviderID, env.Data.ExternalTransactionID)
	switch env.EventType {
	case eventProcessed:
		if env.Data.ExternalTransactionID == "" {
			w.openings++
			return
		}
		c.outcome(key, "PROCESSED", "", env.Data.TransactionID, occurredMs, receivedUs)
	case eventRejected:
		c.outcome(key, "REJECTED", env.Data.FailureCode, env.Data.TransactionID, occurredMs, receivedUs)
	case eventPending:
		c.pendings[key]++
	case eventBalanceChanged:
		c.balances[env.Data.TransactionID]++
	}
}

func (c *collector) outcome(key, status, failure, txID string, occurredMs, receivedUs int64) {
	o := c.outcomes[key]
	if o == nil {
		o = &outcomeEvent{status: status, failureCode: failure, transactionID: txID, occurredMs: occurredMs, receivedUs: receivedUs}
		c.outcomes[key] = o
	}
	o.events++
}

func (c *collector) view(fn func(c *collector)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn(c)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
