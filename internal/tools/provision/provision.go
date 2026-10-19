// Package provision creates the SQS queues and IAM principals of the wallet service.
package provision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	walletAppProfile    = "wallet-app"
	eventsReaderProfile = "events-reader"
)

const (
	providerUserSuffix = "-sqs"
	policyName         = "sqs-access"
	maxAccessKeys      = 2
)

const (
	visibilityTimeoutSeconds   = "30"
	maxMessageRetentionSeconds = "1209600"
)

const (
	minReceiveCount = 1
	maxReceiveCount = 1000
)

var queueAttributes = map[string]string{
	string(sqstypes.QueueAttributeNameContentBasedDeduplication): "false",
	string(sqstypes.QueueAttributeNameVisibilityTimeout):         visibilityTimeoutSeconds,
	string(sqstypes.QueueAttributeNameMessageRetentionPeriod):    maxMessageRetentionSeconds,
}

// DefaultMaxReceiveCount is the maxReceiveCount of the redrive policy of both queues.
const DefaultMaxReceiveCount = 30

var (
	producerActions  = []string{"sqs:SendMessage", "sqs:GetQueueUrl"}
	consumerActions  = []string{"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"}
	publisherActions = []string{"sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"}
	batchActions     = []string{"sqs:SendMessageBatch"}
	renewActions     = []string{"sqs:ChangeMessageVisibilityBatch"}
)

// Options says what to provision and where to write the results.
type Options struct {
	InputQueue, InputDLQ, EventsQueue, EventsDLQ string
	MaxReceiveCount                              int
	Providers                                    []string
	CredentialsPath                              string
}

// Result is what Run provisioned.
type Result struct {
	QueueURLs map[string]string
	Profiles  map[string]Credentials
}

// Credentials is an IAM access key.
type Credentials struct{ AccessKeyID, SecretAccessKey string }

// Run provisions the queues, the redrive and the IAM principals, then writes the credentials file.
func Run(ctx context.Context, sqsClient *sqs.Client, iamClient *iam.Client, opt Options) (Result, error) {
	if err := opt.validate(); err != nil {
		return Result{}, fmt.Errorf("invalid options: %w", err)
	}

	queues, err := ensureQueues(ctx, sqsClient, opt)
	if err != nil {
		return Result{}, err
	}

	res := Result{
		QueueURLs: make(map[string]string, len(queues)),
		Profiles:  make(map[string]Credentials, len(opt.Providers)+2),
	}
	for name, q := range queues {
		res.QueueURLs[name] = q.url
	}

	for _, p := range principals(opt, queues) {
		if err := ensureUser(ctx, iamClient, p.user); err != nil {
			return Result{}, err
		}
		if err := putPolicy(ctx, iamClient, p.user, p.statements); err != nil {
			return Result{}, err
		}
		stored, err := storedCredentials(ctx, opt.CredentialsPath, p.profile)
		if err != nil {
			return Result{}, err
		}
		creds, err := ensureAccessKey(ctx, iamClient, p.user, stored)
		if err != nil {
			return Result{}, err
		}
		res.Profiles[p.profile] = creds
	}

	if err := writeCredentialsFile(opt.CredentialsPath, credentialsFile(res.Profiles)); err != nil {
		return Result{}, err
	}
	return res, nil
}

func (o Options) validate() error {
	var errs []error
	for _, f := range []struct{ name, value string }{
		{"input queue", o.InputQueue}, {"input DLQ", o.InputDLQ},
		{"events queue", o.EventsQueue}, {"events DLQ", o.EventsDLQ},
		{"credentials path", o.CredentialsPath},
	} {
		if f.value == "" {
			errs = append(errs, fmt.Errorf("%s is empty", f.name))
		}
	}
	if o.MaxReceiveCount < minReceiveCount || o.MaxReceiveCount > maxReceiveCount {
		errs = append(errs, fmt.Errorf("max receive count %d is outside %d..%d", o.MaxReceiveCount, minReceiveCount, maxReceiveCount))
	}
	if len(o.Providers) == 0 {
		errs = append(errs, errors.New("no providers"))
	}
	taken := map[string]bool{walletAppProfile: true, eventsReaderProfile: true}
	for _, p := range o.Providers {
		switch {
		case p == "":
			errs = append(errs, errors.New("provider name is empty"))
		case taken[p]:
			errs = append(errs, fmt.Errorf("provider %q is repeated or a reserved profile name", p))
		}
		taken[p] = true
	}
	return errors.Join(errs...)
}

type queue struct{ url, arn string }

func ensureQueues(ctx context.Context, c *sqs.Client, opt Options) (map[string]queue, error) {
	queues := make(map[string]queue, 4)
	for _, pair := range []struct{ source, dlq string }{
		{opt.InputQueue, opt.InputDLQ},
		{opt.EventsQueue, opt.EventsDLQ},
	} {
		dlq, err := ensureQueue(ctx, c, pair.dlq, queueAttributes)
		if err != nil {
			return nil, err
		}
		redrive, err := json.Marshal(struct {
			DeadLetterTargetArn string `json:"deadLetterTargetArn"`
			MaxReceiveCount     string `json:"maxReceiveCount"`
		}{dlq.arn, strconv.Itoa(opt.MaxReceiveCount)})
		if err != nil {
			return nil, fmt.Errorf("encode redrive policy of %s: %w", pair.source, err)
		}
		attrs := maps.Clone(queueAttributes)
		attrs[string(sqstypes.QueueAttributeNameRedrivePolicy)] = string(redrive)
		source, err := ensureQueue(ctx, c, pair.source, attrs)
		if err != nil {
			return nil, err
		}
		queues[pair.dlq], queues[pair.source] = dlq, source
	}
	return queues, nil
}

func ensureQueue(ctx context.Context, c *sqs.Client, name string, attrs map[string]string) (queue, error) {
	created, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: map[string]string{string(sqstypes.QueueAttributeNameFifoQueue): "true"},
	})
	if err != nil {
		return queue{}, fmt.Errorf("create queue %s: %w", name, err)
	}
	q := queue{url: aws.ToString(created.QueueUrl)}
	if _, err := c.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: aws.String(q.url), Attributes: attrs}); err != nil {
		return queue{}, fmt.Errorf("set attributes of queue %s: %w", name, err)
	}
	got, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(q.url),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return queue{}, fmt.Errorf("get ARN of queue %s: %w", name, err)
	}
	q.arn = got.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	if q.arn == "" {
		return queue{}, fmt.Errorf("get ARN of queue %s: empty QueueArn attribute", name)
	}
	return q, nil
}

type principal struct {
	user, profile string
	statements    []statement
}

type statement struct {
	Effect   string
	Action   []string
	Resource []string
}

func allow(actions []string, resources ...string) statement {
	return statement{Effect: "Allow", Action: actions, Resource: resources}
}

func principals(opt Options, queues map[string]queue) []principal {
	input, events := queues[opt.InputQueue].arn, queues[opt.EventsQueue].arn
	ps := []principal{
		{
			user: walletAppProfile, profile: walletAppProfile,
			statements: []statement{
				allow(consumerActions, input),
				allow(renewActions, input),
				allow(publisherActions, queues[opt.InputDLQ].arn, events),
				allow(batchActions, events),
			},
		},
		{
			user: eventsReaderProfile, profile: eventsReaderProfile,
			statements: []statement{allow(consumerActions, events)},
		},
	}
	for _, p := range opt.Providers {
		ps = append(ps, principal{
			user: p + providerUserSuffix, profile: p,
			statements: []statement{allow(producerActions, input)},
		})
	}
	return ps
}

func ensureUser(ctx context.Context, c *iam.Client, name string) error {
	_, err := c.GetUser(ctx, &iam.GetUserInput{UserName: aws.String(name)})
	if err == nil {
		return nil
	}
	var notFound *iamtypes.NoSuchEntityException
	if !errors.As(err, &notFound) {
		return fmt.Errorf("get user %s: %w", name, err)
	}
	if _, err := c.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(name)}); err != nil {
		return fmt.Errorf("create user %s: %w", name, err)
	}
	return nil
}

func putPolicy(ctx context.Context, c *iam.Client, user string, statements []statement) error {
	doc, err := json.Marshal(struct {
		Version   string
		Statement []statement
	}{"2012-10-17", statements})
	if err != nil {
		return fmt.Errorf("encode policy of user %s: %w", user, err)
	}
	if _, err := c.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
		UserName:       aws.String(user),
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(string(doc)),
	}); err != nil {
		return fmt.Errorf("put policy of user %s: %w", user, err)
	}
	return nil
}

func storedCredentials(ctx context.Context, path, profile string) (Credentials, error) {
	f, err := os.Open(filepath.Clean(path))
	if errors.Is(err, fs.ErrNotExist) {
		return Credentials{}, nil
	}
	if err != nil {
		return Credentials{}, fmt.Errorf("read credentials file: %w", err)
	}
	_ = f.Close()

	shared, err := config.LoadSharedConfigProfile(ctx, profile, func(o *config.LoadSharedConfigOptions) {
		o.CredentialsFiles = []string{path}
		o.ConfigFiles = []string{}
	})
	var notExist config.SharedConfigProfileNotExistError
	if errors.As(err, &notExist) {
		return Credentials{}, nil
	}
	if err != nil {
		return Credentials{}, fmt.Errorf("read profile %s from %s: %w", profile, path, err)
	}
	return Credentials{AccessKeyID: shared.Credentials.AccessKeyID, SecretAccessKey: shared.Credentials.SecretAccessKey}, nil
}

func ensureAccessKey(ctx context.Context, c *iam.Client, user string, stored Credentials) (Credentials, error) {
	listed, err := c.ListAccessKeys(ctx, &iam.ListAccessKeysInput{UserName: aws.String(user)})
	if err != nil {
		return Credentials{}, fmt.Errorf("list access keys of user %s: %w", user, err)
	}
	keys := listed.AccessKeyMetadata
	if stored.AccessKeyID != "" && stored.SecretAccessKey != "" && slices.ContainsFunc(keys, func(k iamtypes.AccessKeyMetadata) bool {
		return aws.ToString(k.AccessKeyId) == stored.AccessKeyID && k.Status == iamtypes.StatusTypeActive
	}) {
		return stored, nil
	}

	slices.SortFunc(keys, func(a, b iamtypes.AccessKeyMetadata) int {
		return aws.ToTime(a.CreateDate).Compare(aws.ToTime(b.CreateDate))
	})
	for ; len(keys) >= maxAccessKeys; keys = keys[1:] {
		if _, err := c.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{
			UserName:    aws.String(user),
			AccessKeyId: keys[0].AccessKeyId,
		}); err != nil {
			return Credentials{}, fmt.Errorf("delete stale access key of user %s: %w", user, err)
		}
	}

	created, err := c.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String(user)})
	if err != nil {
		return Credentials{}, fmt.Errorf("create access key of user %s: %w", user, err)
	}
	return Credentials{
		AccessKeyID:     aws.ToString(created.AccessKey.AccessKeyId),
		SecretAccessKey: aws.ToString(created.AccessKey.SecretAccessKey),
	}, nil
}

func credentialsFile(profiles map[string]Credentials) []byte {
	var b bytes.Buffer
	for i, name := range slices.Sorted(maps.Keys(profiles)) {
		if i > 0 {
			b.WriteByte('\n')
		}
		c := profiles[name]
		fmt.Fprintf(&b, "[%s]\naws_access_key_id = %s\naws_secret_access_key = %s\n", name, c.AccessKeyID, c.SecretAccessKey)
	}
	return b.Bytes()
}

func writeCredentialsFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create credentials directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("write credentials file: %w", err)
	}
	_, err = tmp.Write(data)
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("write credentials file: %w", err)
	}
	return nil
}
