package sqsin

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
)

// ProviderTokenAttribute is the message attribute carrying the access token of the provider.
const ProviderTokenAttribute = "ProviderToken"

// The errors of TokenAttributeIdentity.
var (
	ErrMissingProviderToken = errors.New("sqsin: message has no ProviderToken attribute")
	ErrMissingSentTimestamp = errors.New("sqsin: message has no valid SentTimestamp")
	ErrInvalidProviderToken = errors.New("sqsin: provider token rejected")
	ErrNotProvider          = errors.New("sqsin: token is not a provider authorized to write")
)

// IdentityResolver tells which provider sent a message.
type IdentityResolver interface {
	ProviderFor(ctx context.Context, msg types.Message) (string, error)
}

// TokenVerifier verifies an access token as if at a given instant.
type TokenVerifier interface {
	VerifyAt(ctx context.Context, raw string, at time.Time) (oidc.Principal, error)
}

// TokenAttributeIdentity identifies the producer by the access token in its message attribute.
type TokenAttributeIdentity struct {
	Verifier TokenVerifier
}

// ProviderFor returns the ProviderID of the token carried by msg.
func (id TokenAttributeIdentity) ProviderFor(ctx context.Context, msg types.Message) (string, error) {
	attr, ok := msg.MessageAttributes[ProviderTokenAttribute]
	if !ok || aws.ToString(attr.DataType) != "String" || aws.ToString(attr.StringValue) == "" {
		return "", ErrMissingProviderToken
	}
	sentAt, err := sentTimestamp(msg)
	if err != nil {
		return "", err
	}

	p, err := id.Verifier.VerifyAt(ctx, aws.ToString(attr.StringValue), sentAt)
	switch {
	case errors.Is(err, oidc.ErrUnknownKey):
		return "", fmt.Errorf("sqsin: provider token signed with a key not fetched yet: %w", err)
	case errors.Is(err, oidc.ErrInvalidToken), errors.Is(err, oidc.ErrExpiredToken), errors.Is(err, oidc.ErrMisconfiguredPrincipal):
		return "", fmt.Errorf("%w: %w", ErrInvalidProviderToken, err)
	case err != nil:
		return "", fmt.Errorf("sqsin: verify provider token: %w", err)
	}
	if !p.IsProvider() || !p.Has(oidc.ScopeWageringWrite) {
		return "", ErrNotProvider
	}
	return p.ProviderID, nil
}

func sentTimestamp(msg types.Message) (time.Time, error) {
	raw, ok := msg.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)]
	if !ok {
		return time.Time{}, ErrMissingSentTimestamp
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{}, ErrMissingSentTimestamp
	}
	return time.UnixMilli(ms).UTC(), nil
}
