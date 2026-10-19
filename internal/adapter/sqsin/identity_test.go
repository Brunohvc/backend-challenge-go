package sqsin_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/golang-jwt/jwt/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

const (
	testIssuer = "http://keycloak.test/realms/wagering"
	testKID    = "test-key"
)

var sentAt = time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)

type tokenIssuer struct {
	key      *rsa.PrivateKey
	verifier *oidc.Verifier
	down     atomic.Bool
}

func newTokenIssuer(t *testing.T) *tokenIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	jwks, err := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": testKID,
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}})
	if err != nil {
		t.Fatalf("marshal JWKS: %v", err)
	}
	ti := &tokenIssuer{key: key}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if ti.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write(jwks)
	}))
	t.Cleanup(srv.Close)

	v := oidc.NewVerifier(config.OIDCConfig{Issuer: testIssuer, JWKSURL: srv.URL, Audience: "wagering-api", ClockSkew: 30 * time.Second}, nil)
	if err := v.Start(t.Context()); err != nil {
		t.Fatalf("verifier Start: %v", err)
	}
	t.Cleanup(func() { _ = v.Stop(t.Context()) })
	ti.verifier = v
	return ti
}

func (ti *tokenIssuer) token(t *testing.T, client string, exp time.Time) string {
	t.Helper()
	return ti.tokenWith(t, client, exp, func(jwt.MapClaims) {})
}

func (ti *tokenIssuer) tokenWith(t *testing.T, client string, exp time.Time, edit func(jwt.MapClaims)) string {
	t.Helper()
	claims := jwt.MapClaims{
		"iss": testIssuer, "aud": "wagering-api", "sub": "service-account-" + client, "azp": client,
		"iat": exp.Add(-5 * time.Minute).Unix(), "exp": exp.Unix(),
		"scope": "wagering:write wagering:read",
	}
	if client == "wallet-service" {
		claims["scope"] = "wallets:manage"
	} else {
		claims["provider_id"] = client
	}
	edit(claims)
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = testKID
	raw, err := tok.SignedString(ti.key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return raw
}

func message(token, sent string) types.Message {
	msg := types.Message{MessageId: aws.String("m-1"), Body: aws.String(exampleBody), Attributes: map[string]string{}}
	if token != "" {
		msg.MessageAttributes = map[string]types.MessageAttributeValue{
			sqsin.ProviderTokenAttribute: {DataType: aws.String("String"), StringValue: aws.String(token)},
		}
	}
	if sent != "" {
		msg.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)] = sent
	}
	return msg
}

func millis(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}

func TestTokenAttributeIdentity(t *testing.T) {
	ti := newTokenIssuer(t)
	id := sqsin.TokenAttributeIdentity{Verifier: ti.verifier}
	valid := ti.token(t, "provider-a", sentAt.Add(5*time.Minute))

	tests := []struct {
		name         string
		msg          types.Message
		wantProvider string
		wantErr      []error
	}{
		{
			name:         "valid at SentTimestamp",
			msg:          message(valid, millis(sentAt)),
			wantProvider: "provider-a",
		},
		{
			name:    "no attribute",
			msg:     message("", millis(sentAt)),
			wantErr: []error{sqsin.ErrMissingProviderToken},
		},
		{
			name: "binary attribute",
			msg: func() types.Message {
				m := message(valid, millis(sentAt))
				m.MessageAttributes[sqsin.ProviderTokenAttribute] = types.MessageAttributeValue{DataType: aws.String("Binary"), BinaryValue: []byte(valid)}
				return m
			}(),
			wantErr: []error{sqsin.ErrMissingProviderToken},
		},
		{
			name:    "expired at SentTimestamp",
			msg:     message(ti.token(t, "provider-a", sentAt.Add(-time.Minute)), millis(sentAt)),
			wantErr: []error{sqsin.ErrInvalidProviderToken, oidc.ErrExpiredToken},
		},
		{
			name:    "token of the internal client",
			msg:     message(ti.token(t, "wallet-service", sentAt.Add(5*time.Minute)), millis(sentAt)),
			wantErr: []error{sqsin.ErrNotProvider},
		},
		{
			name: "provider without wagering:write",
			msg: message(ti.tokenWith(t, "provider-a", sentAt.Add(5*time.Minute), func(c jwt.MapClaims) {
				c["scope"] = "wagering:read"
			}), millis(sentAt)),
			wantErr: []error{sqsin.ErrNotProvider},
		},
		{
			name: "token with no role",
			msg: message(ti.tokenWith(t, "provider-a", sentAt.Add(5*time.Minute), func(c jwt.MapClaims) {
				delete(c, "provider_id")
			}), millis(sentAt)),
			wantErr: []error{sqsin.ErrInvalidProviderToken, oidc.ErrMisconfiguredPrincipal},
		},
		{
			name:    "forged token",
			msg:     message(valid+"x", millis(sentAt)),
			wantErr: []error{sqsin.ErrInvalidProviderToken, oidc.ErrInvalidToken},
		},
		{
			name:    "no SentTimestamp",
			msg:     message(valid, ""),
			wantErr: []error{sqsin.ErrMissingSentTimestamp},
		},
		{
			name:    "SentTimestamp not a number",
			msg:     message(valid, "yesterday"),
			wantErr: []error{sqsin.ErrMissingSentTimestamp},
		},
		{
			name:    "SentTimestamp zero",
			msg:     message(valid, "0"),
			wantErr: []error{sqsin.ErrMissingSentTimestamp},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := id.ProviderFor(t.Context(), tt.msg)

			for _, want := range tt.wantErr {
				if !errors.Is(err, want) {
					t.Errorf("err = %v, want it to match %v", err, want)
				}
			}
			if tt.wantErr == nil && err != nil {
				t.Fatalf("ProviderFor: %v", err)
			}
			if got != tt.wantProvider {
				t.Errorf("provider = %q, want %q", got, tt.wantProvider)
			}
		})
	}
}

type fakeVerifier struct{ err error }

func (f fakeVerifier) VerifyAt(context.Context, string, time.Time) (oidc.Principal, error) {
	return oidc.Principal{}, f.err
}

func TestTokenAttributeIdentity_VerifierFailures(t *testing.T) {
	boom := errors.New("boom")

	tests := []struct {
		name          string
		err           error
		wantPermanent bool
	}{
		{"invalid token", fmt.Errorf("wrapped: %w", oidc.ErrInvalidToken), true},
		{"expired token", oidc.ErrExpiredToken, true},
		{"misconfigured principal", oidc.ErrMisconfiguredPrincipal, true},
		{"unknown signing key", fmt.Errorf("%w: %w", oidc.ErrInvalidToken, oidc.ErrUnknownKey), false},
		{"verifier failure", boom, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := sqsin.TokenAttributeIdentity{Verifier: fakeVerifier{err: tt.err}}

			_, err := id.ProviderFor(t.Context(), message("token", millis(sentAt)))

			if !errors.Is(err, tt.err) {
				t.Errorf("err = %v, want it to wrap %v", err, tt.err)
			}
			if got := errors.Is(err, sqsin.ErrInvalidProviderToken); got != tt.wantPermanent {
				t.Errorf("errors.Is(err, ErrInvalidProviderToken) = %v, want %v", got, tt.wantPermanent)
			}
		})
	}
}

func TestTokenAttributeIdentity_UnfetchedKeyIsTransient(t *testing.T) {
	tests := []struct {
		name        string
		down        bool
		wantUnknown bool
	}{
		{name: "identity provider down", down: true},
		{name: "key not in the key set", wantUnknown: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ti := newTokenIssuer(t)
			id := sqsin.TokenAttributeIdentity{Verifier: ti.verifier}
			other, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
				"iss": testIssuer, "aud": "wagering-api", "sub": "sa", "azp": "provider-a", "provider_id": "provider-a",
				"scope": "wagering:write", "exp": sentAt.Add(5 * time.Minute).Unix(),
			})
			tok.Header["kid"] = "rotated-key"
			raw, err := tok.SignedString(other)
			if err != nil {
				t.Fatal(err)
			}
			ti.down.Store(tt.down)

			_, err = id.ProviderFor(t.Context(), message(raw, millis(sentAt)))

			if err == nil {
				t.Fatal("ProviderFor succeeded with a key the verifier never had")
			}
			if got := errors.Is(err, oidc.ErrUnknownKey); got != tt.wantUnknown {
				t.Errorf("errors.Is(err, oidc.ErrUnknownKey) = %v, want %v (err %v)", got, tt.wantUnknown, err)
			}
			for _, permanent := range []error{sqsin.ErrInvalidProviderToken, sqsin.ErrNotProvider, sqsin.ErrMissingProviderToken} {
				if errors.Is(err, permanent) {
					t.Errorf("err = %v matches %v; a key not fetched yet must be transient", err, permanent)
				}
			}
		})
	}
}

func TestTokenAttributeIdentity_StoppedVerifier(t *testing.T) {
	ti := newTokenIssuer(t)
	raw := ti.token(t, "provider-a", sentAt.Add(5*time.Minute))
	if err := ti.verifier.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	id := sqsin.TokenAttributeIdentity{Verifier: ti.verifier}

	_, err := id.ProviderFor(t.Context(), message(raw, millis(sentAt)))

	if err == nil || errors.Is(err, sqsin.ErrInvalidProviderToken) {
		t.Errorf("err = %v, want an error that is not ErrInvalidProviderToken", err)
	}
}

func TestTokenAttributeIdentity_ErrorsAreDistinct(t *testing.T) {
	errs := []error{sqsin.ErrMissingProviderToken, sqsin.ErrMissingSentTimestamp, sqsin.ErrInvalidProviderToken, sqsin.ErrNotProvider}
	for i, a := range errs {
		for j, b := range errs {
			if i != j && errors.Is(a, b) {
				t.Errorf("%v matches %v", a, b)
			}
		}
	}
}

var _ sqsin.IdentityResolver = sqsin.TokenAttributeIdentity{}
