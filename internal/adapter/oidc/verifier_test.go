package oidc_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

const (
	testIssuer   = "http://keycloak.test/realms/wagering"
	testAudience = "wagering-api"
	testKID      = "test-key"
	testSkew     = 30 * time.Second
)

var now = time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type idp struct {
	key        *rsa.PrivateKey
	url        string
	srv        *httptest.Server
	alg        string
	jwks       atomic.Pointer[[]byte]
	failWith   atomic.Int32
	empty      atomic.Bool
	hang       atomic.Bool
	latency    atomic.Int64
	requests   atomic.Int32
	cancelNext atomic.Pointer[context.CancelFunc]
}

func newIDP(t *testing.T) *idp {
	t.Helper()
	return newIDPWithAlg(t, jwt.SigningMethodRS256.Alg())
}

func newIDPWithAlg(t *testing.T, alg string) *idp {
	t.Helper()
	p := &idp{key: newKey(t), alg: alg}
	p.publish(t, testKID, p.key)
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.requests.Add(1)
		if cancel := p.cancelNext.Swap(nil); cancel != nil {
			(*cancel)()
			<-r.Context().Done()
			return
		}
		if p.hang.Load() {
			<-r.Context().Done()
			return
		}
		time.Sleep(time.Duration(p.latency.Load()))
		if code := p.failWith.Load(); code != 0 {
			w.WriteHeader(int(code))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if p.empty.Load() {
			_, _ = w.Write([]byte(`{"keys":[]}`))
			return
		}
		_, _ = w.Write(*p.jwks.Load())
	}))
	t.Cleanup(p.srv.Close)
	p.url = p.srv.URL
	return p
}

func (p *idp) rotate(t *testing.T, kid string) *rsa.PrivateKey {
	t.Helper()
	key := newKey(t)
	p.publish(t, kid, key)
	return key
}

func (p *idp) publish(t *testing.T, kid string, key *rsa.PrivateKey) {
	t.Helper()
	jwk := map[string]string{
		"kty": "RSA",
		"use": "sig",
		"kid": kid,
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
	if p.alg != "" {
		jwk["alg"] = p.alg
	}
	jwks, err := json.Marshal(map[string]any{"keys": []map[string]string{jwk}})
	if err != nil {
		t.Fatalf("marshal JWKS: %v", err)
	}
	p.jwks.Store(&jwks)
}

func newKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	return key
}

func startVerifier(t *testing.T, jwksURL string, before ...func(*oidc.Verifier)) *oidc.Verifier {
	t.Helper()
	v := oidc.NewVerifier(config.OIDCConfig{
		Issuer:    testIssuer,
		JWKSURL:   jwksURL,
		Audience:  testAudience,
		ClockSkew: testSkew,
	}, nil)
	for _, f := range before {
		f(v)
	}
	if err := v.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := v.Stop(t.Context()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return v
}

func providerClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss":         testIssuer,
		"aud":         testAudience,
		"sub":         "service-account-provider-a",
		"azp":         "provider-a",
		"exp":         now.Add(5 * time.Minute).Unix(),
		"iat":         now.Unix(),
		"scope":       "profile wagering:write wagering:read",
		"provider_id": "provider-a",
	}
}

func internalClaims() jwt.MapClaims {
	c := providerClaims()
	delete(c, "provider_id")
	c["sub"] = "service-account-backoffice"
	c["azp"] = "backoffice"
	c["scope"] = "wallets:manage wagering:read"
	return c
}

func sign(t *testing.T, key *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid
	raw, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return raw
}

func TestVerify_ValidProvider(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	tests := []struct {
		name string
		aud  any
	}{
		{"audience as string", testAudience},
		{"audience as array", []string{"account", testAudience}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := providerClaims()
			c["aud"] = tt.aud

			got, err := v.VerifyAt(t.Context(), sign(t, p.key, testKID, c), now)
			if err != nil {
				t.Fatalf("VerifyAt: %v", err)
			}

			if got.ProviderID != "provider-a" {
				t.Errorf("principal = %+v, want the provider from the claims", got)
			}
			if !got.Has(oidc.ScopeWageringWrite) || !got.Has(oidc.ScopeWageringRead) {
				t.Errorf("scopes = %v, want wagering:write and wagering:read", got.Scopes)
			}
			if !got.IsProvider() || got.IsInternal() {
				t.Errorf("IsProvider = %v, IsInternal = %v, want true, false", got.IsProvider(), got.IsInternal())
			}
		})
	}
}

func TestVerify_ValidInternal(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	got, err := v.VerifyAt(t.Context(), sign(t, p.key, testKID, internalClaims()), now)
	if err != nil {
		t.Fatalf("VerifyAt: %v", err)
	}

	if got.ProviderID != "" {
		t.Errorf("principal = %+v, want no provider", got)
	}
	if !got.IsInternal() || got.IsProvider() {
		t.Errorf("IsInternal = %v, IsProvider = %v, want true, false", got.IsInternal(), got.IsProvider())
	}
}

func TestVerify_UsesCurrentTime(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)
	c := providerClaims()
	c["exp"] = time.Now().Add(5 * time.Minute).Unix()

	if _, err := v.Verify(t.Context(), sign(t, p.key, testKID, c)); err != nil {
		t.Errorf("Verify of a token valid now: %v", err)
	}
}

func TestVerify_Expired(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	tests := []struct {
		name    string
		expAgo  time.Duration
		wantErr error
	}{
		{"expired within the clock skew", testSkew - time.Second, nil},
		{"expired beyond the clock skew", testSkew + time.Second, oidc.ErrExpiredToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := providerClaims()
			c["exp"] = now.Add(-tt.expAgo).Unix()

			_, err := v.VerifyAt(t.Context(), sign(t, p.key, testKID, c), now)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestVerify_InvalidClaims(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	tests := []struct {
		name   string
		mutate func(jwt.MapClaims)
	}{
		{"missing expiry", func(c jwt.MapClaims) { delete(c, "exp") }},
		{"missing subject", func(c jwt.MapClaims) { delete(c, "sub") }},
		{"not yet valid", func(c jwt.MapClaims) { c["nbf"] = now.Add(time.Hour).Unix() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := providerClaims()
			tt.mutate(c)

			_, err := v.VerifyAt(t.Context(), sign(t, p.key, testKID, c), now)

			if !errors.Is(err, oidc.ErrInvalidToken) {
				t.Errorf("err = %v, want ErrInvalidToken", err)
			}
		})
	}
}

func TestVerify_WrongIssuer(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)
	c := providerClaims()
	c["iss"] = "http://keycloak.test/realms/other"

	_, err := v.VerifyAt(t.Context(), sign(t, p.key, testKID, c), now)

	if !errors.Is(err, oidc.ErrInvalidToken) {
		t.Errorf("err = %v, want ErrInvalidToken", err)
	}
}

func TestVerify_WrongAudience(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	tests := []struct {
		name string
		aud  any
	}{
		{"other audience as string", "account"},
		{"other audiences as array", []string{"account", "wagering-admin"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := providerClaims()
			c["aud"] = tt.aud

			_, err := v.VerifyAt(t.Context(), sign(t, p.key, testKID, c), now)

			if !errors.Is(err, oidc.ErrInvalidToken) {
				t.Errorf("err = %v, want ErrInvalidToken", err)
			}
		})
	}
}

func TestVerify_UnknownKey(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)
	other := newKey(t)

	tests := []struct {
		name           string
		kid            string
		wantUnknownKey bool
	}{
		{"published key ID", testKID, false},
		{"unpublished key ID", "other-key", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := v.VerifyAt(t.Context(), sign(t, other, tt.kid, providerClaims()), now)

			if !errors.Is(err, oidc.ErrInvalidToken) {
				t.Errorf("err = %v, want ErrInvalidToken", err)
			}
			if got := errors.Is(err, oidc.ErrUnknownKey); got != tt.wantUnknownKey {
				t.Errorf("errors.Is(err, ErrUnknownKey) = %v, want %v (err %v)", got, tt.wantUnknownKey, err)
			}
		})
	}
}

func TestVerify_HS256Rejected(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)
	pub, err := x509.MarshalPKIXPublicKey(&p.key.PublicKey)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, providerClaims())
	tok.Header["kid"] = testKID
	raw, err := tok.SignedString(pub)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	_, err = v.VerifyAt(t.Context(), raw, now)

	if !errors.Is(err, oidc.ErrInvalidToken) {
		t.Errorf("err = %v, want ErrInvalidToken", err)
	}
}

func TestVerify_OnlyRS256Accepted(t *testing.T) {
	p := newIDPWithAlg(t, "")
	v := startVerifier(t, p.url)

	tests := []struct {
		name    string
		method  jwt.SigningMethod
		wantErr error
	}{
		{"RS256", jwt.SigningMethodRS256, nil},
		{"RS512", jwt.SigningMethodRS512, oidc.ErrInvalidToken},
		{"PS256", jwt.SigningMethodPS256, oidc.ErrInvalidToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok := jwt.NewWithClaims(tt.method, providerClaims())
			tok.Header["kid"] = testKID
			raw, err := tok.SignedString(p.key)
			if err != nil {
				t.Fatalf("sign token: %v", err)
			}

			_, err = v.VerifyAt(t.Context(), raw, now)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestVerifyAt_ZeroTimeRejected(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	_, err := v.VerifyAt(t.Context(), sign(t, p.key, testKID, providerClaims()), time.Time{})

	if !errors.Is(err, oidc.ErrInvalidToken) {
		t.Errorf("err = %v, want ErrInvalidToken", err)
	}
}

func TestVerify_Misconfigured(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	tests := []struct {
		name   string
		claims func() jwt.MapClaims
	}{
		{"both provider and internal", func() jwt.MapClaims {
			c := providerClaims()
			c["scope"] = "wagering:write wallets:manage"
			return c
		}},
		{"neither provider nor internal", func() jwt.MapClaims {
			c := internalClaims()
			c["scope"] = "wagering:read"
			return c
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := v.VerifyAt(t.Context(), sign(t, p.key, testKID, tt.claims()), now)

			if !errors.Is(err, oidc.ErrMisconfiguredPrincipal) {
				t.Errorf("err = %v, want ErrMisconfiguredPrincipal", err)
			}
		})
	}
}

func TestVerify_MissingToken(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	_, err := v.Verify(t.Context(), "")

	if !errors.Is(err, oidc.ErrMissingToken) {
		t.Errorf("err = %v, want ErrMissingToken", err)
	}
}

func TestVerify_Malformed(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	_, err := v.VerifyAt(t.Context(), "not-a-jwt", now)

	if !errors.Is(err, oidc.ErrInvalidToken) {
		t.Errorf("err = %v, want ErrInvalidToken", err)
	}
}

func TestVerify_BeforeStart(t *testing.T) {
	v := oidc.NewVerifier(config.OIDCConfig{Issuer: testIssuer, JWKSURL: "http://127.0.0.1:1", Audience: testAudience}, nil)

	_, err := v.VerifyAt(t.Context(), "a.b.c", now)

	if err == nil || errors.Is(err, oidc.ErrInvalidToken) {
		t.Errorf("err = %v, want a not-started error that is not ErrInvalidToken", err)
	}
}

func TestStart_FailsWhenJWKSUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	v := oidc.NewVerifier(config.OIDCConfig{Issuer: testIssuer, JWKSURL: srv.URL, Audience: testAudience}, nil)

	if err := v.Start(t.Context()); err == nil {
		t.Fatal("Start succeeded with an unreachable JWKS, want an error")
	}
}

func TestStart_FailsWhenJWKSNotFound(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	v := oidc.NewVerifier(config.OIDCConfig{Issuer: testIssuer, JWKSURL: srv.URL, Audience: testAudience}, nil)

	if err := v.Start(t.Context()); err == nil {
		t.Fatal("Start succeeded with a JWKS answering 404, want an error")
	}
}

func TestStart_BoundedByContext(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	v := oidc.NewVerifier(config.OIDCConfig{Issuer: testIssuer, JWKSURL: srv.URL, Audience: testAudience}, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	if err := v.Start(ctx); err == nil {
		t.Fatal("Start succeeded although its context expired, want an error")
	}
}

func TestVerify_KeySetOutageIsNotAVerdict(t *testing.T) {
	tests := []struct {
		name   string
		outage func(*idp)
	}{
		{"key set answers 500", func(p *idp) { p.failWith.Store(http.StatusInternalServerError) }},
		{"key set unreachable", func(p *idp) { p.srv.Close() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newIDP(t)
			v := startVerifier(t, p.url)
			tt.outage(p)

			_, err := v.VerifyAt(t.Context(), sign(t, newKey(t), "rotated-key", providerClaims()), now)

			if err == nil {
				t.Fatal("VerifyAt succeeded with an unknown key and no key set, want an error")
			}
			requireKeySetUnavailable(t, err)
		})
	}
}

func requireKeySetUnavailable(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("err = nil, want the key set reported unavailable")
	}
	if !errors.Is(err, jwt.ErrTokenUnverifiable) || !strings.Contains(err.Error(), "key set unavailable") {
		t.Errorf("err = %v, want a key set unavailable error wrapping jwt.ErrTokenUnverifiable", err)
	}
	for _, verdict := range []error{oidc.ErrInvalidToken, oidc.ErrUnknownKey, oidc.ErrExpiredToken, oidc.ErrMisconfiguredPrincipal, oidc.ErrMissingToken} {
		if errors.Is(err, verdict) {
			t.Errorf("err = %v matches %v; an unusable key set is not a verdict on the token", err, verdict)
		}
	}
}

func TestVerify_UnknownKeyWithHealthyKeySet(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	_, err := v.VerifyAt(t.Context(), sign(t, newKey(t), "rotated-key", providerClaims()), now)

	requireUnknownKey(t, err)
}

func requireUnknownKey(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, oidc.ErrInvalidToken) || !errors.Is(err, oidc.ErrUnknownKey) {
		t.Errorf("err = %v, want it to match both ErrInvalidToken and ErrUnknownKey", err)
	}
}

func TestVerify_CachedKeysServeDuringOutage(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)
	p.failWith.Store(http.StatusServiceUnavailable)
	_, err := v.VerifyAt(t.Context(), sign(t, newKey(t), "rotated-key", providerClaims()), now)
	requireKeySetUnavailable(t, err)

	t.Run("token signed with a cached key verifies", func(t *testing.T) {
		if _, err := v.VerifyAt(t.Context(), sign(t, p.key, testKID, providerClaims()), now); err != nil {
			t.Errorf("VerifyAt: %v", err)
		}
	})
	t.Run("forged signature on a cached key is still invalid", func(t *testing.T) {
		_, err := v.VerifyAt(t.Context(), sign(t, newKey(t), testKID, providerClaims()), now)

		if !errors.Is(err, oidc.ErrInvalidToken) {
			t.Errorf("err = %v, want ErrInvalidToken: the key was there to check against", err)
		}
	})
}

func TestVerify_SecondUnknownKeyWhileHealthyIsInvalid(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	for i := range 3 {
		_, err := v.VerifyAt(t.Context(), sign(t, newKey(t), fmt.Sprintf("unknown-%d", i), providerClaims()), now)

		requireUnknownKey(t, err)
	}
	if got := p.requests.Load(); got != 2 {
		t.Errorf("JWKS requests = %d, want 2", got)
	}
}

func TestVerify_CanceledCallerDoesNotMarkTheKeySetDown(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p.cancelNext.Store(&cancel)
	_, err := v.VerifyAt(ctx, sign(t, newKey(t), "garbage-1", providerClaims()), now)
	if errors.Is(err, oidc.ErrInvalidToken) || err == nil {
		t.Errorf("canceled caller: err = %v, want a non-verdict error so the work is retried", err)
	}

	_, err = v.VerifyAt(t.Context(), sign(t, newKey(t), "garbage-2", providerClaims()), now)
	if !errors.Is(err, oidc.ErrInvalidToken) {
		t.Errorf("after a canceled caller: err = %v, want ErrInvalidToken", err)
	}
}

func TestVerify_EmptyKeySetIsUnavailable(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)
	p.empty.Store(true)
	_, err := v.VerifyAt(t.Context(), sign(t, newKey(t), "rotated-key", providerClaims()), now)
	requireKeySetUnavailable(t, err)

	_, err = v.VerifyAt(t.Context(), sign(t, p.key, testKID, providerClaims()), now)

	requireKeySetUnavailable(t, err)
}

func TestVerify_UnknownKeyIsAnsweredAtOnce(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url, func(v *oidc.Verifier) { v.SetUnknownKeyRefreshEvery(2 * time.Second) })
	_, err := v.VerifyAt(t.Context(), sign(t, newKey(t), "unknown-1", providerClaims()), now)
	if !errors.Is(err, oidc.ErrInvalidToken) {
		t.Fatalf("first unknown kid: err = %v, want ErrInvalidToken", err)
	}
	raw := sign(t, newKey(t), "unknown-2", providerClaims())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()
	_, err = v.VerifyAt(ctx, raw, now)
	elapsed := time.Since(start)

	if !errors.Is(err, oidc.ErrInvalidToken) {
		t.Errorf("second unknown kid: err = %v, want ErrInvalidToken", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("second unknown kid answered after %v, want at once", elapsed)
	}
}

func TestVerify_RotatedKeyIsFetchedOnDemand(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url)
	p.latency.Store(int64(50 * time.Millisecond))
	rotated := p.rotate(t, "rotated-key")

	if _, err := v.VerifyAt(t.Context(), sign(t, rotated, "rotated-key", providerClaims()), now); err != nil {
		t.Errorf("VerifyAt with the rotated key: %v", err)
	}
}

func TestVerify_RotatedKeyAfterTheRefreshWasSpent(t *testing.T) {
	const every = 200 * time.Millisecond
	p := newIDP(t)
	v := startVerifier(t, p.url, func(v *oidc.Verifier) { v.SetUnknownKeyRefreshEvery(every) })
	_, err := v.VerifyAt(t.Context(), sign(t, newKey(t), "garbage", providerClaims()), now)
	if !errors.Is(err, oidc.ErrInvalidToken) {
		t.Fatalf("made-up kid: err = %v, want ErrInvalidToken", err)
	}
	raw := sign(t, p.rotate(t, "rotated-key"), "rotated-key", providerClaims())

	deadline := time.Now().Add(10 * every)
	for {
		_, err := v.VerifyAt(t.Context(), raw, now)
		if err == nil {
			return
		}
		if !errors.Is(err, oidc.ErrInvalidToken) || !errors.Is(err, oidc.ErrUnknownKey) {
			t.Fatalf("rotated key while the refresh is spent: err = %v, want ErrInvalidToken and ErrUnknownKey", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("rotated key still refused %v after the refresh was spent: %v", 10*every, err)
		}
		time.Sleep(every / 4)
	}
}

func TestVerify_HangingKeySetIsBounded(t *testing.T) {
	p := newIDP(t)
	v := startVerifier(t, p.url, func(v *oidc.Verifier) { v.SetKeySetFetchTimeout(200 * time.Millisecond) })
	p.hang.Store(true)
	raw := sign(t, newKey(t), "rotated-key", providerClaims())
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	start := time.Now()
	_, err := v.VerifyAt(ctx, raw, now)
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Errorf("answered after %v, want within the fetch timeout", elapsed)
	}
	requireKeySetUnavailable(t, err)
}
