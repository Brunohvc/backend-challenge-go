// Package oidc verifies OAuth 2.0 access tokens and turns them into the Principal of the API.
package oidc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/time/rate"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

// The sentinel errors of the package.
var (
	ErrMissingToken           = errors.New("oidc: missing access token")
	ErrInvalidToken           = errors.New("oidc: invalid access token")
	ErrUnknownKey             = errors.New("oidc: signing key not in the key set")
	ErrExpiredToken           = errors.New("oidc: expired access token")
	ErrMisconfiguredPrincipal = errors.New("oidc: token is neither a provider nor an internal principal")
)

const (
	keySetRefreshInterval     = 5 * time.Minute
	unknownKeyRefreshInterval = 30 * time.Second
)

var errNotStarted = errors.New("oidc: verifier not started")

type claims struct {
	jwt.RegisteredClaims
	Scope      string `json:"scope"`
	ProviderID string `json:"provider_id"`
}

// Verifier verifies access tokens against the identity provider's JWKS.
type Verifier struct {
	jwksURL    string
	parserOpts []jwt.ParserOption

	mu     sync.RWMutex
	keys   keyfunc.Keyfunc
	cancel context.CancelFunc

	log *slog.Logger

	unknownKeyRefresh  *rate.Limiter
	keySetFetchTimeout time.Duration

	keySetFailing atomic.Bool
}

// NewVerifier returns a verifier for the tokens cfg describes, logging refresh failures to log.
func NewVerifier(cfg config.OIDCConfig, log *slog.Logger) *Verifier {
	if log == nil {
		log = slog.Default()
	}
	return &Verifier{
		jwksURL:            cfg.JWKSURL,
		log:                log,
		unknownKeyRefresh:  rate.NewLimiter(rate.Every(unknownKeyRefreshInterval), 1),
		keySetFetchTimeout: config.KeySetFetchTimeout,
		parserOpts: []jwt.ParserOption{
			jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
			jwt.WithIssuer(cfg.Issuer),
			jwt.WithAudience(cfg.Audience),
			jwt.WithExpirationRequired(),
			jwt.WithLeeway(cfg.ClockSkew),
		},
	}
}

// Start fetches the JWKS and starts its background refresh.
func (v *Verifier) Start(ctx context.Context) error {
	refreshCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopBound := context.AfterFunc(ctx, cancel)
	failFast := false
	noLibraryRateLimit := rate.NewLimiter(rate.Inf, 1)
	unknownKeyFetchDeadline := v.keySetFetchTimeout
	keys, err := keyfunc.NewDefaultOverrideCtx(refreshCtx, []string{v.jwksURL}, keyfunc.Override{
		NoErrorReturnFirstHTTPReq: &failFast,
		RefreshInterval:           keySetRefreshInterval,
		HTTPTimeout:               v.keySetFetchTimeout,
		RefreshUnknownKID:         noLibraryRateLimit,
		RateLimitWaitMax:          unknownKeyFetchDeadline,
		Client:                    &http.Client{Transport: keySetTransport{base: http.DefaultTransport, failing: &v.keySetFailing}},
		RefreshErrorHandlerFunc: func(url string) func(context.Context, error) {
			return func(ctx context.Context, err error) {
				if errors.Is(err, context.Canceled) {
					return
				}
				v.keySetFailing.Store(true)
				v.log.WarnContext(ctx, "refresh the identity provider key set", "url", url, "error", err)
			}
		},
	})
	if !stopBound() && err == nil {
		err = ctx.Err()
	}
	if err != nil {
		cancel()
		return fmt.Errorf("oidc: fetch JWKS: %w", err)
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	v.keys, v.cancel = keys, cancel
	return nil
}

// Stop ends the background refresh of the JWKS.
func (v *Verifier) Stop(context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.cancel != nil {
		v.cancel()
	}
	v.keys, v.cancel = nil, nil
	return nil
}

// Verify verifies raw at the current time and returns the principal it describes.
func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	return v.VerifyAt(ctx, raw, clock.System{}.Now())
}

// VerifyAt verifies raw as if at the instant at.
func (v *Verifier) VerifyAt(ctx context.Context, raw string, at time.Time) (Principal, error) {
	if raw == "" {
		return Principal{}, ErrMissingToken
	}
	if at.IsZero() {
		return Principal{}, fmt.Errorf("%w: no verification time", ErrInvalidToken)
	}
	v.mu.RLock()
	keys := v.keys
	v.mu.RUnlock()
	if keys == nil {
		return Principal{}, errNotStarted
	}

	var c claims
	opts := slices.Concat(v.parserOpts, []jwt.ParserOption{jwt.WithTimeFunc(func() time.Time { return at })})
	if _, err := jwt.ParseWithClaims(raw, &c, v.keyLookup(ctx, keys), opts...); err != nil {
		if errors.Is(err, jwt.ErrTokenUnverifiable) {
			if unusable := v.keySetUnusable(ctx, keys); unusable != nil {
				return Principal{}, unusable(err)
			}
			if errors.Is(err, jwkset.ErrKeyNotFound) {
				return Principal{}, fmt.Errorf("%w: %w: %w", ErrInvalidToken, ErrUnknownKey, err)
			}
		}
		if errors.Is(err, jwt.ErrTokenExpired) {
			return Principal{}, fmt.Errorf("%w: %w", ErrExpiredToken, err)
		}
		return Principal{}, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	if c.Subject == "" {
		return Principal{}, fmt.Errorf("%w: sub claim is required", ErrInvalidToken)
	}

	p := Principal{
		ProviderID: c.ProviderID,
		Scopes:     strings.Fields(c.Scope),
	}
	if !p.IsProvider() && !p.IsInternal() {
		return Principal{}, ErrMisconfiguredPrincipal
	}
	return p, nil
}

func (v *Verifier) keyLookup(ctx context.Context, keys keyfunc.Keyfunc) jwt.Keyfunc {
	lookup := keys.KeyfuncCtx(ctx)
	return func(t *jwt.Token) (any, error) {
		if kid, ok := t.Header["kid"].(string); ok && !cached(ctx, keys, kid) && !v.unknownKeyRefresh.Allow() {
			return nil, fmt.Errorf("%w %q, and the key set was fetched for an unknown key too recently", jwkset.ErrKeyNotFound, kid)
		}
		return lookup(t)
	}
}

func cached(ctx context.Context, keys keyfunc.Keyfunc, kid string) bool {
	all, err := keys.Storage().KeyReadAll(context.WithoutCancel(ctx))
	if err != nil {
		return false
	}
	for _, k := range all {
		if k.Marshal().KID == kid {
			return true
		}
	}
	return false
}

func (v *Verifier) keySetUnusable(ctx context.Context, keys keyfunc.Keyfunc) func(error) error {
	switch {
	case ctx.Err() != nil:
		return func(err error) error { return fmt.Errorf("oidc: verification interrupted: %w: %w", ctx.Err(), err) }
	case v.keySetFailing.Load():
		return func(err error) error { return fmt.Errorf("oidc: key set unavailable: %w", err) }
	}
	keysInCache, readErr := keys.Storage().KeyReadAll(context.WithoutCancel(ctx))
	if readErr != nil || len(keysInCache) == 0 {
		return func(err error) error { return fmt.Errorf("oidc: key set unavailable: no usable keys: %w", err) }
	}
	return nil
}

type keySetTransport struct {
	base    http.RoundTripper
	failing *atomic.Bool
}

func (t keySetTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(r)
	if err == nil && resp.StatusCode == http.StatusOK {
		t.failing.Store(false)
	}
	return resp, err
}
