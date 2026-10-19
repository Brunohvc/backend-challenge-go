//go:build load

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const tokenRenewal = time.Minute

var devSecrets = map[string]string{
	"provider-a":     "provider-a-secret",
	"provider-b":     "provider-b-secret",
	"wallet-service": "wallet-service-secret",
}

type tokenSource struct {
	tokenURL string
	clientID string
	secret   string
	http     *http.Client

	mu      sync.Mutex
	token   string
	expires time.Time
}

func newTokenSource(keycloakURL, clientID string, client *http.Client) *tokenSource {
	return &tokenSource{
		tokenURL: strings.TrimSuffix(keycloakURL, "/") + "/realms/wagering/protocol/openid-connect/token",
		clientID: clientID,
		secret:   devSecrets[clientID],
		http:     client,
	}
}

func (s *tokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && time.Until(s.expires) > tokenRenewal {
		return s.token, nil
	}
	token, err := s.fetch(ctx)
	if err != nil {
		return "", err
	}
	expires, err := expiryOf(token)
	if err != nil {
		return "", err
	}
	s.token, s.expires = token, expires
	return token, nil
}

func (s *tokenSource) fetch(ctx context.Context) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}}
	header := http.Header{}
	header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(s.clientID+":"+s.secret)))
	header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := send(ctx, s.http, http.MethodPost, s.tokenURL, header, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("token of %s: %w", s.clientID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("token of %s: %w", s.clientID, err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token of %s: status %d: %s", s.clientID, resp.StatusCode, raw)
	}
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.AccessToken == "" {
		return "", fmt.Errorf("token of %s: no access_token in the answer", s.clientID)
	}
	return body.AccessToken, nil
}

func expiryOf(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, errors.New("access token is not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("access token payload: %w", err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == 0 {
		return time.Time{}, errors.New("access token has no exp claim")
	}
	return time.Unix(claims.Exp, 0), nil
}
