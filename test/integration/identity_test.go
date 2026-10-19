//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

const (
	providerA            = "provider-a"
	providerASecret      = "provider-a-secret"
	providerB            = "provider-b"
	providerBSecret      = "provider-b-secret"
	walletService        = "wallet-service"
	walletServiceSecret  = "wallet-service-secret"
	providerAShort       = "provider-a-short"
	providerAShortSecret = "provider-a-short-secret"
)

const (
	scopeWageringWrite = "wagering:write"
	scopeWageringRead  = "wagering:read"
	scopeWalletsManage = "wallets:manage"
)

func token(t *testing.T, clientID, secret string) string {
	t.Helper()
	tok, err := env.KC.Token(t.Context(), clientID, secret)
	if err != nil {
		t.Fatalf("token for %s: %v", clientID, err)
	}
	return tok
}

func tokenParts(t *testing.T, raw string) (header, payload, signature string) {
	t.Helper()
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts, want 3", len(parts))
	}
	return parts[0], parts[1], parts[2]
}

func decodeSegment(t *testing.T, segment string, v any) {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatalf("decode token segment: %v", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		t.Fatalf("decode token segment %s: %v", raw, err)
	}
}

func encodeSegment(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("encode token segment: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

type tokenClaims struct {
	IssuedAt   int64            `json:"iat"`
	Expiry     int64            `json:"exp"`
	Audience   jwt.ClaimStrings `json:"aud"`
	Scope      string           `json:"scope"`
	ProviderID string           `json:"provider_id"`
}

func claimsOf(t *testing.T, raw string) tokenClaims {
	t.Helper()
	_, payload, _ := tokenParts(t, raw)
	var c tokenClaims
	decodeSegment(t, payload, &c)
	return c
}

func keyIDOf(t *testing.T, raw string) string {
	t.Helper()
	header, _, _ := tokenParts(t, raw)
	var h struct {
		KeyID string `json:"kid"`
	}
	decodeSegment(t, header, &h)
	if h.KeyID == "" {
		t.Fatal("Keycloak token has no kid")
	}
	return h.KeyID
}

func resigned(t *testing.T, raw, kid string) string {
	t.Helper()
	_, payload, _ := tokenParts(t, raw)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	signing := encodeSegment(t, map[string]string{"alg": "RS256", "typ": "JWT", "kid": kid}) + "." + payload
	sig, err := jwt.SigningMethodRS256.Sign(signing, key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func unsigned(t *testing.T, raw string) string {
	t.Helper()
	_, payload, _ := tokenParts(t, raw)
	return encodeSegment(t, map[string]string{"alg": "none", "typ": "JWT"}) + "." + payload + "."
}

func tampered(t *testing.T, raw, claim, value string) string {
	t.Helper()
	header, payload, signature := tokenParts(t, raw)
	var claims map[string]any
	decodeSegment(t, payload, &claims)
	claims[claim] = value
	return header + "." + encodeSegment(t, claims) + "." + signature
}

const apiAudience = "wagering-api"

func providerClientToken(t *testing.T, providerID string, scopes ...string) string {
	t.Helper()
	return runtimeClientToken(t, providerID, true, scopes)
}

func otherAudienceToken(t *testing.T, providerID string) string {
	t.Helper()
	tok := runtimeClientToken(t, providerID, false, []string{scopeWageringWrite, scopeWageringRead})
	if c := claimsOf(t, tok); slices.Contains(c.Audience, apiAudience) || c.ProviderID != providerID {
		t.Fatalf("token for another audience has aud %v and provider_id %q, want no %s and %s", c.Audience, c.ProviderID, apiAudience, providerID)
	}
	return tok
}

func runtimeClientToken(t *testing.T, providerID string, withAudience bool, scopes []string) string {
	t.Helper()
	clientID, secret := providerID+"-test-"+newID(), "secret-"+newID()
	mappers := []map[string]any{{
		"name": "provider-id", "protocol": "openid-connect", "protocolMapper": "oidc-hardcoded-claim-mapper",
		"config": map[string]string{
			"claim.name": "provider_id", "claim.value": providerID,
			"jsonType.label": "String", "access.token.claim": "true",
		},
	}}
	if withAudience {
		mappers = append(mappers, map[string]any{
			"name": "wagering-api-audience", "protocol": "openid-connect", "protocolMapper": "oidc-audience-mapper",
			"config": map[string]string{"included.custom.audience": apiAudience, "access.token.claim": "true"},
		})
	}
	rep := map[string]any{
		"clientId":                  clientID,
		"protocol":                  "openid-connect",
		"publicClient":              false,
		"secret":                    secret,
		"serviceAccountsEnabled":    true,
		"standardFlowEnabled":       false,
		"implicitFlowEnabled":       false,
		"directAccessGrantsEnabled": false,
		"defaultClientScopes":       scopes,
		"optionalClientScopes":      []string{},
		"protocolMappers":           mappers,
	}
	location, err := env.KC.CreateClient(t.Context(), rep)
	if err != nil {
		t.Fatalf("client %s: %v", clientID, err)
	}
	t.Cleanup(func() {
		if err := env.KC.DeleteClient(context.WithoutCancel(t.Context()), location); err != nil {
			t.Errorf("client %s: %v", clientID, err)
		}
	})

	tok := token(t, clientID, secret)
	got := strings.Fields(claimsOf(t, tok).Scope)
	for _, scope := range []string{scopeWageringWrite, scopeWageringRead, scopeWalletsManage} {
		if slices.Contains(got, scope) != slices.Contains(scopes, scope) {
			t.Fatalf("token of %s has scopes %v, want exactly %v of the realm's", clientID, got, scopes)
		}
	}
	return tok
}
