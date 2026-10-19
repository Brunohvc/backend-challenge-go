//go:build integration

package testenv_test

import (
	"context"
	"testing"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

func TestKeycloak_TokenVerifies(t *testing.T) {
	kc, err := testenv.StartKeycloak(t.Context(), "")
	if err != nil {
		t.Fatalf("StartKeycloak: %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(context.WithoutCancel(t.Context())) })
	v := oidc.NewVerifier(config.OIDCConfig{Issuer: kc.Issuer, JWKSURL: kc.JWKSURL, Audience: "wagering-api", ClockSkew: 30 * time.Second}, nil)
	if err := v.Start(t.Context()); err != nil {
		t.Fatalf("verifier Start: %v", err)
	}
	t.Cleanup(func() { _ = v.Stop(t.Context()) })

	tests := []struct {
		clientID, secret string
		wantProvider     string
		wantInternal     bool
	}{
		{"provider-a", "provider-a-secret", "provider-a", false},
		{"wallet-service", "wallet-service-secret", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.clientID, func(t *testing.T) {
			raw, err := kc.Token(t.Context(), tt.clientID, tt.secret)
			if err != nil {
				t.Fatalf("Token: %v", err)
			}

			p, err := v.Verify(t.Context(), raw)
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}

			if p.ProviderID != tt.wantProvider || p.IsInternal() != tt.wantInternal {
				t.Errorf("principal = %+v, want provider %q, internal %v", p, tt.wantProvider, tt.wantInternal)
			}
		})
	}

	t.Run("wrong secret", func(t *testing.T) {
		if _, err := kc.Token(t.Context(), "provider-a", "wrong"); err == nil {
			t.Error("Token succeeded with a wrong secret, want an error")
		}
	})

	t.Run("client added through the admin API", func(t *testing.T) {
		ctx := t.Context()
		if err := kc.AllowAdminOverHTTP(ctx); err != nil {
			t.Fatalf("AllowAdminOverHTTP: %v", err)
		}
		const clientID, secret = "runtime-client", "runtime-secret"
		location, err := kc.CreateClient(ctx, map[string]any{
			"clientId":               clientID,
			"secret":                 secret,
			"publicClient":           false,
			"serviceAccountsEnabled": true,
			"standardFlowEnabled":    false,
			"defaultClientScopes":    []string{"wagering:read"},
			"protocolMappers": []map[string]any{
				{
					"name": "provider-id", "protocol": "openid-connect", "protocolMapper": "oidc-hardcoded-claim-mapper",
					"config": map[string]string{
						"claim.name": "provider_id", "claim.value": "provider-a",
						"jsonType.label": "String", "access.token.claim": "true",
					},
				},
				{
					"name": "audience", "protocol": "openid-connect", "protocolMapper": "oidc-audience-mapper",
					"config": map[string]string{"included.custom.audience": "wagering-api", "access.token.claim": "true"},
				},
			},
		})
		if err != nil {
			t.Fatalf("CreateClient: %v", err)
		}

		raw, err := kc.Token(ctx, clientID, secret)
		if err != nil {
			t.Fatalf("Token of the new client: %v", err)
		}
		p, err := v.Verify(ctx, raw)
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
		if p.ProviderID != "provider-a" || !p.Has(oidc.ScopeWageringRead) || p.Has(oidc.ScopeWageringWrite) {
			t.Errorf("principal = %+v, want provider-a with wagering:read only", p)
		}

		if err := kc.DeleteClient(ctx, location); err != nil {
			t.Fatalf("DeleteClient: %v", err)
		}
		if _, err := kc.Token(ctx, clientID, secret); err == nil {
			t.Error("Token succeeded for a deleted client, want an error")
		}
	})
}
