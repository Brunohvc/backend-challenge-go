package oidc_test

import (
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
)

func TestPrincipal_Kind(t *testing.T) {
	tests := []struct {
		name         string
		p            oidc.Principal
		wantInternal bool
		wantProvider bool
	}{
		{
			name:         "provider",
			p:            oidc.Principal{ProviderID: "provider-a", Scopes: []string{oidc.ScopeWageringWrite}},
			wantProvider: true,
		},
		{
			name:         "internal",
			p:            oidc.Principal{Scopes: []string{oidc.ScopeWalletsManage}},
			wantInternal: true,
		},
		{
			name: "provider holding wallets:manage",
			p:    oidc.Principal{ProviderID: "provider-a", Scopes: []string{oidc.ScopeWalletsManage}},
		},
		{
			name: "no provider and no wallets:manage",
			p:    oidc.Principal{Scopes: []string{oidc.ScopeWageringRead}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.IsInternal(); got != tt.wantInternal {
				t.Errorf("IsInternal() = %v, want %v", got, tt.wantInternal)
			}
			if got := tt.p.IsProvider(); got != tt.wantProvider {
				t.Errorf("IsProvider() = %v, want %v", got, tt.wantProvider)
			}
		})
	}
}

func TestPrincipal_Has(t *testing.T) {
	p := oidc.Principal{Scopes: []string{oidc.ScopeWageringRead}}

	tests := []struct {
		scope string
		want  bool
	}{
		{oidc.ScopeWageringRead, true},
		{oidc.ScopeWageringWrite, false},
		{"wagering", false},
	}
	for _, tt := range tests {
		t.Run(tt.scope, func(t *testing.T) {
			if got := p.Has(tt.scope); got != tt.want {
				t.Errorf("Has(%q) = %v, want %v", tt.scope, got, tt.want)
			}
		})
	}
}
