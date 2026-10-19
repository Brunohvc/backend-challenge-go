package oidc_test

import (
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
)

func TestPrincipalFrom_RoundTrip(t *testing.T) {
	want := oidc.Principal{ProviderID: "p", Scopes: []string{oidc.ScopeWageringWrite}}

	got, ok := oidc.PrincipalFrom(oidc.WithPrincipal(t.Context(), want))

	if !ok || got.ProviderID != want.ProviderID || !got.Has(oidc.ScopeWageringWrite) {
		t.Errorf("PrincipalFrom = %+v, %v, want %+v, true", got, ok, want)
	}
}

func TestPrincipalFrom_Absent(t *testing.T) {
	if _, ok := oidc.PrincipalFrom(t.Context()); ok {
		t.Error("PrincipalFrom reported a principal on a bare context")
	}
}
