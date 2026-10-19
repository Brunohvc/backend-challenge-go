package oidc

import "slices"

// The scopes the API grants.
const (
	ScopeWageringWrite = "wagering:write"
	ScopeWageringRead  = "wagering:read"
	ScopeWalletsManage = "wallets:manage"
)

// Principal is the authenticated caller described by a verified access token.
type Principal struct {
	ProviderID string
	Scopes     []string
}

// Has reports whether the principal was granted scope.
func (p Principal) Has(scope string) bool {
	return slices.Contains(p.Scopes, scope)
}

// IsInternal reports whether the principal is the internal back office.
func (p Principal) IsInternal() bool {
	return p.ProviderID == "" && p.Has(ScopeWalletsManage)
}

// IsProvider reports whether the principal acts for a provider.
func (p Principal) IsProvider() bool {
	return p.ProviderID != "" && !p.Has(ScopeWalletsManage)
}
