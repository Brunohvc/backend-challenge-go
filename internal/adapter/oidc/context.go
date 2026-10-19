package oidc

import "context"

type ctxKey struct{}

// WithPrincipal returns a context carrying p for the handlers behind the authentication.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// PrincipalFrom returns the principal carried by ctx and whether there was one.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}
