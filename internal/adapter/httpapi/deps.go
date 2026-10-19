// Package httpapi is the public HTTP API: server, middleware, problem details and routes.
package httpapi

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wallets"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

// TokenVerifier verifies a bearer token.
type TokenVerifier interface {
	Verify(ctx context.Context, raw string) (oidc.Principal, error)
}

// HealthChecker reports readiness.
type HealthChecker interface {
	Ready(ctx context.Context) (bool, map[string]string)
}

// HTTPMetrics records one served request.
type HTTPMetrics interface {
	Observe(route, method string, code int, d time.Duration)
}

// WalletService is the use case of the wallet routes.
type WalletService interface {
	Open(ctx context.Context, cmd wallets.OpenCommand) (wallet.Snapshot, error)
	Get(ctx context.Context, id uuid.UUID) (wallet.Snapshot, error)
	Ledger(ctx context.Context, id uuid.UUID, afterSeq int64, limit int) (wallets.LedgerPage, error)
	Reconcile(ctx context.Context, id uuid.UUID) (wallets.Reconciliation, error)
}

// WageringService is the use case of the wagering routes.
type WageringService interface {
	Submit(ctx context.Context, cmd wager.Command, src wagering.Source) (wagering.Result, error)
	Get(ctx context.Context, id uuid.UUID) (wager.Snapshot, error)
	GetForProvider(ctx context.Context, id uuid.UUID, providerID string) (wager.Snapshot, error)
	GetByExternal(ctx context.Context, providerID, externalID string) (wager.Snapshot, error)
}

// IDGenerator generates the correlation ids of requests that bring none.
type IDGenerator interface {
	New() uuid.UUID
}

// Deps is what the router needs.
type Deps struct {
	Wallets      WalletService
	Wagering     WageringService
	Verifier     TokenVerifier
	Health       HealthChecker
	Metrics      HTTPMetrics
	Log          *slog.Logger
	MaxBodyBytes int64
	IDs          IDGenerator
	Tracer       tracing.Tracer
}
