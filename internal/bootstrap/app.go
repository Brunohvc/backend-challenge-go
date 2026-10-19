package bootstrap

import (
	"log/slog"
	"time"

	"go.uber.org/fx"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wallets"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const (
	txRetryBase = 10 * time.Millisecond
	txRetryMax  = 100 * time.Millisecond
)

var appModule = fx.Module("app",
	fx.Provide(newWalletService, newWageringService),
)

type serviceParams struct {
	fx.In

	Config  config.Config
	UoW     app.UnitOfWork
	Clock   app.Clock
	IDs     app.IDGenerator
	Metrics *metrics.Metrics
	Tracer  tracing.Tracer
	Log     *slog.Logger
}

func newWalletService(p serviceParams) *wallets.Service {
	return wallets.NewService(p.UoW, p.Clock, p.IDs, p.Metrics, p.Log, txPolicy(p.Config.DB))
}

func newWageringService(p serviceParams) *wagering.Service {
	r := p.Config.Resolver
	return wagering.NewService(p.UoW, p.Clock, p.IDs, p.Metrics, p.Tracer, p.Log, wagering.Config{
		Reference: wager.ReferencePolicy{
			MaxAttempts: r.MaxAttempts,
			TTL:         r.TTL,
			BackoffBase: r.BackoffBase,
			BackoffMax:  r.BackoffMax,
			Jitter:      retry.PositiveJitter,
		},
		Tx:                    txPolicy(p.Config.DB),
		PermanentFailureLimit: r.PermanentFailureLimit,
	})
}

func txPolicy(cfg config.DBConfig) retry.Policy {
	return retry.Policy{Attempts: cfg.TxMaxAttempts, Base: txRetryBase, Max: txRetryMax}
}
