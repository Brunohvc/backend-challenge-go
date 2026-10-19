package bootstrap

import (
	"log/slog"

	"go.uber.org/fx"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/httpapi"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wallets"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/health"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

func httpModule(cfg config.HTTPConfig) fx.Option {
	if !cfg.Enabled {
		return fx.Options()
	}
	return fx.Module("http",
		fx.Provide(newHTTPServer),
		fx.Invoke(registerHTTP),
	)
}

type httpParams struct {
	fx.In

	Config   config.Config
	Wallets  *wallets.Service
	Wagering *wagering.Service
	Verifier *oidc.Verifier
	Health   *health.Checker
	Metrics  *metrics.Metrics
	IDs      app.IDGenerator
	Tracer   tracing.Tracer
	Log      *slog.Logger
}

func newHTTPServer(p httpParams) *httpapi.Server {
	router := httpapi.NewRouter(httpapi.Deps{
		Wallets:      p.Wallets,
		Wagering:     p.Wagering,
		Verifier:     p.Verifier,
		Health:       p.Health,
		Metrics:      p.Metrics,
		Log:          p.Log,
		MaxBodyBytes: p.Config.HTTP.MaxBodyBytes,
		IDs:          p.IDs,
		Tracer:       p.Tracer,
	})
	return httpapi.NewServer(p.Config.HTTP, router, p.Log)
}

func registerHTTP(l lifecycle, s *httpapi.Server) {
	l.add(serverComponent("http", s))
}
