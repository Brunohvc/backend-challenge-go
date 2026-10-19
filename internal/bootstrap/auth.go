package bootstrap

import (
	"log/slog"

	"go.uber.org/fx"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

var authModule = fx.Module("auth",
	fx.Provide(newVerifier),
	fx.Invoke(registerVerifier),
)

func newVerifier(cfg config.Config, log *slog.Logger) *oidc.Verifier {
	return oidc.NewVerifier(cfg.OIDC, log)
}

func registerVerifier(l lifecycle, v *oidc.Verifier) {
	l.add(component{name: "verifier", start: v.Start, stop: v.Stop})
}
