package bootstrap_test

import (
	"maps"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/Brunohvc/backend-challenge-go/internal/bootstrap"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

func sampleConfig(t *testing.T, overrides map[string]string) config.Config {
	t.Helper()
	vars := map[string]string{
		"APP_INSTANCE_ID": "validate",
		"DATABASE_URL":    "postgres://wallet_app:wallet_app@127.0.0.1:1/wallet?sslmode=disable",
		"OIDC_ISSUER":     "http://127.0.0.1:1/realms/wagering",
		"OIDC_JWKS_URL":   "http://127.0.0.1:1/realms/wagering/protocol/openid-connect/certs",
	}
	maps.Copy(vars, overrides)
	cfg, err := config.Load(func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	})
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}
	return cfg
}

func TestModule_Validates(t *testing.T) {
	workersOff := map[string]string{
		"CONSUMER_ENABLED":     "false",
		"OUTBOX_RELAY_ENABLED": "false",
		"RESOLVER_ENABLED":     "false",
	}
	allOff := maps.Clone(workersOff)
	allOff["HTTP_ENABLED"] = "false"
	tests := map[string]map[string]string{
		"everything on":    nil,
		"http off":         {"HTTP_ENABLED": "false"},
		"consumer off":     {"CONSUMER_ENABLED": "false"},
		"outbox relay off": {"OUTBOX_RELAY_ENABLED": "false"},
		"resolver off":     {"RESOLVER_ENABLED": "false"},
		"every worker off": workersOff,
		"everything off":   allOff,
		"tracing on":       {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1"},
	}
	for name, overrides := range tests {
		t.Run(name, func(t *testing.T) {
			if err := fx.ValidateApp(bootstrap.Module(sampleConfig(t, overrides))); err != nil {
				t.Fatalf("fx.ValidateApp() error = %v", err)
			}
		})
	}
}

func TestModule_StopsWithinTheShutdownTimeout(t *testing.T) {
	app := fx.New(bootstrap.Module(sampleConfig(t, map[string]string{"SHUTDOWN_TIMEOUT": "75s"})))
	if err := app.Err(); err != nil {
		t.Fatalf("fx.New() error = %v", err)
	}

	if got, want := app.StopTimeout(), 75*time.Second; got != want {
		t.Errorf("StopTimeout() = %s, want %s", got, want)
	}
}
