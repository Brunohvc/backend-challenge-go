//go:build integration || e2e

package testenv

import (
	"maps"
	"os"
	"testing"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

func fakeEnv() *Env {
	return &Env{
		PG:  &Postgres{AppDSN: "postgres://wallet_app:wallet_app@127.0.0.1:55432/wallet?sslmode=disable"},
		KC:  &Keycloak{Issuer: "http://127.0.0.1:58080/realms/wagering", JWKSURL: "http://127.0.0.1:58080/realms/wagering/protocol/openid-connect/certs"},
		AWS: &MiniStack{Endpoint: "http://127.0.0.1:54566", CredentialsFile: "/tmp/testenv/credentials"},
	}
}

func lookup(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
}

func TestEnv_AppEnv(t *testing.T) {
	t.Run("loads as a valid configuration pointing at the dependencies", func(t *testing.T) {
		e := fakeEnv()

		cfg, err := config.Load(lookup(e.AppEnv("wallet-1", nil)))
		if err != nil {
			t.Fatalf("config.Load: %v", err)
		}

		if cfg.InstanceID != "wallet-1" {
			t.Errorf("InstanceID = %q, want wallet-1", cfg.InstanceID)
		}
		if cfg.DB.URL != e.PG.AppDSN {
			t.Errorf("DB.URL = %q, want the wallet_app DSN %q", cfg.DB.URL, e.PG.AppDSN)
		}
		if cfg.OIDC.Issuer != e.KC.Issuer || cfg.OIDC.JWKSURL != e.KC.JWKSURL || cfg.OIDC.Audience != "wagering-api" {
			t.Errorf("OIDC = %+v, want issuer %q, JWKS %q, audience wagering-api", cfg.OIDC, e.KC.Issuer, e.KC.JWKSURL)
		}
		if cfg.SQS != queueNames {
			t.Errorf("SQS = %+v, want the provisioned names %+v", cfg.SQS, queueNames)
		}
		if cfg.HTTP.Addr != "127.0.0.1:0" || cfg.Admin.Addr != "127.0.0.1:0" {
			t.Errorf("addresses = %q and %q, want free loopback ports", cfg.HTTP.Addr, cfg.Admin.Addr)
		}
		if cfg.Resolver.TTL != 3*time.Second || cfg.Outbox.PollInterval != 100*time.Millisecond {
			t.Errorf("resolver TTL %s, outbox poll %s, want the shortened test values", cfg.Resolver.TTL, cfg.Outbox.PollInterval)
		}
	})

	t.Run("points the AWS SDK at MiniStack with the application profile", func(t *testing.T) {
		got := fakeEnv().AppEnv("wallet-1", nil)

		want := map[string]string{
			"AWS_REGION":                  "us-east-1",
			"AWS_ENDPOINT_URL":            "http://127.0.0.1:54566",
			"AWS_SHARED_CREDENTIALS_FILE": "/tmp/testenv/credentials",
			"AWS_PROFILE":                 "wallet-app",
			"AWS_CONFIG_FILE":             os.DevNull,
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s = %q, want %q", k, got[k], v)
			}
		}
	})

	t.Run("an override wins over the default", func(t *testing.T) {
		got := fakeEnv().AppEnv("wallet-1", map[string]string{
			"HTTP_ADDR":     "127.0.0.1:18080",
			"CONSUMER_NAME": "second",
			"AWS_PROFILE":   "provider-a",
		})

		for k, want := range map[string]string{"HTTP_ADDR": "127.0.0.1:18080", "CONSUMER_NAME": "second", "AWS_PROFILE": "provider-a"} {
			if got[k] != want {
				t.Errorf("%s = %q, want %q", k, got[k], want)
			}
		}
	})

	t.Run("an empty override deletes the variable", func(t *testing.T) {
		got := fakeEnv().AppEnv("wallet-1", map[string]string{"REFERENCE_TTL": ""})

		if v, ok := got["REFERENCE_TTL"]; ok {
			t.Errorf("REFERENCE_TTL = %q, want it absent so the service default applies", v)
		}
	})

	t.Run("never carries static AWS credentials", func(t *testing.T) {
		names := []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_ACCESS_KEY", "AWS_SECRET_KEY"}
		overrides := make(map[string]string, len(names))
		for _, k := range names {
			overrides[k] = "value"
		}

		got := fakeEnv().AppEnv("wallet-1", overrides)

		for _, k := range names {
			if v, ok := got[k]; ok {
				t.Errorf("%s = %q, want it absent: the profile file is the only source of credentials", k, v)
			}
		}
	})

	t.Run("each call returns its own map", func(t *testing.T) {
		e := fakeEnv()
		first := e.AppEnv("wallet-1", nil)
		before := maps.Clone(first)

		first["HTTP_ADDR"] = "changed"
		delete(first, "DATABASE_URL")
		second := e.AppEnv("wallet-1", nil)

		if !maps.Equal(second, before) {
			t.Errorf("a later call saw the caller's edits: %v, want %v", second, before)
		}
	})
}
