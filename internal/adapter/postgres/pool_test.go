package postgres

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

func TestPoolConfig_SessionSettings(t *testing.T) {
	cfg := config.DBConfig{
		URL:              "postgres://wallet_app:s3cr3t@db.internal:5432/wallet?sslmode=disable",
		MaxConns:         7,
		LockTimeout:      1500 * time.Millisecond,
		StatementTimeout: 4 * time.Second,
	}

	pc, err := poolConfig(cfg, "wallet-replica-2")

	if err != nil {
		t.Fatalf("poolConfig() error = %v", err)
	}
	if pc.MaxConns != cfg.MaxConns {
		t.Errorf("MaxConns = %d, want %d", pc.MaxConns, cfg.MaxConns)
	}
	want := map[string]string{
		"application_name":  "wallet-replica-2",
		"lock_timeout":      "1500",
		"statement_timeout": "4000",
	}
	for name, value := range want {
		if got := pc.ConnConfig.RuntimeParams[name]; got != value {
			t.Errorf("%s = %q, want %q", name, got, value)
		}
	}
}

func TestPoolConfig_MalformedURL(t *testing.T) {
	cfg := config.DBConfig{URL: "postgres://wallet_app:s3cr3t@db.internal:not-a-port/wallet", MaxConns: 1}

	_, err := poolConfig(cfg, "test")

	if !errors.Is(err, app.ErrPermanent) {
		t.Fatalf("poolConfig() error = %v, want app.ErrPermanent", err)
	}
	if strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("poolConfig() error leaks the password: %v", err)
	}
}
