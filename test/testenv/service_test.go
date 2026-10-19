//go:build integration || e2e

package testenv

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEnv_ServiceConfig(t *testing.T) {
	t.Run("loads the configuration AppEnv describes, overrides on top", func(t *testing.T) {
		cfg := fakeEnv().ServiceConfig(t, "wallet-7", map[string]string{"OIDC_CLOCK_SKEW": "5s"})

		if cfg.InstanceID != "wallet-7" {
			t.Errorf("InstanceID = %q, want wallet-7", cfg.InstanceID)
		}
		if cfg.OIDC.ClockSkew != 5*time.Second {
			t.Errorf("OIDC.ClockSkew = %v, want the override, 5s", cfg.OIDC.ClockSkew)
		}
		if cfg.DB.URL != fakeEnv().PG.AppDSN {
			t.Errorf("DB.URL = %q, want the application role's DSN", cfg.DB.URL)
		}
	})

	t.Run("lends the AWS SDK the service's variables and hides the shell's", func(t *testing.T) {
		t.Setenv("AWS_PROFILE", "developer")
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA-developer")
		t.Setenv("AWS_DEFAULT_OUTPUT", "json")

		t.Run("during the test", func(t *testing.T) {
			fakeEnv().ServiceConfig(t, "wallet-1", nil)

			for name, want := range map[string]string{
				"AWS_PROFILE":                 "wallet-app",
				"AWS_ENDPOINT_URL":            "http://127.0.0.1:54566",
				"AWS_SHARED_CREDENTIALS_FILE": "/tmp/testenv/credentials",
			} {
				if got := os.Getenv(name); got != want {
					t.Errorf("%s = %q, want %q", name, got, want)
				}
			}
			for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_DEFAULT_OUTPUT"} {
				if v, ok := os.LookupEnv(name); ok {
					t.Errorf("%s = %q, want it unset, as the service process would never see it", name, v)
				}
			}
		})

		for name, want := range map[string]string{"AWS_PROFILE": "developer", "AWS_ACCESS_KEY_ID": "AKIA-developer", "AWS_DEFAULT_OUTPUT": "json"} {
			if got := os.Getenv(name); got != want {
				t.Errorf("after the test, %s = %q, want the shell's %q back", name, got, want)
			}
		}
		if v, ok := os.LookupEnv("AWS_ENDPOINT_URL"); ok {
			t.Errorf("after the test, AWS_ENDPOINT_URL = %q, want it unset again", v)
		}
	})
}

func TestSyncBuffer_ConcurrentWrites(t *testing.T) {
	var b SyncBuffer
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() { _, _ = fmt.Fprintf(&b, "line %02d\n", i) })
	}
	wg.Wait()

	got := b.String()

	for i := range 50 {
		if !strings.Contains(got, fmt.Sprintf("line %02d\n", i)) {
			t.Errorf("line %02d missing from %q", i, got)
		}
	}
}
