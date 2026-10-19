//go:build integration

package testenv_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/awsclient"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

func TestEnv_StartAndAppEnv(t *testing.T) {
	ctx := t.Context()
	env, err := testenv.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	terminated := false
	t.Cleanup(func() {
		if !terminated {
			_ = env.Terminate(context.WithoutCancel(ctx))
		}
	})
	appEnv := env.AppEnv("wallet-it", nil)
	cfg, err := config.Load(func(name string) (string, bool) {
		v, ok := appEnv[name]
		return v, ok
	})
	if err != nil {
		t.Fatalf("config.Load of AppEnv: %v", err)
	}

	t.Run("database accepts the application role", func(t *testing.T) {
		conn, err := pgx.Connect(t.Context(), cfg.DB.URL)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer func() { _ = conn.Close(context.WithoutCancel(t.Context())) }()

		var user string
		if err := conn.QueryRow(t.Context(), "SELECT current_user").Scan(&user); err != nil {
			t.Fatalf("query: %v", err)
		}

		if user != "wallet_app" {
			t.Errorf("current_user = %q, want wallet_app", user)
		}
	})

	t.Run("provider token passes the configured verifier", func(t *testing.T) {
		v := oidc.NewVerifier(cfg.OIDC, nil)
		if err := v.Start(t.Context()); err != nil {
			t.Fatalf("verifier Start: %v", err)
		}
		t.Cleanup(func() { _ = v.Stop(context.WithoutCancel(t.Context())) })
		raw, err := env.KC.Token(t.Context(), "provider-a", "provider-a-secret")
		if err != nil {
			t.Fatalf("Token: %v", err)
		}

		p, err := v.Verify(t.Context(), raw)
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}

		if p.ProviderID != "provider-a" {
			t.Errorf("ProviderID = %q, want provider-a", p.ProviderID)
		}
	})

	t.Run("AWS SDK reaches the provisioned queues with the application profile", func(t *testing.T) {
		for _, kv := range os.Environ() {
			name, _, _ := strings.Cut(kv, "=")
			if _, set := appEnv[name]; set || !strings.HasPrefix(name, "AWS_") {
				continue
			}
			t.Setenv(name, "")
			if err := os.Unsetenv(name); err != nil {
				t.Fatalf("unset %s: %v", name, err)
			}
		}
		for k, v := range appEnv {
			t.Setenv(k, v)
		}
		dir := t.TempDir()
		hostile := filepath.Join(dir, "config")
		if err := os.WriteFile(hostile, []byte("[profile wallet-app]\nca_bundle = "+filepath.Join(dir, "missing.pem")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		defaults := awsconfig.DefaultSharedConfigFiles
		awsconfig.DefaultSharedConfigFiles = []string{hostile}
		t.Cleanup(func() { awsconfig.DefaultSharedConfigFiles = defaults })
		awsCfg, err := awsclient.LoadConfig(t.Context())
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}

		queues, err := awsclient.ResolveQueues(t.Context(), sqs.NewFromConfig(awsCfg), cfg.SQS)
		if err != nil {
			t.Fatalf("ResolveQueues: %v", err)
		}

		if queues != env.AWS.Queues {
			t.Errorf("queues = %+v, want %+v", queues, env.AWS.Queues)
		}
	})

	t.Run("terminate removes everything", func(t *testing.T) {
		dir := filepath.Dir(env.AWS.CredentialsFile)
		containers := map[string]testcontainers.Container{
			"postgres": env.PG.Container, "keycloak": env.KC.Container, "ministack": env.AWS.Container,
		}

		if err := env.Terminate(context.WithoutCancel(t.Context())); err != nil {
			t.Fatalf("Terminate: %v", err)
		}
		terminated = true

		if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("work directory %s: Stat error = %v, want it removed", dir, err)
		}
		for name, c := range containers {
			if _, err := c.State(t.Context()); err == nil {
				t.Errorf("%s container still exists after Terminate", name)
			}
		}
	})
}

func TestStart_FailureLeavesNothing(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	env, err := testenv.Start(ctx)

	if err == nil {
		_ = env.Terminate(context.WithoutCancel(ctx))
		t.Fatal("Start succeeded on a canceled context, want an error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Start error = %v, want one wrapping context.Canceled", err)
	}
	if env != nil {
		t.Errorf("Start returned %+v with its error, want nil", env)
	}
	left, readErr := os.ReadDir(tmp)
	if readErr != nil {
		t.Fatalf("read %s: %v", tmp, readErr)
	}
	if len(left) != 0 {
		t.Errorf("Start left %d entries in the temporary directory, want none", len(left))
	}
}
