//go:build integration || e2e

package testenv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
)

const apiAudience = "wagering-api"

var staticAWSCredentials = []string{
	"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
	"AWS_ACCESS_KEY", "AWS_SECRET_KEY",
}

// Env is the database, the identity provider and the AWS emulator a service process needs.
type Env struct {
	PG  *Postgres
	KC  *Keycloak
	AWS *MiniStack

	dir string
}

// Start starts the database, Keycloak and MiniStack at once.
func Start(ctx context.Context) (*Env, error) {
	dir, err := os.MkdirTemp("", "testenv-")
	if err != nil {
		return nil, fmt.Errorf("testenv: create work directory: %w", err)
	}
	e := &Env{dir: dir}

	err = inParallel(
		func() (err error) { e.PG, err = StartPostgres(ctx); return err },
		func() (err error) { e.KC, err = StartKeycloak(ctx, ""); return err },
		func() (err error) { e.AWS, err = StartMiniStack(ctx, dir); return err },
	)
	if err != nil {
		return nil, errors.Join(err, e.Terminate(context.WithoutCancel(ctx)))
	}
	return e, nil
}

// Terminate stops and removes everything Start created.
func (e *Env) Terminate(ctx context.Context) error {
	var steps []func() error
	if e.PG != nil {
		steps = append(steps, func() error { return e.PG.Terminate(ctx) })
	}
	if e.KC != nil {
		steps = append(steps, func() error { return e.KC.Terminate(ctx) })
	}
	if e.AWS != nil {
		steps = append(steps, func() error { return e.AWS.Terminate(ctx) })
	}
	if e.dir != "" {
		steps = append(steps, func() error {
			if err := os.RemoveAll(e.dir); err != nil {
				return fmt.Errorf("testenv: remove work directory: %w", err)
			}
			return nil
		})
	}
	return inParallel(steps...)
}

// AppEnv returns the environment of a service process on the host, with overrides applied.
func (e *Env) AppEnv(instanceID string, overrides map[string]string) map[string]string {
	env := map[string]string{
		"APP_INSTANCE_ID": instanceID,

		"DATABASE_URL": e.PG.AppDSN,

		"OIDC_ISSUER":     e.KC.Issuer,
		"OIDC_JWKS_URL":   e.KC.JWKSURL,
		"OIDC_AUDIENCE":   apiAudience,
		"OIDC_CLOCK_SKEW": "1s",

		"AWS_REGION":                  AWSRegion,
		"AWS_ENDPOINT_URL":            e.AWS.Endpoint,
		"AWS_SHARED_CREDENTIALS_FILE": e.AWS.CredentialsFile,
		"AWS_PROFILE":                 "wallet-app",
		"AWS_CONFIG_FILE":             os.DevNull,
		"SQS_INPUT_QUEUE":             queueNames.InputQueue,
		"SQS_INPUT_DLQ":               queueNames.InputDLQ,
		"SQS_EVENTS_QUEUE":            queueNames.EventsQueue,

		"HTTP_ADDR":  "127.0.0.1:0",
		"ADMIN_ADDR": "127.0.0.1:0",

		"REFERENCE_TTL":          "3s",
		"REFERENCE_BACKOFF_BASE": "100ms",
		"REFERENCE_BACKOFF_MAX":  "1s",
		"OUTBOX_POLL_INTERVAL":   "100ms",
		"RESOLVER_POLL_INTERVAL": "100ms",
		"SHUTDOWN_DRAIN_DELAY":   "1ms",
	}
	for name, value := range overrides {
		if value == "" {
			delete(env, name)
			continue
		}
		env[name] = value
	}
	for _, name := range staticAWSCredentials {
		delete(env, name)
	}
	return env
}

func inParallel(fns ...func() error) error {
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Go(func() { errs[i] = fn() })
	}
	wg.Wait()
	return errors.Join(errs...)
}
