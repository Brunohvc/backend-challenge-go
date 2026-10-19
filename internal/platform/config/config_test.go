package config

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func requiredEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL":  "postgres://wallet:secret@localhost:5432/wallet",
		"OIDC_ISSUER":   "http://localhost:8080/realms/wagering",
		"OIDC_JWKS_URL": "http://localhost:8080/realms/wagering/protocol/openid-connect/certs",
	}
}

func envWith(overrides map[string]string) map[string]string {
	env := requiredEnv()
	maps.Copy(env, overrides)
	return env
}

func hostnameOf(name string) func() (string, error) {
	return func() (string, error) { return name, nil }
}

func lookupFrom(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
}

func loadEnv(env map[string]string) (Config, error) {
	return load(lookupFrom(env), hostnameOf("test-host"))
}

func problemsOf(t *testing.T, err error) []problem {
	t.Helper()
	var verr *validationError
	if !errors.As(err, &verr) {
		t.Fatalf("error = %v (%T), want *validationError", err, err)
	}
	return verr.problems
}

func TestLoad_Defaults(t *testing.T) {
	env := requiredEnv()

	got, err := loadEnv(env)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := Config{
		InstanceID:         "test-host",
		LogLevel:           slog.LevelInfo,
		ShutdownDrainDelay: 2 * time.Second,
		ShutdownTimeout:    60 * time.Second,
		HTTP: HTTPConfig{
			Enabled:           true,
			Addr:              ":8080",
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       60 * time.Second,
			ShutdownTimeout:   10 * time.Second,
			RequestTimeout:    10 * time.Second,
			MaxBodyBytes:      65536,
			MaxHeaderBytes:    16384,
		},
		Admin: AdminConfig{Addr: ":9090"},
		DB: DBConfig{
			URL:              env["DATABASE_URL"],
			MaxConns:         20,
			LockTimeout:      2 * time.Second,
			StatementTimeout: 5 * time.Second,
			TxMaxAttempts:    3,
		},
		OIDC: OIDCConfig{
			Issuer:    env["OIDC_ISSUER"],
			JWKSURL:   env["OIDC_JWKS_URL"],
			Audience:  "wagering-api",
			ClockSkew: 30 * time.Second,
		},
		SQS: SQSConfig{
			InputQueue:  "wager-transactions.fifo",
			InputDLQ:    "wager-transactions-dlq.fifo",
			EventsQueue: "wallet-events.fifo",
		},
		Consumer: ConsumerConfig{
			Enabled:           true,
			Name:              "wager-transactions",
			MaxGroups:         8,
			VisibilityTimeout: 30 * time.Second,
			ProcessingTimeout: 15 * time.Second,
			DrainTimeout:      20 * time.Second,
			WaitTime:          20 * time.Second,
			MaxReceives:       10,
		},
		Outbox: OutboxConfig{
			Enabled:      true,
			PollInterval: 500 * time.Millisecond,
			BatchSize:    50,
			Lease:        30 * time.Second,
			BackoffMax:   5 * time.Minute,
		},
		Resolver: ResolverConfig{
			Enabled:               true,
			PollInterval:          time.Second,
			BatchSize:             50,
			Lease:                 30 * time.Second,
			MaxAttempts:           10,
			TTL:                   15 * time.Minute,
			BackoffBase:           time.Second,
			BackoffMax:            60 * time.Second,
			PermanentFailureLimit: 3,
		},
		Tracing: TracingConfig{ServiceName: "wallet"},
	}
	if got != want {
		t.Errorf("Load() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoad_Overrides(t *testing.T) {
	env := envWith(map[string]string{
		"APP_INSTANCE_ID":             "replica-7",
		"LOG_LEVEL":                   "DeBuG",
		"HTTP_ENABLED":                "0",
		"HTTP_ADDR":                   "127.0.0.1:8081",
		"HTTP_REQUEST_TIMEOUT":        "3s",
		"HTTP_MAX_BODY_BYTES":         "1024",
		"HTTP_MAX_HEADER_BYTES":       "8192",
		"DB_MAX_CONNS":                "5",
		"DB_TX_MAX_ATTEMPTS":          "1",
		"CONSUMER_ENABLED":            "false",
		"CONSUMER_VISIBILITY_TIMEOUT": "1m",
		"CONSUMER_PROCESSING_TIMEOUT": "59s",
		"CONSUMER_MAX_RECEIVES":       "3",
		"OUTBOX_POLL_INTERVAL":        "250ms",
		"OUTBOX_BATCH_SIZE":           "500",
		"RESOLVER_ENABLED":            "T",
		"RESOLVER_BATCH_SIZE":         "1",
		"SHUTDOWN_TIMEOUT":            "90s",
	})

	got, err := loadEnv(env)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"InstanceID", got.InstanceID, "replica-7"},
		{"LogLevel", got.LogLevel, slog.LevelDebug},
		{"HTTP.Enabled", got.HTTP.Enabled, false},
		{"HTTP.Addr", got.HTTP.Addr, "127.0.0.1:8081"},
		{"HTTP.RequestTimeout", got.HTTP.RequestTimeout, 3 * time.Second},
		{"HTTP.MaxBodyBytes", got.HTTP.MaxBodyBytes, int64(1024)},
		{"HTTP.MaxHeaderBytes", got.HTTP.MaxHeaderBytes, 8192},
		{"DB.MaxConns", got.DB.MaxConns, int32(5)},
		{"DB.TxMaxAttempts", got.DB.TxMaxAttempts, 1},
		{"Consumer.Enabled", got.Consumer.Enabled, false},
		{"Consumer.VisibilityTimeout", got.Consumer.VisibilityTimeout, time.Minute},
		{"Consumer.ProcessingTimeout", got.Consumer.ProcessingTimeout, 59 * time.Second},
		{"Consumer.MaxReceives", got.Consumer.MaxReceives, 3},
		{"Outbox.PollInterval", got.Outbox.PollInterval, 250 * time.Millisecond},
		{"Outbox.BatchSize", got.Outbox.BatchSize, 500},
		{"Resolver.Enabled", got.Resolver.Enabled, true},
		{"Resolver.BatchSize", got.Resolver.BatchSize, 1},
		{"ShutdownTimeout", got.ShutdownTimeout, 90 * time.Second},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoad_LogLevels(t *testing.T) {
	tests := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
		"WARN":  slog.LevelWarn,
		"Error": slog.LevelError,
	}
	for raw, want := range tests {
		t.Run(raw, func(t *testing.T) {
			got, err := loadEnv(envWith(map[string]string{"LOG_LEVEL": raw}))
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got.LogLevel != want {
				t.Errorf("LogLevel = %v, want %v", got.LogLevel, want)
			}
		})
	}
}

func TestLoad_InstanceIDDefaultsToHostname(t *testing.T) {
	want, err := os.Hostname()
	if err != nil {
		t.Skipf("host name unavailable: %v", err)
	}

	got, err := Load(lookupFrom(requiredEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.InstanceID != want {
		t.Errorf("InstanceID = %q, want %q", got.InstanceID, want)
	}
}

func TestLoad_MissingRequired(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []problem
	}{
		{
			name: "consumer disabled",
			env:  map[string]string{"CONSUMER_ENABLED": "false"},
			want: []problem{
				{"DATABASE_URL", "is required"},
				{"OIDC_ISSUER", "is required"},
				{"OIDC_JWKS_URL", "is required"},
			},
		},
		{
			name: "nothing set",
			env:  map[string]string{},
			want: []problem{
				{"DATABASE_URL", "is required"},
				{"OIDC_ISSUER", "is required"},
				{"OIDC_JWKS_URL", "is required"},
			},
		},
		{
			name: "empty counts as missing",
			env: map[string]string{
				"DATABASE_URL":  "",
				"OIDC_ISSUER":   "https://issuer.example",
				"OIDC_JWKS_URL": "https://issuer.example/certs",
			},
			want: []problem{
				{"DATABASE_URL", "is required"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadEnv(tt.env)

			got := problemsOf(t, err)
			if !slices.Equal(got, tt.want) {
				t.Errorf("problems =\n%v\nwant\n%v", got, tt.want)
			}
			for _, p := range tt.want {
				if !strings.Contains(err.Error(), p.name) {
					t.Errorf("error %q does not mention %s", err, p.name)
				}
			}
		})
	}
}

func TestLoad_InvalidValues(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]string
		want      []problem
	}{
		{
			name:      "integer is not a number",
			overrides: map[string]string{"DB_MAX_CONNS": "abc"},
			want:      []problem{{"DB_MAX_CONNS", `must be an integer, got "abc"`}},
		},
		{
			name:      "integer does not fit its type",
			overrides: map[string]string{"DB_MAX_CONNS": "4294967296"},
			want:      []problem{{"DB_MAX_CONNS", `is out of range, got "4294967296"`}},
		},
		{
			name:      "integer below minimum",
			overrides: map[string]string{"DB_TX_MAX_ATTEMPTS": "0"},
			want:      []problem{{"DB_TX_MAX_ATTEMPTS", "must be at least 1, got 0"}},
		},
		{
			name:      "consumer gives up before the first receive",
			overrides: map[string]string{"CONSUMER_MAX_RECEIVES": "0"},
			want:      []problem{{"CONSUMER_MAX_RECEIVES", "must be at least 1, got 0"}},
		},
		{
			name:      "negative duration",
			overrides: map[string]string{"HTTP_READ_TIMEOUT": "-1s"},
			want:      []problem{{"HTTP_READ_TIMEOUT", "must be positive, got -1s"}},
		},
		{
			name:      "zero duration",
			overrides: map[string]string{"OUTBOX_LEASE": "0s"},
			want:      []problem{{"OUTBOX_LEASE", "must be positive, got 0s"}},
		},
		{
			name:      "malformed duration",
			overrides: map[string]string{"DB_LOCK_TIMEOUT": "2"},
			want:      []problem{{"DB_LOCK_TIMEOUT", `must be a duration such as 500ms or 30s, got "2"`}},
		},
		{
			name:      "malformed boolean",
			overrides: map[string]string{"HTTP_ENABLED": "maybe"},
			want:      []problem{{"HTTP_ENABLED", `must be a boolean, got "maybe"`}},
		},
		{
			name:      "unknown log level",
			overrides: map[string]string{"LOG_LEVEL": "verbose"},
			want:      []problem{{"LOG_LEVEL", `must be one of debug, info, warn or error, got "verbose"`}},
		},
		{
			name:      "bounded integer is not a number",
			overrides: map[string]string{"OUTBOX_BATCH_SIZE": "lots"},
			want:      []problem{{"OUTBOX_BATCH_SIZE", `must be an integer, got "lots"`}},
		},
		{
			name:      "outbox batch below range",
			overrides: map[string]string{"OUTBOX_BATCH_SIZE": "0"},
			want:      []problem{{"OUTBOX_BATCH_SIZE", "must be between 1 and 500, got 0"}},
		},
		{
			name:      "resolver batch above range",
			overrides: map[string]string{"RESOLVER_BATCH_SIZE": "501"},
			want:      []problem{{"RESOLVER_BATCH_SIZE", "must be between 1 and 500, got 501"}},
		},
		{
			name:      "processing timeout longer than visibility timeout",
			overrides: map[string]string{"CONSUMER_PROCESSING_TIMEOUT": "40s"},
			want: []problem{{
				"CONSUMER_PROCESSING_TIMEOUT",
				"must be shorter than CONSUMER_VISIBILITY_TIMEOUT (30s), got 40s",
			}},
		},
		{
			name:      "processing timeout equal to visibility timeout",
			overrides: map[string]string{"CONSUMER_PROCESSING_TIMEOUT": "30s"},
			want: []problem{{
				"CONSUMER_PROCESSING_TIMEOUT",
				"must be shorter than CONSUMER_VISIBILITY_TIMEOUT (30s), got 30s",
			}},
		},
		{
			name:      "lock timeout longer than statement timeout",
			overrides: map[string]string{"DB_LOCK_TIMEOUT": "6s"},
			want:      []problem{{"DB_LOCK_TIMEOUT", "must be shorter than DB_STATEMENT_TIMEOUT (5s), got 6s"}},
		},
		{
			name:      "lock timeout equal to statement timeout",
			overrides: map[string]string{"DB_LOCK_TIMEOUT": "5s", "DB_STATEMENT_TIMEOUT": "5s"},
			want:      []problem{{"DB_LOCK_TIMEOUT", "must be shorter than DB_STATEMENT_TIMEOUT (5s), got 5s"}},
		},
		{
			name:      "lock timeout is not checked against an unparsable statement timeout",
			overrides: map[string]string{"DB_LOCK_TIMEOUT": "6s", "DB_STATEMENT_TIMEOUT": "later"},
			want:      []problem{{"DB_STATEMENT_TIMEOUT", `must be a duration such as 500ms or 30s, got "later"`}},
		},
		{
			name:      "reference backoff base above its max",
			overrides: map[string]string{"REFERENCE_BACKOFF_BASE": "2m", "REFERENCE_BACKOFF_MAX": "1m"},
			want:      []problem{{"REFERENCE_BACKOFF_BASE", "must be at most REFERENCE_BACKOFF_MAX (1m0s), got 2m0s"}},
		},
		{
			name:      "long poll under a second",
			overrides: map[string]string{"CONSUMER_WAIT_TIME": "500ms"},
			want:      []problem{{"CONSUMER_WAIT_TIME", "must be between 1s and 20s, got 500ms"}},
		},
		{
			name:      "long poll past what SQS accepts",
			overrides: map[string]string{"CONSUMER_WAIT_TIME": "21s"},
			want:      []problem{{"CONSUMER_WAIT_TIME", "must be between 1s and 20s, got 21s"}},
		},
		{
			name: "ordering is not checked against an unparsable timeout",
			overrides: map[string]string{
				"CONSUMER_PROCESSING_TIMEOUT": "40s",
				"CONSUMER_VISIBILITY_TIMEOUT": "soon",
			},
			want: []problem{{
				"CONSUMER_VISIBILITY_TIMEOUT",
				`must be a duration such as 500ms or 30s, got "soon"`,
			}},
		},
		{
			name: "every problem is reported, sorted by name",
			overrides: map[string]string{
				"HTTP_READ_TIMEOUT":           "-1s",
				"DB_MAX_CONNS":                "abc",
				"CONSUMER_PROCESSING_TIMEOUT": "40s",
				"APP_INSTANCE_ID":             "replica-1",
			},
			want: []problem{
				{"CONSUMER_PROCESSING_TIMEOUT", "must be shorter than CONSUMER_VISIBILITY_TIMEOUT (30s), got 40s"},
				{"DB_MAX_CONNS", `must be an integer, got "abc"`},
				{"HTTP_READ_TIMEOUT", "must be positive, got -1s"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadEnv(envWith(tt.overrides))

			got := problemsOf(t, err)
			if !slices.Equal(got, tt.want) {
				t.Errorf("problems =\n%v\nwant\n%v", got, tt.want)
			}
		})
	}
}

func TestLoad_ProcessingTimeoutOutlastsKeySetFetch(t *testing.T) {
	tooShort := func(got time.Duration) []problem {
		return []problem{{"CONSUMER_PROCESSING_TIMEOUT", fmt.Sprintf(
			"must be longer than the %s the token verifier may take to fetch the key set, got %s",
			KeySetFetchTimeout, got)}}
	}
	tests := []struct {
		name      string
		overrides map[string]string
		want      []problem
	}{
		{
			name:      "just longer than the fetch",
			overrides: map[string]string{"CONSUMER_PROCESSING_TIMEOUT": (KeySetFetchTimeout + time.Millisecond).String()},
		},
		{
			name:      "as long as the fetch",
			overrides: map[string]string{"CONSUMER_PROCESSING_TIMEOUT": KeySetFetchTimeout.String()},
			want:      tooShort(KeySetFetchTimeout),
		},
		{
			name:      "shorter than the fetch",
			overrides: map[string]string{"CONSUMER_PROCESSING_TIMEOUT": "3s"},
			want:      tooShort(3 * time.Second),
		},
		{
			name: "under a visibility timeout of the fetch's length",
			overrides: map[string]string{
				"CONSUMER_VISIBILITY_TIMEOUT": "4s",
				"CONSUMER_PROCESSING_TIMEOUT": "3s",
			},
			want: tooShort(3 * time.Second),
		},
		{
			name: "shorter than the fetch with the consumer off",
			overrides: map[string]string{
				"CONSUMER_ENABLED":            "false",
				"CONSUMER_PROCESSING_TIMEOUT": "3s",
			},
		},
		{
			name: "shorter than the fetch with the consumer on",
			overrides: map[string]string{
				"CONSUMER_ENABLED":            "true",
				"CONSUMER_PROCESSING_TIMEOUT": "3s",
			},
			want: tooShort(3 * time.Second),
		},
		{
			name: "not checked when the consumer switch failed to load",
			overrides: map[string]string{
				"CONSUMER_ENABLED":            "maybe",
				"CONSUMER_PROCESSING_TIMEOUT": "3s",
			},
			want: []problem{{"CONSUMER_ENABLED", `must be a boolean, got "maybe"`}},
		},
		{
			name: "not checked against a timeout that failed to load",
			overrides: map[string]string{
				"CONSUMER_PROCESSING_TIMEOUT": "soon",
			},
			want: []problem{{
				"CONSUMER_PROCESSING_TIMEOUT",
				`must be a duration such as 500ms or 30s, got "soon"`,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadEnv(envWith(tt.overrides))

			if tt.want == nil {
				if err != nil {
					t.Fatalf("Load() error = %v, want nil", err)
				}
				return
			}
			got := problemsOf(t, err)
			if !slices.Equal(got, tt.want) {
				t.Errorf("problems =\n%v\nwant\n%v", got, tt.want)
			}
		})
	}
}

func TestLoad_RequestTimeoutOutlastsKeySetFetch(t *testing.T) {
	tooShort := func(got time.Duration) []problem {
		return []problem{{"HTTP_REQUEST_TIMEOUT", fmt.Sprintf(
			"must be longer than the %s the token verifier may take to fetch the key set, got %s",
			KeySetFetchTimeout, got)}}
	}
	tests := []struct {
		name      string
		overrides map[string]string
		want      []problem
	}{
		{
			name:      "just longer than the fetch",
			overrides: map[string]string{"HTTP_REQUEST_TIMEOUT": (KeySetFetchTimeout + time.Millisecond).String()},
		},
		{
			name:      "as long as the fetch",
			overrides: map[string]string{"HTTP_REQUEST_TIMEOUT": KeySetFetchTimeout.String()},
			want:      tooShort(KeySetFetchTimeout),
		},
		{
			name:      "shorter than the fetch",
			overrides: map[string]string{"HTTP_REQUEST_TIMEOUT": "2s"},
			want:      tooShort(2 * time.Second),
		},
		{
			name: "shorter than the fetch with the API off",
			overrides: map[string]string{
				"HTTP_ENABLED":         "false",
				"HTTP_REQUEST_TIMEOUT": "2s",
			},
		},
		{
			name: "not checked when the API switch failed to load",
			overrides: map[string]string{
				"HTTP_ENABLED":         "maybe",
				"HTTP_REQUEST_TIMEOUT": "2s",
			},
			want: []problem{{"HTTP_ENABLED", `must be a boolean, got "maybe"`}},
		},
		{
			name:      "not checked against a timeout that failed to load",
			overrides: map[string]string{"HTTP_REQUEST_TIMEOUT": "soon"},
			want: []problem{{
				"HTTP_REQUEST_TIMEOUT",
				`must be a duration such as 500ms or 30s, got "soon"`,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadEnv(envWith(tt.overrides))

			if tt.want == nil {
				if err != nil {
					t.Fatalf("Load() error = %v, want nil", err)
				}
				return
			}
			got := problemsOf(t, err)
			if !slices.Equal(got, tt.want) {
				t.Errorf("problems =\n%v\nwant\n%v", got, tt.want)
			}
		})
	}
}

func TestLoad_RequestTimeoutLeavesTheRollbackItsTime(t *testing.T) {
	tooLong := func(write, got time.Duration) []problem {
		return []problem{{"HTTP_REQUEST_TIMEOUT", fmt.Sprintf(
			"must be at most HTTP_WRITE_TIMEOUT (%s) less the %s a rollback may take, so the answer can still be written, got %s",
			write, RollbackTimeout, got)}}
	}
	tests := []struct {
		name      string
		overrides map[string]string
		want      []problem
	}{
		{
			name:      "the rollback fits exactly",
			overrides: map[string]string{"HTTP_WRITE_TIMEOUT": "20s", "HTTP_REQUEST_TIMEOUT": (20*time.Second - RollbackTimeout).String()},
		},
		{
			name: "the rollback does not fit",
			overrides: map[string]string{
				"HTTP_WRITE_TIMEOUT":   "20s",
				"HTTP_REQUEST_TIMEOUT": (20*time.Second - RollbackTimeout + time.Millisecond).String(),
			},
			want: tooLong(20*time.Second, 20*time.Second-RollbackTimeout+time.Millisecond),
		},
		{
			name:      "as long as the write timeout",
			overrides: map[string]string{"HTTP_REQUEST_TIMEOUT": "15s"},
			want:      tooLong(15*time.Second, 15*time.Second),
		},
		{
			name:      "not checked against an unparsable write timeout",
			overrides: map[string]string{"HTTP_REQUEST_TIMEOUT": "20s", "HTTP_WRITE_TIMEOUT": "later"},
			want:      []problem{{"HTTP_WRITE_TIMEOUT", `must be a duration such as 500ms or 30s, got "later"`}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadEnv(envWith(tt.overrides))

			if tt.want == nil {
				if err != nil {
					t.Fatalf("Load() error = %v, want nil", err)
				}
				return
			}
			got := problemsOf(t, err)
			if !slices.Equal(got, tt.want) {
				t.Errorf("problems =\n%v\nwant\n%v", got, tt.want)
			}
		})
	}
}

func TestLoad_ShutdownBudget(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]string
		want      []problem
	}{
		{
			name:      "exactly the sum of the defaults",
			overrides: map[string]string{"SHUTDOWN_TIMEOUT": "49s"},
		},
		{
			name:      "short of the sum of the defaults",
			overrides: map[string]string{"SHUTDOWN_TIMEOUT": "48s"},
			want: []problem{{"SHUTDOWN_TIMEOUT", "must be at least 49s to cover SHUTDOWN_DRAIN_DELAY (2s) + " +
				"OUTBOX_LEASE + 2s to release it (32s) + HTTP_SHUTDOWN_TIMEOUT (10s) + 5s of margin, got 48s"}},
		},
		{
			name: "the consumer is the slowest worker, and its give-backs count",
			overrides: map[string]string{
				"CONSUMER_DRAIN_TIMEOUT": "45s",
				"SHUTDOWN_TIMEOUT":       "1m7s",
			},
		},
		{
			name: "short of the consumer's drain and its give-backs",
			overrides: map[string]string{
				"CONSUMER_DRAIN_TIMEOUT": "45s",
				"SHUTDOWN_TIMEOUT":       "1m6s",
			},
			want: []problem{{"SHUTDOWN_TIMEOUT", "must be at least 1m7s to cover SHUTDOWN_DRAIN_DELAY (2s) + " +
				"CONSUMER_DRAIN_TIMEOUT + 5s of give-backs (50s) + HTTP_SHUTDOWN_TIMEOUT (10s) + 5s of margin, got 1m6s"}},
		},
		{
			name: "the give-backs make the consumer slower than the relay",
			overrides: map[string]string{
				"CONSUMER_DRAIN_TIMEOUT": "30s",
				"SHUTDOWN_TIMEOUT":       "51s",
			},
			want: []problem{{"SHUTDOWN_TIMEOUT", "must be at least 52s to cover SHUTDOWN_DRAIN_DELAY (2s) + " +
				"CONSUMER_DRAIN_TIMEOUT + 5s of give-backs (35s) + HTTP_SHUTDOWN_TIMEOUT (10s) + 5s of margin, got 51s"}},
		},
		{
			name: "the resolver is the slowest worker",
			overrides: map[string]string{
				"RESOLVER_LEASE":   "1m",
				"SHUTDOWN_TIMEOUT": "1m",
			},
			want: []problem{{"SHUTDOWN_TIMEOUT", "must be at least 1m17s to cover SHUTDOWN_DRAIN_DELAY (2s) + " +
				"RESOLVER_LEASE (1m0s) + HTTP_SHUTDOWN_TIMEOUT (10s) + 5s of margin, got 1m0s"}},
		},
		{
			name: "a worker switched off takes no part",
			overrides: map[string]string{
				"OUTBOX_RELAY_ENABLED": "false",
				"OUTBOX_LEASE":         "5m",
				"RESOLVER_LEASE":       "20s",
				"SHUTDOWN_TIMEOUT":     "42s",
			},
		},
		{
			name: "without the API there is neither drain nor HTTP shutdown",
			overrides: map[string]string{
				"HTTP_ENABLED":          "false",
				"SHUTDOWN_DRAIN_DELAY":  "1h",
				"HTTP_SHUTDOWN_TIMEOUT": "1h",
				"SHUTDOWN_TIMEOUT":      "36s",
			},
			want: []problem{{"SHUTDOWN_TIMEOUT", "must be at least 37s to cover OUTBOX_LEASE + 2s to release it (32s) + " +
				"5s of margin, got 36s"}},
		},
		{
			name: "with everything off only the margin is left",
			overrides: map[string]string{
				"HTTP_ENABLED":         "false",
				"CONSUMER_ENABLED":     "false",
				"OUTBOX_RELAY_ENABLED": "false",
				"RESOLVER_ENABLED":     "false",
				"SHUTDOWN_TIMEOUT":     "4s",
			},
			want: []problem{{"SHUTDOWN_TIMEOUT", "must be at least 5s to cover 5s of margin, got 4s"}},
		},
		{
			name: "budgets too large to add up still fail",
			overrides: map[string]string{
				"SHUTDOWN_DRAIN_DELAY":  "2562047h",
				"HTTP_SHUTDOWN_TIMEOUT": "2562047h",
				"SHUTDOWN_TIMEOUT":      "2562047h",
			},
			want: []problem{{"SHUTDOWN_TIMEOUT", "must be at least 2562047h47m16.854775807s to cover " +
				"SHUTDOWN_DRAIN_DELAY (2562047h0m0s) + OUTBOX_LEASE + 2s to release it (32s) + " +
				"HTTP_SHUTDOWN_TIMEOUT (2562047h0m0s) + 5s of margin, got 2562047h0m0s"}},
		},
		{
			name: "a drain too large to add the give-backs to still fails",
			overrides: map[string]string{
				"CONSUMER_DRAIN_TIMEOUT": "2562047h47m16.854775807s",
				"SHUTDOWN_TIMEOUT":       "2562047h",
			},
			want: []problem{{"SHUTDOWN_TIMEOUT", "must be at least 2562047h47m16.854775807s to cover " +
				"SHUTDOWN_DRAIN_DELAY (2s) + CONSUMER_DRAIN_TIMEOUT + 5s of give-backs (2562047h47m16.854775807s) + " +
				"HTTP_SHUTDOWN_TIMEOUT (10s) + 5s of margin, got 2562047h0m0s"}},
		},
		{
			name: "tracing adds the flush of the spans still queued",
			overrides: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://jaeger:4318",
				"SHUTDOWN_TIMEOUT":            "53s",
			},
			want: []problem{{"SHUTDOWN_TIMEOUT", "must be at least 54s to cover SHUTDOWN_DRAIN_DELAY (2s) + " +
				"OUTBOX_LEASE + 2s to release it (32s) + HTTP_SHUTDOWN_TIMEOUT (10s) + " +
				"the flush of the spans (5s) + 5s of margin, got 53s"}},
		},
		{
			name: "nor against an endpoint that failed to load",
			overrides: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "jaeger:4318",
				"SHUTDOWN_TIMEOUT":            "48s",
			},
			want: []problem{{"OTEL_EXPORTER_OTLP_ENDPOINT", "must be an absolute http or https URL, such as http://jaeger:4318"}},
		},
		{
			name: "the sum is not checked against a budget that failed to load",
			overrides: map[string]string{
				"OUTBOX_LEASE":     "soon",
				"SHUTDOWN_TIMEOUT": "1s",
			},
			want: []problem{{"OUTBOX_LEASE", `must be a duration such as 500ms or 30s, got "soon"`}},
		},
		{
			name:      "nor against a timeout that failed to load",
			overrides: map[string]string{"SHUTDOWN_TIMEOUT": "-1s"},
			want:      []problem{{"SHUTDOWN_TIMEOUT", "must be positive, got -1s"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadEnv(envWith(tt.overrides))

			if tt.want == nil {
				if err != nil {
					t.Fatalf("Load() error = %v, want nil", err)
				}
				return
			}
			got := problemsOf(t, err)
			if !slices.Equal(got, tt.want) {
				t.Errorf("problems =\n%v\nwant\n%v", got, tt.want)
			}
		})
	}
}

func TestLoad_Tracing(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]string
		want      TracingConfig
	}{
		{
			name: "off without an endpoint",
			want: TracingConfig{ServiceName: "wallet"},
		},
		{
			name:      "on with an endpoint",
			overrides: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://jaeger:4318"},
			want:      TracingConfig{Endpoint: "http://jaeger:4318", ServiceName: "wallet"},
		},
		{
			name: "named after the service the operator chose",
			overrides: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.internal/otlp/",
				"OTEL_SERVICE_NAME":           "wallet-eu",
			},
			want: TracingConfig{Endpoint: "https://collector.internal/otlp/", ServiceName: "wallet-eu"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadEnv(envWith(tt.overrides))
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got.Tracing != tt.want {
				t.Errorf("Tracing = %+v, want %+v", got.Tracing, tt.want)
			}
			if on := tt.want.Endpoint != ""; got.Tracing.Enabled() != on {
				t.Errorf("Enabled() = %t, want %t", got.Tracing.Enabled(), on)
			}
		})
	}
}

func TestLoad_TracingEndpointMustBeAnHTTPURL(t *testing.T) {
	for _, raw := range []string{"jaeger:4318", "grpc://jaeger:4317", "http://", "/v1/traces", "http://jae ger:4318"} {
		t.Run(raw, func(t *testing.T) {
			_, err := loadEnv(envWith(map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": raw}))

			got := problemsOf(t, err)
			want := []problem{{"OTEL_EXPORTER_OTLP_ENDPOINT", "must be an absolute http or https URL, such as http://jaeger:4318"}}
			if !slices.Equal(got, want) {
				t.Errorf("problems =\n%v\nwant\n%v", got, want)
			}
		})
	}
}

func TestTracingConfig_TracesURL(t *testing.T) {
	tests := map[string]string{
		"http://jaeger:4318":               "http://jaeger:4318/v1/traces",
		"http://jaeger:4318/":              "http://jaeger:4318/v1/traces",
		"https://collector.internal/otlp/": "https://collector.internal/otlp/v1/traces",
	}
	for endpoint, want := range tests {
		if got := (TracingConfig{Endpoint: endpoint}).TracesURL(); got != want {
			t.Errorf("TracesURL() of %q = %q, want %q", endpoint, got, want)
		}
	}
}

func TestStopBudget(t *testing.T) {
	const largest = time.Duration(1<<63 - 1)
	tests := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"consumer with the default drain", ConsumerConfig{DrainTimeout: 20 * time.Second}.StopBudget(), 25 * time.Second},
		{"consumer without a drain", ConsumerConfig{}.StopBudget(), 5 * time.Second},
		{"consumer with the longest drain", ConsumerConfig{DrainTimeout: largest}.StopBudget(), largest},
		{"relay with the default lease", OutboxConfig{Lease: 30 * time.Second}.StopBudget(), 32 * time.Second},
		{"relay with the longest lease", OutboxConfig{Lease: largest}.StopBudget(), largest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("StopBudget() = %s, want %s", tt.got, tt.want)
			}
		})
	}
}

func TestLoad_HostnameUnavailable(t *testing.T) {
	failing := func() (string, error) { return "", errors.New("no host") }

	_, err := load(lookupFrom(requiredEnv()), failing)

	got := problemsOf(t, err)
	if len(got) != 1 || got[0].name != "APP_INSTANCE_ID" {
		t.Fatalf("problems = %v, want a single APP_INSTANCE_ID problem", got)
	}
	if !strings.Contains(got[0].reason, "no host") {
		t.Errorf("reason %q does not carry the host name error", got[0].reason)
	}
}

func TestLoad_HostnameNotConsultedWhenInstanceIDSet(t *testing.T) {
	failing := func() (string, error) { return "", errors.New("must not be called") }
	env := envWith(map[string]string{"APP_INSTANCE_ID": "replica-1"})

	got, err := load(lookupFrom(env), failing)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.InstanceID != "replica-1" {
		t.Errorf("InstanceID = %q, want replica-1", got.InstanceID)
	}
}

func TestValidationError_Message(t *testing.T) {
	err := &validationError{problems: []problem{
		{"DATABASE_URL", "is required"},
		{"DB_MAX_CONNS", `must be an integer, got "abc"`},
	}}

	want := `invalid configuration: DATABASE_URL: is required; DB_MAX_CONNS: must be an integer, got "abc"`
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}
