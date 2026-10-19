// Package config loads the service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	maxBatchSize   = 500
	shutdownMargin = 5 * time.Second
)

// MaxConsumerWaitTime is the longest long poll SQS accepts.
const MaxConsumerWaitTime = 20 * time.Second

// Timeouts of single operations, which the request and shutdown budgets are checked against.
const (
	SQSCallTimeout       = 5 * time.Second
	OutboxReleaseTimeout = 2 * time.Second
	RollbackTimeout      = 5 * time.Second
	KeySetFetchTimeout   = 5 * time.Second
	SpanFlushTimeout     = 5 * time.Second
)

// Config is the complete, validated configuration of one service instance.
type Config struct {
	InstanceID         string
	LogLevel           slog.Level
	ShutdownDrainDelay time.Duration
	ShutdownTimeout    time.Duration

	HTTP     HTTPConfig
	Admin    AdminConfig
	DB       DBConfig
	OIDC     OIDCConfig
	SQS      SQSConfig
	Consumer ConsumerConfig
	Outbox   OutboxConfig
	Resolver ResolverConfig
	Tracing  TracingConfig
}

// HTTPConfig configures the public API server.
type HTTPConfig struct {
	Enabled bool
	Addr    string

	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	RequestTimeout    time.Duration

	MaxBodyBytes   int64
	MaxHeaderBytes int
}

// AdminConfig configures the operational server that serves /metrics.
type AdminConfig struct {
	Addr string
}

// DBConfig configures the PostgreSQL connection pool and transactions.
type DBConfig struct {
	URL              string
	MaxConns         int32
	LockTimeout      time.Duration
	StatementTimeout time.Duration
	TxMaxAttempts    int
}

// OIDCConfig configures validation of the access tokens presented to the API.
type OIDCConfig struct {
	Issuer    string
	JWKSURL   string
	Audience  string
	ClockSkew time.Duration
}

// SQSConfig names the queues.
type SQSConfig struct {
	InputQueue  string
	InputDLQ    string
	EventsQueue string
}

// ConsumerConfig configures the worker that reads the input queue.
type ConsumerConfig struct {
	Enabled           bool
	Name              string
	MaxGroups         int
	VisibilityTimeout time.Duration
	ProcessingTimeout time.Duration
	DrainTimeout      time.Duration
	WaitTime          time.Duration
	MaxReceives       int
}

// OutboxConfig configures the relay that publishes the outbox to the events queue.
type OutboxConfig struct {
	Enabled      bool
	PollInterval time.Duration
	BatchSize    int
	Lease        time.Duration
	BackoffMax   time.Duration
}

// StopBudget is how long the consumer may take to stop.
func (c ConsumerConfig) StopBudget() time.Duration {
	return addCapped(c.DrainTimeout, SQSCallTimeout)
}

// StopBudget is how long the relay may take to stop.
func (o OutboxConfig) StopBudget() time.Duration {
	return addCapped(o.Lease, OutboxReleaseTimeout)
}

// ResolverConfig configures the background resolver of transaction references.
type ResolverConfig struct {
	Enabled               bool
	PollInterval          time.Duration
	BatchSize             int
	Lease                 time.Duration
	MaxAttempts           int
	TTL                   time.Duration
	BackoffBase           time.Duration
	BackoffMax            time.Duration
	PermanentFailureLimit int
}

// TracingConfig configures the export of traces; with no endpoint, tracing is off.
type TracingConfig struct {
	Endpoint    string
	ServiceName string
}

// Enabled reports whether spans are recorded and exported.
func (t TracingConfig) Enabled() bool { return t.Endpoint != "" }

// TracesURL is where the OTLP/HTTP exporter sends spans: the endpoint with the traces path appended.
func (t TracingConfig) TracesURL() string {
	u, err := url.Parse(t.Endpoint)
	if err != nil {
		return ""
	}
	return u.JoinPath("v1", "traces").String()
}

type problem struct {
	name   string
	reason string
}

type validationError struct {
	problems []problem
}

func (e *validationError) Error() string {
	parts := make([]string, len(e.problems))
	for i, p := range e.problems {
		parts[i] = p.name + ": " + p.reason
	}
	return "invalid configuration: " + strings.Join(parts, "; ")
}

// Load reads and validates the configuration through lookup, reporting every problem at once.
func Load(lookup func(string) (string, bool)) (Config, error) {
	return load(lookup, os.Hostname)
}

func load(lookup func(string) (string, bool), hostname func() (string, error)) (Config, error) {
	e := &env{lookup: lookup}

	cfg := Config{
		InstanceID:         instanceID(e, hostname),
		LogLevel:           logLevel(e),
		ShutdownDrainDelay: duration(e, "SHUTDOWN_DRAIN_DELAY", 2*time.Second),
		ShutdownTimeout:    duration(e, "SHUTDOWN_TIMEOUT", 60*time.Second),
		HTTP: HTTPConfig{
			Enabled:           boolean(e, "HTTP_ENABLED", true),
			Addr:              str(e, "HTTP_ADDR", ":8080"),
			ReadHeaderTimeout: duration(e, "HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
			ReadTimeout:       duration(e, "HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout:      duration(e, "HTTP_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:       duration(e, "HTTP_IDLE_TIMEOUT", 60*time.Second),
			ShutdownTimeout:   duration(e, "HTTP_SHUTDOWN_TIMEOUT", 10*time.Second),
			RequestTimeout:    duration(e, "HTTP_REQUEST_TIMEOUT", 10*time.Second),
			MaxBodyBytes:      integer(e, "HTTP_MAX_BODY_BYTES", int64(65536), 1),
			MaxHeaderBytes:    integer(e, "HTTP_MAX_HEADER_BYTES", 16384, 1),
		},
		Admin: AdminConfig{
			Addr: str(e, "ADMIN_ADDR", ":9090"),
		},
		DB: DBConfig{
			URL:              required(e, "DATABASE_URL"),
			MaxConns:         integer(e, "DB_MAX_CONNS", int32(20), 1),
			LockTimeout:      duration(e, "DB_LOCK_TIMEOUT", 2*time.Second),
			StatementTimeout: duration(e, "DB_STATEMENT_TIMEOUT", 5*time.Second),
			TxMaxAttempts:    integer(e, "DB_TX_MAX_ATTEMPTS", 3, 1),
		},
		OIDC: OIDCConfig{
			Issuer:    required(e, "OIDC_ISSUER"),
			JWKSURL:   required(e, "OIDC_JWKS_URL"),
			Audience:  str(e, "OIDC_AUDIENCE", "wagering-api"),
			ClockSkew: duration(e, "OIDC_CLOCK_SKEW", 30*time.Second),
		},
		SQS: SQSConfig{
			InputQueue:  str(e, "SQS_INPUT_QUEUE", "wager-transactions.fifo"),
			InputDLQ:    str(e, "SQS_INPUT_DLQ", "wager-transactions-dlq.fifo"),
			EventsQueue: str(e, "SQS_EVENTS_QUEUE", "wallet-events.fifo"),
		},
		Consumer: ConsumerConfig{
			Enabled:           boolean(e, "CONSUMER_ENABLED", true),
			Name:              str(e, "CONSUMER_NAME", "wager-transactions"),
			MaxGroups:         integer(e, "CONSUMER_MAX_GROUPS", 8, 1),
			VisibilityTimeout: duration(e, "CONSUMER_VISIBILITY_TIMEOUT", 30*time.Second),
			ProcessingTimeout: duration(e, "CONSUMER_PROCESSING_TIMEOUT", 15*time.Second),
			DrainTimeout:      duration(e, "CONSUMER_DRAIN_TIMEOUT", 20*time.Second),
			WaitTime:          duration(e, "CONSUMER_WAIT_TIME", 20*time.Second),
			MaxReceives:       integer(e, "CONSUMER_MAX_RECEIVES", 10, 1),
		},
		Outbox: OutboxConfig{
			Enabled:      boolean(e, "OUTBOX_RELAY_ENABLED", true),
			PollInterval: duration(e, "OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
			BatchSize:    integerBetween(e, "OUTBOX_BATCH_SIZE", 50, 1, maxBatchSize),
			Lease:        duration(e, "OUTBOX_LEASE", 30*time.Second),
			BackoffMax:   duration(e, "OUTBOX_BACKOFF_MAX", 5*time.Minute),
		},
		Resolver: ResolverConfig{
			Enabled:               boolean(e, "RESOLVER_ENABLED", true),
			PollInterval:          duration(e, "RESOLVER_POLL_INTERVAL", time.Second),
			BatchSize:             integerBetween(e, "RESOLVER_BATCH_SIZE", 50, 1, maxBatchSize),
			Lease:                 duration(e, "RESOLVER_LEASE", 30*time.Second),
			MaxAttempts:           integer(e, "REFERENCE_MAX_ATTEMPTS", 10, 1),
			TTL:                   duration(e, "REFERENCE_TTL", 15*time.Minute),
			BackoffBase:           duration(e, "REFERENCE_BACKOFF_BASE", time.Second),
			BackoffMax:            duration(e, "REFERENCE_BACKOFF_MAX", 60*time.Second),
			PermanentFailureLimit: integer(e, "PERMANENT_FAILURE_LIMIT", 3, 1),
		},
		Tracing: TracingConfig{
			Endpoint:    otlpEndpoint(e),
			ServiceName: str(e, "OTEL_SERVICE_NAME", "wallet"),
		},
	}

	if !e.invalid("DB_LOCK_TIMEOUT", "DB_STATEMENT_TIMEOUT") && cfg.DB.LockTimeout >= cfg.DB.StatementTimeout {
		e.add("DB_LOCK_TIMEOUT", fmt.Sprintf("must be shorter than DB_STATEMENT_TIMEOUT (%s), got %s",
			cfg.DB.StatementTimeout, cfg.DB.LockTimeout))
	}
	if !e.invalid("CONSUMER_PROCESSING_TIMEOUT", "CONSUMER_VISIBILITY_TIMEOUT") &&
		cfg.Consumer.ProcessingTimeout >= cfg.Consumer.VisibilityTimeout {
		e.add("CONSUMER_PROCESSING_TIMEOUT", fmt.Sprintf(
			"must be shorter than CONSUMER_VISIBILITY_TIMEOUT (%s), got %s",
			cfg.Consumer.VisibilityTimeout, cfg.Consumer.ProcessingTimeout))
	}
	if !e.invalid("HTTP_REQUEST_TIMEOUT", "HTTP_WRITE_TIMEOUT") &&
		addCapped(cfg.HTTP.RequestTimeout, RollbackTimeout) > cfg.HTTP.WriteTimeout {
		e.add("HTTP_REQUEST_TIMEOUT", fmt.Sprintf(
			"must be at most HTTP_WRITE_TIMEOUT (%s) less the %s a rollback may take, so the answer can still be written, got %s",
			cfg.HTTP.WriteTimeout, RollbackTimeout, cfg.HTTP.RequestTimeout))
	}
	if !e.invalid("HTTP_ENABLED", "HTTP_REQUEST_TIMEOUT") &&
		cfg.HTTP.Enabled && cfg.HTTP.RequestTimeout <= KeySetFetchTimeout {
		e.add("HTTP_REQUEST_TIMEOUT", fmt.Sprintf(
			"must be longer than the %s the token verifier may take to fetch the key set, got %s",
			KeySetFetchTimeout, cfg.HTTP.RequestTimeout))
	}
	if !e.invalid("CONSUMER_ENABLED", "CONSUMER_PROCESSING_TIMEOUT") &&
		cfg.Consumer.Enabled && cfg.Consumer.ProcessingTimeout <= KeySetFetchTimeout {
		e.add("CONSUMER_PROCESSING_TIMEOUT", fmt.Sprintf(
			"must be longer than the %s the token verifier may take to fetch the key set, got %s",
			KeySetFetchTimeout, cfg.Consumer.ProcessingTimeout))
	}
	if !e.invalid("REFERENCE_BACKOFF_BASE", "REFERENCE_BACKOFF_MAX") && cfg.Resolver.BackoffBase > cfg.Resolver.BackoffMax {
		e.add("REFERENCE_BACKOFF_BASE", fmt.Sprintf("must be at most REFERENCE_BACKOFF_MAX (%s), got %s",
			cfg.Resolver.BackoffMax, cfg.Resolver.BackoffBase))
	}
	checkShutdownBudget(e, cfg)

	if w := cfg.Consumer.WaitTime; !e.invalid("CONSUMER_WAIT_TIME") && (w < time.Second || w > MaxConsumerWaitTime) {
		e.add("CONSUMER_WAIT_TIME", fmt.Sprintf("must be between 1s and %s, got %s", MaxConsumerWaitTime, w))
	}

	if err := e.err(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

type budget struct {
	name string
	d    time.Duration
}

func checkShutdownBudget(e *env, cfg Config) {
	if e.invalid("SHUTDOWN_TIMEOUT", "SHUTDOWN_DRAIN_DELAY", "HTTP_ENABLED", "HTTP_SHUTDOWN_TIMEOUT",
		"CONSUMER_ENABLED", "CONSUMER_DRAIN_TIMEOUT", "OUTBOX_RELAY_ENABLED", "OUTBOX_LEASE",
		"RESOLVER_ENABLED", "RESOLVER_LEASE", "OTEL_EXPORTER_OTLP_ENDPOINT") {
		return
	}
	workers := []struct {
		on bool
		budget
	}{
		{cfg.Consumer.Enabled, budget{
			fmt.Sprintf("CONSUMER_DRAIN_TIMEOUT + %s of give-backs", SQSCallTimeout), cfg.Consumer.StopBudget()}},
		{cfg.Outbox.Enabled, budget{
			fmt.Sprintf("OUTBOX_LEASE + %s to release it", OutboxReleaseTimeout), cfg.Outbox.StopBudget()}},
		{cfg.Resolver.Enabled, budget{"RESOLVER_LEASE", cfg.Resolver.Lease}},
	}
	var slowest budget
	for _, w := range workers {
		if w.on && w.d > slowest.d {
			slowest = w.budget
		}
	}

	var steps []budget
	if cfg.HTTP.Enabled {
		steps = append(steps, budget{"SHUTDOWN_DRAIN_DELAY", cfg.ShutdownDrainDelay})
	}
	if slowest.d > 0 {
		steps = append(steps, slowest)
	}
	if cfg.HTTP.Enabled {
		steps = append(steps, budget{"HTTP_SHUTDOWN_TIMEOUT", cfg.HTTP.ShutdownTimeout})
	}
	if cfg.Tracing.Enabled() {
		steps = append(steps, budget{"the flush of the spans", SpanFlushTimeout})
	}

	need := shutdownMargin
	parts := make([]string, 0, len(steps)+1)
	for _, s := range steps {
		need = addCapped(need, s.d)
		parts = append(parts, fmt.Sprintf("%s (%s)", s.name, s.d))
	}
	if need <= cfg.ShutdownTimeout {
		return
	}
	parts = append(parts, fmt.Sprintf("%s of margin", shutdownMargin))
	e.add("SHUTDOWN_TIMEOUT", fmt.Sprintf("must be at least %s to cover %s, got %s",
		need, strings.Join(parts, " + "), cfg.ShutdownTimeout))
}

func addCapped(a, b time.Duration) time.Duration {
	if b > math.MaxInt64-a {
		return math.MaxInt64
	}
	return a + b
}

func instanceID(e *env, hostname func() (string, error)) string {
	if v, ok := e.value("APP_INSTANCE_ID"); ok {
		return v
	}
	name, err := hostname()
	if err != nil {
		e.add("APP_INSTANCE_ID", fmt.Sprintf("is not set and the host name is unavailable: %v", err))
		return ""
	}
	return name
}

func otlpEndpoint(e *env) string {
	return parsed(e, "OTEL_EXPORTER_OTLP_ENDPOINT", "", func(raw string) (string, error) {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return "", errors.New("must be an absolute http or https URL, such as http://jaeger:4318")
		}
		return raw, nil
	})
}

func logLevel(e *env) slog.Level {
	return parsed(e, "LOG_LEVEL", slog.LevelInfo, func(raw string) (slog.Level, error) {
		switch strings.ToLower(raw) {
		case "debug":
			return slog.LevelDebug, nil
		case "info":
			return slog.LevelInfo, nil
		case "warn":
			return slog.LevelWarn, nil
		case "error":
			return slog.LevelError, nil
		}
		return 0, fmt.Errorf("must be one of debug, info, warn or error, got %q", raw)
	})
}
