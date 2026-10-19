//go:build load

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	phaseMixed      = "mixed"
	phaseContention = "contention"
)

type config struct {
	target         string
	keycloak       string
	metrics        []string
	inputQueue     string
	eventsQueue    string
	concurrency    []int
	phases         []string
	warmup         time.Duration
	duration       time.Duration
	wallets        int
	hotWallets     int
	sqsShare       int
	replayShare    int
	crossShare     int
	sqsBacklog     int
	sqsSenders     int
	initialBalance int64
	seed           uint64
	eventReaders   int
	sampleEvery    time.Duration
	drainTimeout   time.Duration
	out            string
}

type configReport struct {
	Target              string   `json:"target"`
	Metrics             []string `json:"metrics"`
	Concurrency         []int    `json:"concurrency"`
	Phases              []string `json:"phases"`
	WarmupMs            int64    `json:"warmupMs"`
	DurationMs          int64    `json:"durationMs"`
	Wallets             int      `json:"wallets"`
	HotWallets          int      `json:"hotWallets"`
	SQSSharePercent     int      `json:"sqsSharePercent"`
	ReplaySharePercent  int      `json:"replaySharePercent"`
	CrossSharePercent   int      `json:"crossSharePercent"`
	SQSBacklog          int      `json:"sqsBacklog"`
	SQSSenders          int      `json:"sqsSenders"`
	InitialBalance      string   `json:"initialBalance"`
	Seed                uint64   `json:"seed"`
	EventReaders        int      `json:"eventReaders"`
	SampleEveryMs       int64    `json:"sampleEveryMs"`
	DrainTimeoutSeconds int64    `json:"drainTimeoutSeconds"`
}

func (c config) report() configReport {
	return configReport{
		Target: c.target, Metrics: c.metrics, Concurrency: c.concurrency, Phases: c.phases,
		WarmupMs: c.warmup.Milliseconds(), DurationMs: c.duration.Milliseconds(),
		Wallets: c.wallets, HotWallets: c.hotWallets, SQSSharePercent: c.sqsShare,
		ReplaySharePercent: c.replayShare, CrossSharePercent: c.crossShare, SQSBacklog: c.sqsBacklog, SQSSenders: c.sqsSenders,
		InitialBalance: formatMinor(c.initialBalance), Seed: c.seed, EventReaders: c.eventReaders,
		SampleEveryMs: c.sampleEvery.Milliseconds(), DrainTimeoutSeconds: int64(c.drainTimeout / time.Second),
	}
}

func parseConfig(args []string, stderr io.Writer) (config, error) {
	var c config
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.target, "target", "http://nginx", "base URL of the API, through the load balancer")
	fs.StringVar(&c.keycloak, "keycloak", "http://keycloak:8080", "base URL of Keycloak")
	metrics := fs.String("metrics", "http://app-1:9090/metrics,http://app-2:9090/metrics,http://app-3:9090/metrics",
		"comma-separated /metrics URLs, one per replica")
	fs.StringVar(&c.inputQueue, "input-queue", "wager-transactions.fifo", "queue providers send transactions to")
	fs.StringVar(&c.eventsQueue, "events-queue", "wallet-events.fifo", "queue the service publishes events to")
	concurrency := fs.String("concurrency", "16,64", "comma-separated concurrency levels, each run through every phase")
	phases := fs.String("phases", phaseMixed+","+phaseContention, "comma-separated phases: mixed, contention")
	fs.DurationVar(&c.warmup, "warmup", 5*time.Second, "warm-up of each phase, left out of the measurements")
	fs.DurationVar(&c.duration, "duration", 10*time.Second, "measured window of each phase")
	fs.IntVar(&c.wallets, "wallets", 200, "wallets of the mixed phase")
	fs.IntVar(&c.hotWallets, "hot-wallets", 5, "wallets of the contention phase")
	fs.IntVar(&c.sqsShare, "sqs-share", 20, "percent of rounds sent over SQS")
	fs.IntVar(&c.replayShare, "replay-share", 5, "percent of rounds that resend one operation with the same key")
	fs.IntVar(&c.crossShare, "cross-share", 5, "percent of rounds that resend one operation over the other channel")
	fs.IntVar(&c.sqsBacklog, "sqs-backlog", 500, "input queue messages sent and not yet consumed above which SQS rounds go over HTTP")
	fs.IntVar(&c.sqsSenders, "sqs-senders", 4, "goroutines that send the SQS rounds, apart from the HTTP workers")
	balance := fs.String("initial-balance", "10000000.00", "initial balance of every wallet, in BRL")
	fs.Uint64Var(&c.seed, "seed", 1, "seed of the operation mix")
	fs.IntVar(&c.eventReaders, "event-readers", 8, "concurrent receivers of the events queue")
	fs.DurationVar(&c.sampleEvery, "sample-every", 250*time.Millisecond, "interval between /metrics samples")
	fs.DurationVar(&c.drainTimeout, "drain-timeout", 30*time.Minute, "how long to wait for queues and outbox to drain")
	fs.StringVar(&c.out, "out", "/out", "directory the JSON report is written to")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected arguments %q", fs.Args())
	}

	var errs []error
	var err error
	c.metrics = splitList(*metrics)
	c.phases = splitList(*phases)
	if c.concurrency, err = parseInts(*concurrency); err != nil {
		errs = append(errs, fmt.Errorf("-concurrency: %w", err))
	}
	if c.initialBalance, err = parseMinor(*balance); err != nil || c.initialBalance <= 0 {
		errs = append(errs, fmt.Errorf("-initial-balance must be a positive amount like 1000.00, got %q", *balance))
	}
	errs = append(errs, c.validate()...)
	if err := errors.Join(errs...); err != nil {
		return config{}, err
	}
	return c, nil
}

func (c config) validate() []error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}
	check(len(c.metrics) > 0, "-metrics needs at least one URL")
	check(len(c.phases) > 0, "-phases needs at least one phase")
	for _, p := range c.phases {
		check(p == phaseMixed || p == phaseContention, "-phases: unknown phase %q", p)
	}
	check(c.warmup >= 0, "-warmup must not be negative")
	check(c.duration > 0, "-duration must be positive")
	check(c.wallets > 0 && c.hotWallets > 0, "-wallets and -hot-wallets must be positive")
	for _, share := range []int{c.sqsShare, c.replayShare, c.crossShare} {
		check(share >= 0 && share <= 100, "shares are percentages from 0 to 100")
	}
	check(c.replayShare+c.crossShare <= 100, "-replay-share plus -cross-share must not pass 100")
	check(c.sqsBacklog > 0, "-sqs-backlog must be positive")
	check(c.sqsSenders > 0, "-sqs-senders must be positive")
	check(c.eventReaders > 0, "-event-readers must be positive")
	check(c.sampleEvery > 0, "-sample-every must be positive")
	check(c.drainTimeout > 0, "-drain-timeout must be positive")
	return errs
}

func splitList(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseInts(s string) ([]int, error) {
	parts := splitList(s)
	if len(parts) == 0 {
		return nil, errors.New("needs at least one value")
	}
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("%q is not a positive integer", part)
		}
		if !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	return out, nil
}
