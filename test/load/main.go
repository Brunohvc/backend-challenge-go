//go:build load

// Command load drives the compose stack with a reproducible workload and reports throughput, latency and integrity.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
)

const readyTimeout = 2 * time.Minute

type runner struct {
	cfg      config
	mix      mix
	runID    string
	started  time.Time
	ids      idgen.V7
	api      *apiClient
	producer *producer
	events   *collector
	scraper  *scraper
	sampler  *sampler
}

func (r *runner) since() int64 {
	return time.Since(r.started).Nanoseconds()
}

func (r *runner) sleepUntil(ctx context.Context, ns int64) {
	if d := time.Duration(ns - r.since()); d > 0 {
		sleep(ctx, d)
	}
}

func (r *runner) progress(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s %s\n", time.Now().UTC().Format(time.TimeOnly), fmt.Sprintf(format, args...))
}

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "load: %v\n", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	passed, err := run(ctx, cfg)
	stop()
	switch {
	case err != nil:
		fmt.Fprintf(os.Stderr, "load: %v\n", err)
		os.Exit(1)
	case !passed:
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) (bool, error) {
	r, err := newRunner(ctx, cfg)
	if err != nil {
		return false, err
	}
	r.progress("load test %s against %s", r.runID, cfg.target)
	if err := r.waitReady(ctx); err != nil {
		return false, err
	}
	if err := r.sampler.prime(ctx); err != nil {
		return false, err
	}

	bgCtx, cancel := context.WithCancel(ctx)
	var bg sync.WaitGroup
	for range cfg.eventReaders {
		bg.Go(func() { r.events.run(bgCtx) })
	}
	bg.Go(func() { r.sampler.run(bgCtx) })

	rep := runReport{
		RunID: r.runID, StartedAt: r.started.UTC().Format(time.RFC3339),
		Environment: currentEnvironment(), Config: cfg.report(), Passed: true,
	}
	runErr := r.runPhases(ctx, &rep)
	cancel()
	bg.Wait()

	rep.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	if runErr != nil {
		rep.Passed, rep.Error = false, runErr.Error()
	}
	r.events.view(func(c *collector) {
		rep.Events = eventsReport{Received: int64(len(c.seen)), Redelivered: c.redelivered, Malformed: c.malformed, Errors: c.errors}
	})
	fmt.Print(summary(rep))
	path, err := writeJSON(cfg.out, rep)
	if err != nil {
		return false, errors.Join(runErr, err)
	}
	fmt.Printf("report: %s\n", path)
	return rep.Passed, runErr
}

func (r *runner) runPhases(ctx context.Context, rep *runReport) error {
	for _, conc := range r.cfg.concurrency {
		for _, name := range r.cfg.phases {
			p, err := r.runPhase(ctx, name, conc)
			if err != nil {
				return fmt.Errorf("%s with concurrency %d: %w", name, conc, err)
			}
			rep.Phases = append(rep.Phases, p)
			rep.Passed = rep.Passed && p.Integrity.Passed
		}
	}
	return nil
}

func newRunner(ctx context.Context, cfg config) (*runner, error) {
	maxConc := 0
	for _, c := range cfg.concurrency {
		maxConc = max(maxConc, c)
	}
	transport := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        2*maxConc + 32,
		MaxIdleConnsPerHost: 2*maxConc + 32,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

	r := &runner{
		cfg:     cfg,
		mix:     mix{sqsShare: cfg.sqsShare, replayShare: cfg.replayShare, crossShare: cfg.crossShare},
		started: time.Now(),
		scraper: &scraper{urls: cfg.metrics, http: &http.Client{Timeout: 5 * time.Second}},
	}
	r.runID = strings.ReplaceAll(r.ids.New().String(), "-", "")[20:]

	tokens := map[string]*tokenSource{}
	for _, p := range providers {
		tokens[p] = newTokenSource(cfg.keycloak, p, client)
	}
	r.api = &apiClient{base: strings.TrimSuffix(cfg.target, "/"), http: client, tokens: tokens,
		internal: newTokenSource(cfg.keycloak, internalClient, client)}

	clients := map[string]*sqs.Client{}
	for _, p := range providers {
		c, err := sqsClient(ctx, p)
		if err != nil {
			return nil, err
		}
		clients[p] = c
	}
	inputURL, err := queueURL(ctx, clients[providers[0]], cfg.inputQueue)
	if err != nil {
		return nil, err
	}
	r.producer = &producer{queueURL: inputURL, clients: clients, tokens: tokens}

	reader, err := rootSQSClient(ctx)
	if err != nil {
		return nil, err
	}
	eventsURL, err := queueURL(ctx, reader, cfg.eventsQueue)
	if err != nil {
		return nil, err
	}
	r.events = newCollector(reader, eventsURL)
	r.sampler = &sampler{scraper: r.scraper, every: cfg.sampleEvery, started: r.started, producer: r.producer}
	return r, nil
}

func (r *runner) waitReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	for {
		err := r.api.ready(ctx)
		if err == nil {
			_, err = r.scraper.scrapeAll(ctx)
		}
		if err == nil {
			for _, p := range providers {
				if _, err = r.api.tokens[p].Token(ctx); err != nil {
					break
				}
			}
		}
		if err == nil {
			_, err = r.api.internal.Token(ctx)
		}
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%w after %s: %w", errNotReady, readyTimeout, err)
		}
		sleep(ctx, time.Second)
	}
}

func currentEnvironment() environment {
	return environment{
		GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		CPUs: runtime.NumCPU(), MemTotalKB: memTotalKB(),
	}
}

func memTotalKB() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kb, err := strconv.ParseInt(fields[1], 10, 64)
			if err == nil {
				return kb
			}
		}
	}
	return 0
}
