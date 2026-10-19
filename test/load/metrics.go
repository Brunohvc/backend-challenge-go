//go:build load

package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var scrapedFamilies = []string{
	"wagering_transactions_total",
	"wagering_duplicates_total",
	"wagering_processing_seconds_sum",
	"wagering_processing_seconds_count",
	"db_tx_retries_total",
	"sqs_messages_total",
	"sqs_dlq_total",
	"outbox_publish_total",
	"outbox_pending_events",
	"outbox_oldest_pending_age_seconds",
	"reference_resolutions_total",
	"reconciliation_divergences_total",
	"http_requests_total",
	"http_request_seconds_sum",
	"http_request_seconds_count",
}

func keepFamily(name string) bool {
	for _, f := range scrapedFamilies {
		if name == f {
			return true
		}
	}
	return false
}

type scraper struct {
	urls []string
	http *http.Client
}

func (s *scraper) scrapeOne(ctx context.Context, url string) (series, error) {
	resp, err := send(ctx, s.http, http.MethodGet, url, nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape %s: status %d", url, resp.StatusCode)
	}
	out, err := parseExposition(resp.Body, keepFamily)
	if err != nil {
		return nil, fmt.Errorf("scrape %s: %w", url, err)
	}
	return out, nil
}

func (s *scraper) scrapeAll(ctx context.Context) ([]series, error) {
	out := make([]series, len(s.urls))
	errs := make([]error, len(s.urls))
	var wg sync.WaitGroup
	for i, url := range s.urls {
		wg.Go(func() { out[i], errs[i] = s.scrapeOne(ctx, url) })
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func deltaAll(before, after []series) series {
	parts := make([]series, 0, len(after))
	for i := range after {
		parts = append(parts, counterDelta(before[i], after[i]))
	}
	return merge(parts...)
}

func consumedMessages(snaps []series) int64 {
	var n int64
	for _, s := range snaps {
		for _, v := range s {
			if v.name == "sqs_messages_total" && v.labels["result"] != "retry" {
				n += v.micros / microsPerUnit
			}
		}
	}
	return n
}

type gaugeSample struct {
	atNs     int64
	pending  int64
	oldestMs int64
	consumed int64
	sqsSent  int64
}

type sampler struct {
	scraper  *scraper
	every    time.Duration
	started  time.Time
	producer *producer

	consumedBase atomic.Int64
	consumed     atomic.Int64
	errors       atomic.Int64

	mu      sync.Mutex
	samples []gaugeSample
}

func (s *sampler) inflight() int64 {
	return s.producer.sent.Load() - (s.consumed.Load() - s.consumedBase.Load())
}

func (s *sampler) prime(ctx context.Context) error {
	snaps, err := s.scraper.scrapeAll(ctx)
	if err != nil {
		return err
	}
	n := consumedMessages(snaps)
	s.consumedBase.Store(n)
	s.consumed.Store(n)
	return nil
}

func (s *sampler) run(ctx context.Context) {
	t := time.NewTicker(s.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		at := time.Since(s.started).Nanoseconds()
		snaps, err := s.scraper.scrapeAll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.errors.Add(1)
			continue
		}
		g := gaugeSample{atNs: at, sqsSent: s.producer.sent.Load()}
		for _, snap := range snaps {
			g.pending = max(g.pending, snap.sum("outbox_pending_events", nil)/microsPerUnit)
			g.oldestMs = max(g.oldestMs, snap.sum("outbox_oldest_pending_age_seconds", nil)/1000)
		}
		g.consumed = consumedMessages(snaps)
		s.consumed.Store(g.consumed)
		s.mu.Lock()
		s.samples = append(s.samples, g)
		s.mu.Unlock()
	}
}

func (s *sampler) between(fromNs, toNs int64) []gaugeSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []gaugeSample
	for _, g := range s.samples {
		if g.atNs >= fromNs && g.atNs < toNs {
			out = append(out, g)
		}
	}
	return out
}
