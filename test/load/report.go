//go:build load

package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

type rate struct {
	Count     int64  `json:"count"`
	PerSecond string `json:"perSecond"`
}

func newRate(count int64, window time.Duration) rate {
	return rate{Count: count, PerSecond: formatTenths(perSecondTenths(count, window))}
}

type outboxReport struct {
	Samples           int   `json:"samples"`
	PendingMax        int64 `json:"pendingMax"`
	PendingP95        int64 `json:"pendingP95"`
	OldestAgeMaxMs    int64 `json:"oldestAgeMaxMs"`
	OldestAgeP95Ms    int64 `json:"oldestAgeP95Ms"`
	PendingAtStop     int64 `json:"pendingAtStop"`
	DrainMs           int64 `json:"drainMs"`
	DrainPublished    int64 `json:"drainPublished"`
	InputBacklogMax   int64 `json:"inputBacklogMax"`
	ScrapeErrorsTotal int64 `json:"scrapeErrorsTotal"`
}

type resendReport struct {
	FirstSends    int64 `json:"firstSendsAnsweredAsReplay"`
	HTTPReplays   int64 `json:"httpReplays"`
	SQSReplays    int64 `json:"sqsReplays"`
	HTTPToSQS     int64 `json:"httpToSqs"`
	Races         int64 `json:"sqsHttpRaces"`
	RacesWonHTTP  int64 `json:"racesWonByHttp"`
	RacesWonSQS   int64 `json:"racesWonBySqs"`
	RacesFailed   int64 `json:"racesFailed"`
	Ambiguous     int64 `json:"ambiguous"`
	HTTPReplayBad int64 `json:"httpReplaysWithAnotherOutcome"`
}

type phaseReport struct {
	Phase        string             `json:"phase"`
	Concurrency  int                `json:"concurrency"`
	Wallets      int                `json:"wallets"`
	WarmupMs     int64              `json:"warmupMs"`
	WindowMs     int64              `json:"windowMs"`
	Rounds       int64              `json:"roundsStartedInWindow"`
	Throughput   map[string]rate    `json:"throughput"`
	Latency      map[string]latency `json:"latency"`
	Errors       map[string]int64   `json:"errors"`
	Window       map[string]int64   `json:"windowCounters"`
	ServerMeanUs map[string]int64   `json:"serverMeanUs"`
	Outbox       outboxReport       `json:"outbox"`
	Resends      resendReport       `json:"resends"`
	SQSDiverted  int64              `json:"sqsRoundsDivertedToHttp"`
	Integrity    integrity          `json:"integrity"`
}

func (p *phase) report(window, drain series, check integrity) phaseReport {
	cfg := p.r.cfg
	rep := phaseReport{
		Phase: p.name, Concurrency: p.conc, Wallets: len(p.wallets),
		WarmupMs: cfg.warmup.Milliseconds(), WindowMs: cfg.duration.Milliseconds(),
		Throughput: map[string]rate{}, Latency: map[string]latency{},
		Errors: map[string]int64{}, Window: map[string]int64{}, ServerMeanUs: map[string]int64{},
		Integrity: check,
	}
	resends := p.resends()
	for _, w := range p.workers {
		rep.SQSDiverted += w.diverted
		rep.Rounds += w.rounds
		for k, v := range w.errs {
			rep.Errors[k] += v
		}
	}
	for k, v := range p.raceErrs {
		rep.Errors[k] += v
	}
	rep.Resends = resendReport{
		FirstSends: resends.firstSendReplays, HTTPReplays: resends.httpReplays, SQSReplays: resends.sqsReplays, HTTPToSQS: resends.crossToSQS,
		Races: resends.races, RacesWonHTTP: resends.racesWonHTTP, RacesWonSQS: resends.racesWonSQS,
		RacesFailed: resends.racesFailed, Ambiguous: resends.ambiguous, HTTPReplayBad: resends.httpReplayBad,
	}

	p.latencies(&rep, resends)
	p.counters(&rep, window)
	p.outbox(&rep)
	rep.Outbox.DrainPublished = drain.count("outbox_publish_total", map[string]string{"result": "published"})
	return rep
}

func (p *phase) latencies(rep *phaseReport, resends resendStats) {
	cfg := p.r.cfg
	windowStartMs := p.r.started.Add(time.Duration(p.measureStartNs)).UnixMilli()
	windowEndMs := p.r.started.Add(time.Duration(p.measureEndNs)).UnixMilli()
	samples := map[string][]int64{}
	var httpOps, sqsSent, sqsDecided int64

	ops := p.ops()
	p.r.events.view(func(c *collector) {
		for _, op := range ops {
			if op.channel == channelSQS && op.via == channelSQS && !op.raced {
				if o := c.outcomes[op.key]; o != nil && o.occurredMs >= windowStartMs && o.occurredMs < windowEndMs {
					sqsDecided++
				}
			}
			if !op.inWindow {
				continue
			}
			switch {
			case op.channel == channelHTTP && op.failed == "":
				httpOps++
				samples["http."+op.kind] = append(samples["http."+op.kind], op.tookUs)
				samples["http.all"] = append(samples["http.all"], op.tookUs)
			case op.channel == channelSQS && op.failed == "":
				sqsSent++
				samples["sqs.send"] = append(samples["sqs.send"], op.tookUs)
				o := c.outcomes[op.key]
				if o == nil || op.raced || op.via != channelSQS {
					continue
				}
				decide := max(o.occurredMs*1000-op.sentWallUs, 0)
				samples["sqs.decide."+op.kind] = append(samples["sqs.decide."+op.kind], decide)
				samples["sqs.decide.all"] = append(samples["sqs.decide.all"], decide)
				samples["sqs.event.all"] = append(samples["sqs.event.all"], o.receivedUs-op.sentWallUs)
			}
		}
		for _, w := range p.wallets {
			we := c.wallets[w.id]
			if we == nil {
				continue
			}
			for _, t := range we.timings {
				if t.occurredMs < windowStartMs || t.occurredMs >= windowEndMs {
					continue
				}
				samples["event.delay"] = append(samples["event.delay"], t.receivedUs-t.occurredMs*1000)
				if t.sentMs > 0 {
					samples["event.publish"] = append(samples["event.publish"], max(t.sentMs-t.occurredMs, 0)*1000)
					samples["event.deliver"] = append(samples["event.deliver"], max(t.receivedUs-t.sentMs*1000, 0))
				}
			}
		}
	})
	samples["http.replay"] = resends.replayUs
	samples["http.race"] = resends.raceUs
	for k, v := range samples {
		if len(v) > 0 {
			rep.Latency[k] = summarize(v)
		}
	}
	rep.Throughput["total"] = newRate(httpOps+sqsSent, cfg.duration)
	rep.Throughput["http"] = newRate(httpOps, cfg.duration)
	rep.Throughput["sqs.sent"] = newRate(sqsSent, cfg.duration)
	rep.Throughput["sqs.decided"] = newRate(sqsDecided, cfg.duration)
	for _, k := range kinds {
		if l, ok := rep.Latency["http."+k]; ok {
			rep.Throughput["http."+k] = newRate(int64(l.Count), cfg.duration)
		}
	}
}

func (p *phase) counters(rep *phaseReport, window series) {
	for reason, v := range window.byLabel("db_tx_retries_total", "reason") {
		rep.Window["db_tx_retries_total{reason="+reason+"}"] = v / microsPerUnit
	}
	for _, s := range window {
		switch s.name {
		case "wagering_duplicates_total":
			rep.Window["wagering_duplicates_total{channel="+s.labels["channel"]+",source="+s.labels["source"]+"}"] += s.micros / microsPerUnit
		case "sqs_messages_total":
			rep.Window["sqs_messages_total{result="+s.labels["result"]+"}"] += s.micros / microsPerUnit
		case "sqs_dlq_total":
			rep.Window["sqs_dlq_total{reason="+s.labels["reason"]+"}"] += s.micros / microsPerUnit
		case "outbox_publish_total":
			rep.Window["outbox_publish_total{result="+s.labels["result"]+"}"] += s.micros / microsPerUnit
		case "reference_resolutions_total":
			rep.Window["reference_resolutions_total{result="+s.labels["result"]+"}"] += s.micros / microsPerUnit
		case "http_requests_total":
			if code := s.labels["code"]; code == "409" || strings.HasPrefix(code, "5") {
				rep.Window["http_requests_total{code="+code+"}"] += s.micros / microsPerUnit
			}
		case "wagering_transactions_total":
			key := "wagering_transactions_total{channel=" + s.labels["channel"] + ",status=" + s.labels["status"] + "}"
			rep.Window[key] += s.micros / microsPerUnit
		}
	}
	for _, ch := range []string{channelHTTP, channelSQS} {
		match := map[string]string{"channel": ch}
		if n := window.count("wagering_processing_seconds_count", match); n > 0 {
			rep.ServerMeanUs["processing."+ch] = window.sum("wagering_processing_seconds_sum", match) / n
		}
	}
	match := map[string]string{"route": "POST /wagering/transactions"}
	if n := window.count("http_request_seconds_count", match); n > 0 {
		rep.ServerMeanUs["http.POST /wagering/transactions"] = window.sum("http_request_seconds_sum", match) / n
	}
}

func (p *phase) outbox(rep *phaseReport) {
	in := p.r.sampler.between(p.measureStartNs, p.measureEndNs)
	pending := make([]int64, 0, len(in))
	oldest := make([]int64, 0, len(in))
	for _, g := range in {
		pending = append(pending, g.pending)
		oldest = append(oldest, g.oldestMs)
		rep.Outbox.InputBacklogMax = max(rep.Outbox.InputBacklogMax, g.sqsSent-(g.consumed-p.r.sampler.consumedBase.Load()))
	}
	slices.Sort(pending)
	slices.Sort(oldest)
	rep.Outbox.Samples = len(in)
	if len(in) > 0 {
		rep.Outbox.PendingMax, rep.Outbox.PendingP95 = pending[len(pending)-1], percentile(pending, 950)
		rep.Outbox.OldestAgeMaxMs, rep.Outbox.OldestAgeP95Ms = oldest[len(oldest)-1], percentile(oldest, 950)
	}
	if tail := p.r.sampler.between(p.measureEndNs-p.r.cfg.sampleEvery.Nanoseconds(), p.stoppedNs+p.r.cfg.sampleEvery.Nanoseconds()); len(tail) > 0 {
		rep.Outbox.PendingAtStop = tail[len(tail)-1].pending
	}
	rep.Outbox.DrainMs = (p.drainedNs - p.measureEndNs) / int64(time.Millisecond)
	rep.Outbox.ScrapeErrorsTotal = p.r.sampler.errors.Load()
}

type environment struct {
	GoVersion  string `json:"goVersion"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	CPUs       int    `json:"cpus"`
	MemTotalKB int64  `json:"memTotalKB"`
}

type runReport struct {
	RunID       string        `json:"runId"`
	StartedAt   string        `json:"startedAt"`
	FinishedAt  string        `json:"finishedAt"`
	Environment environment   `json:"environment"`
	Config      configReport  `json:"config"`
	Phases      []phaseReport `json:"phases"`
	Passed      bool          `json:"passed"`
	Error       string        `json:"error,omitempty"`
	Events      eventsReport  `json:"events"`
}

type eventsReport struct {
	Received    int64 `json:"received"`
	Redelivered int64 `json:"redelivered"`
	Malformed   int64 `json:"malformed"`
	Errors      int64 `json:"errors"`
}

func writeJSON(dir string, rep runReport) (string, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return "", fmt.Errorf("open the report directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	name := "load-" + rep.RunID + ".json"
	path := filepath.Join(dir, name)
	f, err := root.Create(name)
	if err != nil {
		return "", fmt.Errorf("create %s: %w", path, err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close %s: %w", path, err)
	}
	if err := ownLikeDir(root, dir, name); err != nil {
		fmt.Fprintf(os.Stderr, "load: %s stays owned by root: %v\n", path, err)
	}
	return path, nil
}

func ownLikeDir(root *os.Root, dir, name string) error {
	if os.Geteuid() != 0 {
		return nil
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st.Uid == 0 {
		return nil
	}
	return root.Chown(name, int(st.Uid), int(st.Gid))
}

var latencyRows = []struct{ key, label string }{
	{"http.BET", "HTTP BET"},
	{"http.WIN", "HTTP WIN"},
	{"http.LOSS", "HTTP LOSS"},
	{"http.REFUND", "HTTP REFUND"},
	{"http.ROLLBACK", "HTTP ROLLBACK"},
	{"http.all", "HTTP, all kinds"},
	{"http.replay", "HTTP replay (same key)"},
	{"http.race", "HTTP resend racing SQS"},
	{"sqs.send", "SQS SendMessage"},
	{"sqs.decide.all", "SQS send -> decided"},
	{"sqs.event.all", "SQS send -> outcome event"},
	{"event.publish", "event: processedAt -> sent to SQS"},
	{"event.deliver", "event: sent to SQS -> received"},
	{"event.delay", "event: processedAt -> received"},
}

func summary(rep runReport) string {
	var b strings.Builder
	printSummary(&b, rep)
	return b.String()
}

func printSummary(w *strings.Builder, rep runReport) {
	fmt.Fprintf(w, "load test %s, %s to %s\n", rep.RunID, rep.StartedAt, rep.FinishedAt)
	fmt.Fprintf(w, "generator: %s %s/%s, %d CPUs visible, %d MB\n",
		rep.Environment.GoVersion, rep.Environment.OS, rep.Environment.Arch, rep.Environment.CPUs, rep.Environment.MemTotalKB/1024)
	c := rep.Config
	fmt.Fprintf(w, "mix: %d%% of rounds over SQS, %d%% with a replay, %d%% with a cross-channel resend; SQS backlog cap %d, %d SQS senders; seed %d\n",
		c.SQSSharePercent, c.ReplaySharePercent, c.CrossSharePercent, c.SQSBacklog, c.SQSSenders, c.Seed)
	for _, p := range rep.Phases {
		printPhase(w, p)
	}
	fmt.Fprintf(w, "\nevents received %d (redelivered %d, malformed %d, receive errors %d)\n",
		rep.Events.Received, rep.Events.Redelivered, rep.Events.Malformed, rep.Events.Errors)
	if rep.Error != "" {
		fmt.Fprintf(w, "stopped early: %s\n", rep.Error)
	}
	fmt.Fprintf(w, "result: %s\n", verdict(rep.Passed))
}

func printPhase(w *strings.Builder, p phaseReport) {
	fmt.Fprintf(w, "\n== %s, concurrency %d, %d wallets, warm-up %s, window %s, %d rounds started in the window\n",
		p.Phase, p.Concurrency, p.Wallets, time.Duration(p.WarmupMs)*time.Millisecond,
		time.Duration(p.WindowMs)*time.Millisecond, p.Rounds)
	t := p.Throughput
	fmt.Fprintf(w, "throughput: %s ops/s (HTTP %s, SQS sent %s, SQS decided %s)\n",
		t["total"].PerSecond, t["http"].PerSecond, t["sqs.sent"].PerSecond, t["sqs.decided"].PerSecond)
	var kinds []string
	for _, k := range []string{kindBet, kindWin, kindLoss, kindRefund, kindRollback} {
		if r, ok := t["http."+k]; ok {
			kinds = append(kinds, k+" "+r.PerSecond)
		}
	}
	fmt.Fprintf(w, "HTTP by kind (ops/s): %s\n", strings.Join(kinds, ", "))
	fmt.Fprintf(w, "%-34s %8s %9s %9s %9s %9s\n", "latency (ms)", "n", "p50", "p95", "p99", "max")
	for _, row := range latencyRows {
		l, ok := p.Latency[row.key]
		if !ok {
			continue
		}
		fmt.Fprintf(w, "%-34s %8d %9s %9s %9s %9s\n", row.label, l.Count,
			formatMillis(l.P50Us), formatMillis(l.P95Us), formatMillis(l.P99Us), formatMillis(l.MaxUs))
	}
	if len(p.ServerMeanUs) > 0 {
		var parts []string
		for _, k := range slices.Sorted(maps.Keys(p.ServerMeanUs)) {
			parts = append(parts, k+" "+formatMillis(p.ServerMeanUs[k])+" ms")
		}
		fmt.Fprintf(w, "server-side means: %s\n", strings.Join(parts, ", "))
	}
	fmt.Fprintf(w, "errors: %s\n", joinCounts(p.Errors))
	fmt.Fprintf(w, "window counters (3 replicas): %s\n", joinCounts(p.Window))
	r := p.Resends
	fmt.Fprintf(w, "resends: %d first sends answered as replays, %d HTTP replays, %d SQS replays, %d HTTP->SQS, %d SQS/HTTP races (HTTP won %d, SQS won %d, failed %d), %d ambiguous\n",
		r.FirstSends, r.HTTPReplays, r.SQSReplays, r.HTTPToSQS, r.Races, r.RacesWonHTTP, r.RacesWonSQS, r.RacesFailed, r.Ambiguous)
	o := p.Outbox
	fmt.Fprintf(w, "outbox: pending p95 %d max %d, oldest age p95 %s max %s ms (%d samples); input backlog max %d\n",
		o.PendingP95, o.PendingMax, formatMillis(o.OldestAgeP95Ms*1000), formatMillis(o.OldestAgeMaxMs*1000),
		o.Samples, o.InputBacklogMax)
	drain := time.Duration(o.DrainMs) * time.Millisecond
	fmt.Fprintf(w, "drain: %s after the window, %d events published meanwhile (%s/s)\n",
		drain, o.DrainPublished, formatTenths(perSecondTenths(o.DrainPublished, drain)))
	sent, all := t["sqs.sent"].Count, t["total"].Count
	if all > 0 {
		fmt.Fprintf(w, "SQS share: %s%% of the operations in the window; %d SQS rounds went over HTTP while the senders or the input backlog were full\n",
			formatTenths((sent*1000+all/2)/all), p.SQSDiverted)
	}
	in := p.Integrity
	fmt.Fprintf(w, "integrity %s: %d/%d wallets consistent, %d negative, %d balance mismatches; ledger %d entries = %d movements + %d openings; "+
		"decided %d (processed %d, rejected %d), counted by the replicas %d; duplicates by key %d expected, %d counted, by inbox %d; "+
		"%d duplicate decisions, %d missing events, %d redelivered; DLQ %d\n",
		verdict(in.Passed), in.Consistent, in.Wallets, in.NegativeBalances, in.BalanceMismatches,
		in.LedgerEntries, in.Movements, in.Openings, in.DecidedOps, in.Processed, in.Rejected, in.DecisionsCounted,
		in.DuplicatesExpected, in.DuplicatesCounted, in.InboxDuplicates, in.DuplicateDecisions, in.MissingOutcomeEvents+in.MissingBalanceEvents,
		in.RedeliveredEvents, in.DeadLettered)
	if len(in.Rejections) > 0 || in.LookedUp > 0 {
		fmt.Fprintf(w, "rejections: %s; %d operations without a known outcome looked up, %d of them recorded after no answer\n",
			joinCounts(in.Rejections), in.LookedUp, in.UnansweredRecorded)
	}
	for _, f := range in.Failures {
		fmt.Fprintf(w, "  - %s\n", f)
	}
}

func joinCounts(m map[string]int64) string {
	if len(m) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}
