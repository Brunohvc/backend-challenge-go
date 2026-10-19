//go:build load

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	channelHTTP = "http"
	channelSQS  = "sqs"

	statusProcessed = "PROCESSED"
	statusRejected  = "REJECTED"
	statusPending   = "PENDING_REFERENCE"
	statusAbsent    = "ABSENT"
)

var providers = []string{"provider-a", "provider-b"}

type walletState struct {
	id      string
	player  string
	initial int64
}

type opRecord struct {
	provider string
	ext      string
	key      string
	wallet   *walletState
	round    string
	kind     string
	ref      string
	refKind  string
	amount   int64
	channel  string
	startNs  int64
	inWindow bool

	httpStatus int
	failed     string
	tookUs     int64

	sentWallUs int64
	raced      bool

	status  string
	failure string
	txID    string
	via     string
}

func (o *opRecord) body() txBody {
	return txBody{
		ProviderID: o.provider, ExternalTransactionID: o.ext, PlayerID: o.wallet.player, WalletID: o.wallet.id,
		RoundID: o.round, GameID: gameID, Kind: o.kind, Money: brl(o.amount), ReferenceExternalTransactionID: o.ref,
	}
}

type resendStats struct {
	firstSendReplays int64
	httpReplays      int64
	httpReplayBad    int64
	sqsReplays       int64
	crossToSQS       int64
	races            int64
	racesWonHTTP     int64
	racesWonSQS      int64
	racesFailed      int64
	ambiguous        int64
	replayUs         []int64
	raceUs           []int64
}

func (s *resendStats) landed() int64 {
	return s.firstSendReplays + s.httpReplays + s.sqsReplays + s.crossToSQS + s.racesWonHTTP + s.racesWonSQS
}

func (s *resendStats) add(o resendStats) {
	s.firstSendReplays += o.firstSendReplays
	s.httpReplays += o.httpReplays
	s.httpReplayBad += o.httpReplayBad
	s.sqsReplays += o.sqsReplays
	s.crossToSQS += o.crossToSQS
	s.races += o.races
	s.racesWonHTTP += o.racesWonHTTP
	s.racesWonSQS += o.racesWonSQS
	s.racesFailed += o.racesFailed
	s.ambiguous += o.ambiguous
	s.replayUs = append(s.replayUs, o.replayUs...)
	s.raceUs = append(s.raceUs, o.raceUs...)
}

type phase struct {
	r       *runner
	name    string
	tag     string
	conc    int
	wallets []*walletState

	stop           atomic.Bool
	seq            atomic.Int64
	races          sync.WaitGroup
	raceMu         sync.Mutex
	raceStats      resendStats
	raceErrs       map[string]int64
	measureStartNs int64
	measureEndNs   int64
	stoppedNs      int64
	drainedNs      int64
	drained        bool
	sqsJobs        chan sqsJob

	workers []*worker
}

type sqsJob struct {
	plan   roundPlan
	ops    []*opRecord
	resend *opRecord
}

func (p *phase) resends() resendStats {
	var total resendStats
	for _, w := range p.workers {
		total.add(w.resend)
	}
	p.raceMu.Lock()
	total.add(p.raceStats)
	p.raceMu.Unlock()
	return total
}

func (p *phase) inWindow(ns int64) bool {
	return ns >= p.measureStartNs && ns < p.measureEndNs
}

func (p *phase) ops() []*opRecord {
	var all []*opRecord
	for _, w := range p.workers {
		all = append(all, w.ops...)
	}
	return all
}

type worker struct {
	p        *phase
	rng      *rand.Rand
	ops      []*opRecord
	resend   resendStats
	diverted int64
	rounds   int64
	errs     map[string]int64
}

func (w *worker) run(ctx context.Context) {
	for !w.p.stop.Load() && ctx.Err() == nil {
		plan := planRound(w.rng, w.p.r.mix)
		wallet := w.p.wallets[w.rng.IntN(len(w.p.wallets))]
		provider := providers[w.rng.IntN(len(providers))]
		rid := roundID(w.p.r.runID, w.p.tag, w.p.seq.Add(1))
		ops := make([]*opRecord, len(plan.ops))
		for i, spec := range plan.ops {
			ops[i] = &opRecord{
				provider: provider, ext: externalID(rid, i), wallet: wallet, round: rid,
				kind: spec.kind, refKind: refKind(plan.ops, i), amount: spec.amount,
			}
			ops[i].key = idempotencyKey(provider, ops[i].ext)
			if spec.ref >= 0 {
				ops[i].ref = ops[spec.ref].ext
			}
		}
		inWindow := w.p.inWindow(w.p.r.since())
		if inWindow {
			w.rounds++
		}
		if plan.sqs && w.p.r.sampler.inflight() < int64(w.p.r.cfg.sqsBacklog) && w.handOff(sqsJob{plan: plan, ops: ops}) {
			continue
		}
		if plan.sqs && inWindow {
			w.diverted++
		}
		w.httpRound(ctx, plan, ops)
	}
}

func (w *worker) handOff(job sqsJob) bool {
	select {
	case w.p.sqsJobs <- job:
		return true
	default:
		return false
	}
}

func (w *worker) send(ctx context.Context) {
	for job := range w.p.sqsJobs {
		if job.resend != nil {
			if w.resendSQS(ctx, job.resend) {
				w.resend.crossToSQS++
			}
			continue
		}
		w.sqsRound(ctx, job.plan, job.ops)
	}
}

func (w *worker) httpRound(ctx context.Context, plan roundPlan, ops []*opRecord) {
	for i, op := range ops {
		if w.p.stop.Load() || ctx.Err() != nil {
			return
		}
		op.channel = channelHTTP
		w.start(op)
		w.ops = append(w.ops, op)
		w.submitHTTP(ctx, op)
		if op.status != statusProcessed {
			return
		}
		if plan.resend == resendNone || plan.resendAt != i {
			continue
		}
		switch plan.resend {
		case resendReplay:
			w.replayHTTP(ctx, op)
		case resendCross:
			w.handOff(sqsJob{resend: op})
		}
	}
}

func (w *worker) sqsRound(ctx context.Context, plan roundPlan, ops []*opRecord) {
	for i, op := range ops {
		if ctx.Err() != nil {
			return
		}
		op.channel = channelSQS
		w.start(op)
		w.ops = append(w.ops, op)
		op.sentWallUs = time.Now().UnixMicro()
		took, err := w.p.r.producer.send(ctx, op.body(), op.key, w.p.r.ids.New().String())
		op.tookUs = took.Microseconds()
		if err != nil {
			op.failed = "sqs send"
			w.errs["sqs send"]++
			return
		}
		switch {
		case plan.resend == resendCross && i == 0:
			op.raced = true
			w.p.races.Go(func() { w.p.race(ctx, op, plan.raceDelay) })
		case plan.resend == resendReplay && plan.resendAt == i:
			if w.resendSQS(ctx, op) {
				w.resend.sqsReplays++
			}
		}
	}
}

func (w *worker) start(op *opRecord) {
	op.startNs = w.p.r.since()
	op.inWindow = w.p.inWindow(op.startNs)
}

func (w *worker) submitHTTP(ctx context.Context, op *opRecord) {
	rep, err := w.p.r.api.submit(ctx, op.body(), op.key, op.ext)
	if err != nil {
		op.failed = transportError(err)
		w.errs["http "+op.failed]++
		return
	}
	op.httpStatus, op.tookUs = rep.status, rep.took.Microseconds()
	switch rep.status {
	case http.StatusCreated, http.StatusAccepted, http.StatusUnprocessableEntity:
		op.status, op.failure, op.txID, op.via = rep.txStatus, rep.failureCode, rep.transactionID, channelHTTP
		if rep.status == http.StatusUnprocessableEntity {
			w.errs["http 422 "+rep.failureCode]++
		}
	case http.StatusOK:
		op.status, op.failure, op.txID, op.via = rep.txStatus, rep.failureCode, rep.transactionID, channelHTTP
		w.resend.firstSendReplays++
	default:
		bucket := fmt.Sprintf("http %d %s", rep.status, rep.code)
		if rep.retryAfter {
			bucket += " (Retry-After)"
		}
		op.failed = bucket
		w.errs[bucket]++
	}
}

func (w *worker) replayHTTP(ctx context.Context, op *opRecord) {
	rep, err := w.p.r.api.submit(ctx, op.body(), op.key, op.ext+"-replay")
	if err != nil {
		w.resend.ambiguous++
		w.errs["http replay "+transportError(err)]++
		return
	}
	if op.inWindow {
		w.resend.replayUs = append(w.resend.replayUs, rep.took.Microseconds())
	}
	if !rep.replay {
		w.errs[fmt.Sprintf("http replay %d %s", rep.status, rep.code)]++
		if rep.status == http.StatusOK || rep.status == http.StatusCreated {
			w.resend.httpReplayBad++
		}
		return
	}
	w.resend.httpReplays++
	if rep.status != http.StatusOK || rep.transactionID != op.txID || rep.txStatus != op.status {
		w.resend.httpReplayBad++
	}
}

func (w *worker) resendSQS(ctx context.Context, op *opRecord) bool {
	if _, err := w.p.r.producer.send(ctx, op.body(), op.key, w.p.r.ids.New().String()); err != nil {
		w.errs["sqs resend send"]++
		return false
	}
	return true
}

func (p *phase) race(ctx context.Context, op *opRecord, delay time.Duration) {
	sleep(ctx, delay)
	rep, err := p.r.api.submit(ctx, op.body(), op.key, op.ext+"-race")

	p.raceMu.Lock()
	defer p.raceMu.Unlock()
	s := &p.raceStats
	s.races++
	if err != nil {
		s.ambiguous++
		p.raceErrs["http race "+transportError(err)]++
		return
	}
	if op.inWindow {
		s.raceUs = append(s.raceUs, rep.took.Microseconds())
	}
	switch {
	case rep.status == http.StatusCreated:
		s.racesWonHTTP++
		op.status, op.txID, op.via = rep.txStatus, rep.transactionID, channelHTTP
	case rep.status == http.StatusOK && rep.replay:
		s.racesWonSQS++
	default:
		s.racesFailed++
		p.raceErrs[fmt.Sprintf("http race %d %s", rep.status, rep.code)]++
	}
}

func transportError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "transport error"
	}
}

func (r *runner) openWallets(ctx context.Context, n int) ([]*walletState, error) {
	wallets := make([]*walletState, n)
	errs := make([]error, n)
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for i := range wallets {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			player := r.ids.New().String()
			id, err := r.api.openWallet(ctx, player, r.cfg.initialBalance)
			wallets[i], errs[i] = &walletState{id: id, player: player, initial: r.cfg.initialBalance}, err
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return wallets, nil
}

func (r *runner) runPhase(ctx context.Context, name string, conc int) (phaseReport, error) {
	n := r.cfg.wallets
	if name == phaseContention {
		n = r.cfg.hotWallets
	}
	p := &phase{r: r, name: name, conc: conc, tag: fmt.Sprintf("%c%d", name[0], conc), raceErrs: map[string]int64{}}
	r.progress("%s, concurrency %d: opening %d wallets", name, conc, n)
	wallets, err := r.openWallets(ctx, n)
	if err != nil {
		return phaseReport{}, fmt.Errorf("open wallets: %w", err)
	}
	p.wallets = wallets

	before, err := r.scraper.scrapeAll(ctx)
	if err != nil {
		return phaseReport{}, err
	}
	startNs := r.since()
	p.measureStartNs = startNs + r.cfg.warmup.Nanoseconds()
	p.measureEndNs = p.measureStartNs + r.cfg.duration.Nanoseconds()

	p.sqsJobs = make(chan sqsJob, r.cfg.sqsSenders)
	var wg, senders sync.WaitGroup
	for range r.cfg.sqsSenders {
		w := &worker{p: p, errs: map[string]int64{}}
		p.workers = append(p.workers, w)
		senders.Go(func() { w.send(ctx) })
	}
	for i := range conc {
		w := &worker{p: p, rng: mixRand(r.cfg.seed, conc, i), errs: map[string]int64{}}
		p.workers = append(p.workers, w)
		wg.Go(func() { w.run(ctx) })
	}
	stopWorkers := func() {
		p.stop.Store(true)
		wg.Wait()
		close(p.sqsJobs)
		senders.Wait()
		p.races.Wait()
	}
	r.progress("%s, concurrency %d: warming up for %s", name, conc, r.cfg.warmup)

	r.sleepUntil(ctx, p.measureStartNs)
	windowStart, err := r.scraper.scrapeAll(ctx)
	if err != nil {
		stopWorkers()
		return phaseReport{}, err
	}
	r.progress("%s, concurrency %d: measuring for %s", name, conc, r.cfg.duration)
	r.sleepUntil(ctx, p.measureEndNs)
	windowEnd, err := r.scraper.scrapeAll(ctx)
	stopWorkers()
	p.stoppedNs = r.since()
	if err != nil {
		return phaseReport{}, err
	}
	if ctx.Err() != nil {
		return phaseReport{}, ctx.Err()
	}

	r.progress("%s, concurrency %d: draining the queues and the outbox", name, conc)
	p.drain(ctx)
	after, err := r.scraper.scrapeAll(ctx)
	if err != nil {
		return phaseReport{}, err
	}

	r.progress("%s, concurrency %d: checking integrity", name, conc)
	check, err := p.verify(ctx, deltaAll(before, after))
	if err != nil {
		return phaseReport{}, err
	}
	rep := p.report(deltaAll(windowStart, windowEnd), deltaAll(windowEnd, after), check)
	r.progress("%s, concurrency %d: done, integrity %s", name, conc, verdict(check.Passed))
	return rep, nil
}

type drainState struct {
	awaiting int
	openings int
	inflight int64
}

func (d drainState) done() bool {
	return d.awaiting == 0 && d.openings == 0 && d.inflight <= 0
}

func (p *phase) drainState() drainState {
	var d drainState
	ops := p.ops()
	p.r.events.view(func(c *collector) {
		for _, op := range ops {
			if !expectsOutcome(op) {
				continue
			}
			o := c.outcomes[op.key]
			switch {
			case o == nil:
				d.awaiting++
			case o.status == statusProcessed && movesMoney(op.kind) && c.balances[o.transactionID] == 0:
				d.awaiting++
			}
		}
		for _, w := range p.wallets {
			if we := c.wallets[w.id]; we == nil || we.openings == 0 {
				d.openings++
			}
		}
	})
	d.inflight = p.r.sampler.inflight()
	return d
}

func expectsOutcome(op *opRecord) bool {
	if op.channel == channelSQS {
		return op.failed == ""
	}
	return op.status == statusProcessed || op.status == statusRejected
}

func (p *phase) drain(ctx context.Context) {
	deadline := time.Now().Add(p.r.cfg.drainTimeout)
	for ctx.Err() == nil {
		if p.drainState().done() {
			p.drained = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		sleep(ctx, 250*time.Millisecond)
	}
	p.drainedNs = p.r.since()
}

func verdict(ok bool) string {
	if ok {
		return "OK"
	}
	return "FAILED"
}
