//go:build load

package main

import (
	"context"
	"fmt"
	"sync"
)

const maxListedFailures = 20

type integrity struct {
	Passed                 bool             `json:"passed"`
	Wallets                int              `json:"wallets"`
	Consistent             int              `json:"consistent"`
	NegativeBalances       int              `json:"negativeBalances"`
	BalanceMismatches      int              `json:"balanceMismatches"`
	LedgerEntries          int64            `json:"ledgerEntries"`
	Openings               int64            `json:"openings"`
	Movements              int64            `json:"movements"`
	LedgerMismatches       int              `json:"ledgerMismatches"`
	DecidedOps             int64            `json:"decidedOps"`
	Processed              int64            `json:"processed"`
	Rejected               int64            `json:"rejected"`
	Pending                int64            `json:"pending"`
	Absent                 int64            `json:"absent"`
	Rejections             map[string]int64 `json:"rejections"`
	LookedUp               int              `json:"lookedUp"`
	UnansweredRecorded     int              `json:"unansweredRecorded"`
	RefusedButRecorded     int              `json:"refusedButRecorded"`
	MissingOutcomeEvents   int              `json:"missingOutcomeEvents"`
	DuplicateDecisions     int              `json:"duplicateDecisions"`
	MissingBalanceEvents   int              `json:"missingBalanceEvents"`
	DuplicateBalanceEvents int              `json:"duplicateBalanceEvents"`
	TransactionMismatches  int              `json:"transactionMismatches"`
	RedeliveredEvents      int64            `json:"redeliveredEvents"`
	DecisionsCounted       int64            `json:"decisionsCounted"`
	DuplicatesExpected     int64            `json:"duplicatesExpected"`
	DuplicatesAmbiguous    int64            `json:"duplicatesAmbiguous"`
	DuplicatesCounted      int64            `json:"duplicatesCounted"`
	InboxDuplicates        int64            `json:"inboxDuplicates"`
	DeadLettered           int64            `json:"deadLettered"`
	Divergences            int64            `json:"divergences"`
	Drained                bool             `json:"drained"`
	Failures               []string         `json:"failures"`
}

func (in *integrity) fail(format string, args ...any) {
	in.Passed = false
	if len(in.Failures) < maxListedFailures {
		in.Failures = append(in.Failures, fmt.Sprintf(format, args...))
	}
}

type walletExpectation struct {
	balance   int64
	movements int64
	txIDs     map[string]bool
}

func (p *phase) verify(ctx context.Context, whole series) (integrity, error) {
	in := integrity{Passed: true, Wallets: len(p.wallets), Drained: p.drained, Rejections: map[string]int64{}}
	if !p.drained {
		in.fail("the queues and the outbox did not drain within %s: %+v", p.r.cfg.drainTimeout, p.drainState())
	}
	ops := p.ops()
	p.settleFromEvents(ops)
	if err := p.lookUp(ctx, ops, &in); err != nil {
		return in, err
	}

	expect := map[string]*walletExpectation{}
	for _, w := range p.wallets {
		expect[w.id] = &walletExpectation{balance: w.initial, txIDs: map[string]bool{}}
	}
	for _, op := range ops {
		switch op.status {
		case statusProcessed:
			in.Processed++
			e := expect[op.wallet.id]
			e.balance += effect(op.kind, op.refKind, op.amount)
			if movesMoney(op.kind) {
				e.movements++
				e.txIDs[op.txID] = true
			}
		case statusRejected:
			in.Rejected++
			in.Rejections[op.failure]++
		case statusPending:
			in.Pending++
		case statusAbsent:
			in.Absent++
		}
	}
	in.DecidedOps = in.Processed + in.Rejected
	if in.Rejected > 0 {
		in.fail("%d operations were rejected, but the workload only sends valid sequences: %v", in.Rejected, in.Rejections)
	}

	p.checkEvents(ops, &in)
	if err := p.checkWallets(ctx, expect, &in); err != nil {
		return in, err
	}
	p.checkCounters(whole, &in)
	return in, nil
}

func (p *phase) settleFromEvents(ops []*opRecord) {
	p.r.events.view(func(c *collector) {
		for _, op := range ops {
			if op.channel != channelSQS || op.failed != "" || op.via == channelHTTP {
				continue
			}
			if o := c.outcomes[op.key]; o != nil {
				op.status, op.failure, op.txID, op.via = o.status, o.failureCode, o.transactionID, channelSQS
			}
		}
	})
}

func (p *phase) lookUp(ctx context.Context, ops []*opRecord, in *integrity) error {
	var todo []*opRecord
	for _, op := range ops {
		if op.status == "" || op.status == statusPending {
			todo = append(todo, op)
		}
	}
	in.LookedUp = len(todo)
	errs := make([]error, len(todo))
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for i, op := range todo {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			v, found, err := p.r.api.transactionByExternalID(ctx, op.provider, op.ext)
			switch {
			case err != nil:
				errs[i] = err
			case !found:
				op.status = statusAbsent
			default:
				op.status, op.failure, op.txID, op.via = v.Status, v.FailureCode, v.TransactionID, "lookup"
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return fmt.Errorf("look up transactions: %w", err)
		}
	}
	for _, op := range todo {
		switch {
		case op.channel != channelHTTP || op.status == statusAbsent:
		case op.httpStatus == 0:
			in.UnansweredRecorded++
		default:
			in.RefusedButRecorded++
			in.fail("%s was refused with %d but is recorded as %s", op.key, op.httpStatus, op.status)
		}
		if op.channel == channelSQS && op.status == statusAbsent && op.failed == "" {
			in.fail("SQS message for %s was never decided", op.key)
		}
		if op.status == statusPending {
			in.fail("%s is still %s", op.key, statusPending)
		}
	}
	return nil
}

func (p *phase) checkEvents(ops []*opRecord, in *integrity) {
	p.r.events.view(func(c *collector) {
		in.RedeliveredEvents = c.redelivered
		for _, op := range ops {
			if op.status != statusProcessed && op.status != statusRejected {
				continue
			}
			o := c.outcomes[op.key]
			switch {
			case o == nil:
				in.MissingOutcomeEvents++
				in.fail("no outcome event for %s", op.key)
				continue
			case o.events > 1:
				in.DuplicateDecisions++
				in.fail("%d outcome events with distinct ids for %s", o.events, op.key)
			}
			if o.transactionID != op.txID || o.status != op.status {
				in.TransactionMismatches++
				in.fail("%s: answered %s %s, event says %s %s", op.key, op.status, op.txID, o.status, o.transactionID)
			}
			if op.status != statusProcessed || !movesMoney(op.kind) {
				continue
			}
			switch n := c.balances[op.txID]; {
			case n == 0:
				in.MissingBalanceEvents++
				in.fail("no %s for transaction %s", eventBalanceChanged, op.txID)
			case n > 1:
				in.DuplicateBalanceEvents++
				in.fail("%d %s events with distinct ids for transaction %s", n, eventBalanceChanged, op.txID)
			}
		}
		for _, w := range p.wallets {
			openings := 0
			if we := c.wallets[w.id]; we != nil {
				openings = we.openings
			}
			if openings != 1 {
				in.fail("wallet %s: %d opening events, want 1", w.id, openings)
			}
		}
	})
}

type walletCheck struct {
	consistent bool
	negative   bool
	balanceOK  bool
	ledgerOK   bool
	entries    int64
	problems   []string
	err        error
}

func (p *phase) checkWallets(ctx context.Context, expect map[string]*walletExpectation, in *integrity) error {
	checks := make([]walletCheck, len(p.wallets))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i, w := range p.wallets {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			checks[i] = p.checkWallet(ctx, w, expect[w.id])
		})
	}
	wg.Wait()
	for i, c := range checks {
		if c.err != nil {
			return fmt.Errorf("check wallet %s: %w", p.wallets[i].id, c.err)
		}
		e := expect[p.wallets[i].id]
		in.Movements += e.movements
		in.Openings++
		in.LedgerEntries += c.entries
		if c.consistent {
			in.Consistent++
		}
		if c.negative {
			in.NegativeBalances++
		}
		if !c.balanceOK {
			in.BalanceMismatches++
		}
		if !c.ledgerOK {
			in.LedgerMismatches++
		}
		for _, problem := range c.problems {
			in.fail("wallet %s: %s", p.wallets[i].id, problem)
		}
	}
	return nil
}

func (p *phase) checkWallet(ctx context.Context, w *walletState, e *walletExpectation) walletCheck {
	c := walletCheck{balanceOK: true, ledgerOK: true}
	rec, err := p.r.api.reconcile(ctx, w.id)
	if err != nil {
		return walletCheck{err: err}
	}
	stored, err := parseMinor(rec.StoredBalance.Amount)
	if err != nil {
		return walletCheck{err: err}
	}
	c.consistent = rec.Consistent
	if !rec.Consistent {
		c.problems = append(c.problems, "reconciliation is not consistent")
	}
	if stored < 0 {
		c.negative = true
		c.problems = append(c.problems, "negative balance "+formatMinor(stored))
	}
	if stored != e.balance {
		c.balanceOK = false
		c.problems = append(c.problems, fmt.Sprintf("balance %s, the outcomes add up to %s", formatMinor(stored), formatMinor(e.balance)))
	}
	if rec.CheckedEntries != e.movements+1 {
		c.ledgerOK = false
		c.problems = append(c.problems, fmt.Sprintf("reconciliation checked %d entries, want %d", rec.CheckedEntries, e.movements+1))
	}

	items, err := p.r.api.ledger(ctx, w.id)
	if err != nil {
		return walletCheck{err: err}
	}
	c.entries = int64(len(items))
	c.ledgerOK = c.ledgerOK && ledgerMatches(items, w.initial, e, &c.problems)
	for _, it := range items {
		if after, err := parseMinor(it.BalanceAfter.Amount); err != nil || after < 0 {
			c.negative = true
			c.problems = append(c.problems, "ledger entry with a negative or unreadable balance "+it.BalanceAfter.Amount)
			break
		}
	}
	return c
}

func ledgerMatches(items []ledgerItem, initial int64, e *walletExpectation, problems *[]string) bool {
	ok := true
	report := func(format string, args ...any) {
		ok = false
		*problems = append(*problems, fmt.Sprintf(format, args...))
	}
	if int64(len(items)) != e.movements+1 {
		report("ledger has %d entries, want %d", len(items), e.movements+1)
	}
	if len(items) == 0 {
		return false
	}
	if first := items[0]; first.Direction != "CREDIT" || first.Amount.Amount != formatMinor(initial) || first.BalanceBefore.Amount != "0.00" {
		report("first ledger entry is not the opening credit of %s", formatMinor(initial))
	}
	seen := map[string]bool{}
	for _, it := range items[1:] {
		switch {
		case seen[it.TransactionID]:
			report("transaction %s has two ledger entries", it.TransactionID)
		case !e.txIDs[it.TransactionID]:
			report("ledger entry for transaction %s that no processed operation answered", it.TransactionID)
		}
		seen[it.TransactionID] = true
	}
	for id := range e.txIDs {
		if !seen[id] {
			report("processed transaction %s has no ledger entry", id)
		}
	}
	return ok
}

func (p *phase) checkCounters(whole series, in *integrity) {
	resends := p.resends()
	for _, status := range []string{statusProcessed, statusRejected, "FAILED"} {
		in.DecisionsCounted += whole.count("wagering_transactions_total", map[string]string{"status": status})
	}
	in.DuplicatesExpected = resends.landed()
	in.DuplicatesAmbiguous = resends.ambiguous
	in.DuplicatesCounted = whole.count("wagering_duplicates_total", map[string]string{"source": "idempotency"})
	in.InboxDuplicates = whole.count("wagering_duplicates_total", map[string]string{"source": "inbox"})
	in.DeadLettered = whole.count("sqs_dlq_total", nil)
	in.Divergences = whole.count("reconciliation_divergences_total", nil)

	if in.DecisionsCounted != in.DecidedOps {
		in.fail("the replicas counted %d decisions, the operations add up to %d", in.DecisionsCounted, in.DecidedOps)
	}
	if in.DuplicatesCounted < in.DuplicatesExpected || in.DuplicatesCounted > in.DuplicatesExpected+in.DuplicatesAmbiguous {
		in.fail("the replicas counted %d duplicates, the resends add up to %d (plus up to %d ambiguous)",
			in.DuplicatesCounted, in.DuplicatesExpected, in.DuplicatesAmbiguous)
	}
	if resends.httpReplayBad > 0 {
		in.fail("%d HTTP replays did not return the stored outcome", resends.httpReplayBad)
	}
	if in.DeadLettered != 0 {
		in.fail("%d messages went to the dead-letter queue", in.DeadLettered)
	}
	if in.Divergences != 0 {
		in.fail("%d reconciliation divergences", in.Divergences)
	}
}
