//go:build load

package main

import (
	"fmt"
	"math/rand/v2"
	"time"
)

const (
	kindBet      = "BET"
	kindWin      = "WIN"
	kindLoss     = "LOSS"
	kindRefund   = "REFUND"
	kindRollback = "ROLLBACK"
)

var kinds = []string{kindBet, kindWin, kindLoss, kindRefund, kindRollback}

const (
	minBet       = 100
	maxBet       = 5000
	gameID       = "load-test"
	maxRaceDelay = 400 * time.Millisecond
)

type opSpec struct {
	kind   string
	amount int64
	ref    int
}

type resend int

const (
	resendNone resend = iota
	resendReplay
	resendCross
)

type roundPlan struct {
	shape     string
	ops       []opSpec
	sqs       bool
	resend    resend
	resendAt  int
	raceDelay time.Duration
}

type mix struct {
	sqsShare    int
	replayShare int
	crossShare  int
}

type shape struct {
	name   string
	weight int
	build  func(r *rand.Rand, bet int64) []opSpec
}

var shapes = []shape{
	{name: "bet-win", weight: 30, build: func(r *rand.Rand, bet int64) []opSpec {
		return []opSpec{{kindBet, bet, -1}, {kindWin, prize(r, bet), 0}}
	}},
	{name: "bet-loss", weight: 30, build: func(_ *rand.Rand, bet int64) []opSpec {
		return []opSpec{{kindBet, bet, -1}, {kindLoss, 0, -1}}
	}},
	{name: "bet-refund", weight: 10, build: func(_ *rand.Rand, bet int64) []opSpec {
		return []opSpec{{kindBet, bet, -1}, {kindRefund, bet, 0}}
	}},
	{name: "bet-rollback", weight: 10, build: func(_ *rand.Rand, bet int64) []opSpec {
		return []opSpec{{kindBet, bet, -1}, {kindRollback, bet, 0}}
	}},
	{name: "bet-win-rollback", weight: 10, build: func(r *rand.Rand, bet int64) []opSpec {
		won := prize(r, bet)
		return []opSpec{{kindBet, bet, -1}, {kindWin, won, 0}, {kindRollback, won, 1}}
	}},
	{name: "bet-refund-rollback", weight: 10, build: func(_ *rand.Rand, bet int64) []opSpec {
		return []opSpec{{kindBet, bet, -1}, {kindRefund, bet, 0}, {kindRollback, bet, 1}}
	}},
}

func mixRand(seed uint64, conc, worker int) *rand.Rand {
	return rand.New(rand.NewPCG(seed, uint64(conc)<<32|uint64(worker))) //nolint:gosec // a seeded, reproducible mix, not a secret
}

func prize(r *rand.Rand, bet int64) int64 {
	return 1 + r.Int64N(2*bet)
}

func pickShape(r *rand.Rand) shape {
	total := 0
	for _, s := range shapes {
		total += s.weight
	}
	roll := r.IntN(total)
	for _, s := range shapes {
		if roll < s.weight {
			return s
		}
		roll -= s.weight
	}
	return shapes[len(shapes)-1]
}

func planRound(r *rand.Rand, m mix) roundPlan {
	bet := minBet + r.Int64N(maxBet-minBet+1)
	s := pickShape(r)
	p := roundPlan{shape: s.name, ops: s.build(r, bet), sqs: r.IntN(100) < m.sqsShare}
	switch roll := r.IntN(100); {
	case roll < m.replayShare:
		p.resend, p.resendAt = resendReplay, r.IntN(len(p.ops))
	case roll < m.replayShare+m.crossShare:
		p.resend = resendCross
		if p.sqs {
			p.raceDelay = time.Duration(r.Int64N(int64(maxRaceDelay)))
		} else {
			p.resendAt = r.IntN(len(p.ops))
		}
	}
	return p
}

func refKind(ops []opSpec, i int) string {
	if ops[i].ref < 0 {
		return ""
	}
	return ops[ops[i].ref].kind
}

func effect(kind, ref string, amount int64) int64 {
	switch kind {
	case kindBet:
		return -amount
	case kindWin, kindRefund:
		return amount
	case kindRollback:
		if ref == kindBet {
			return amount
		}
		return -amount
	}
	return 0
}

func movesMoney(kind string) bool {
	return kind != kindLoss
}

func roundID(runID, tag string, seq int64) string {
	return fmt.Sprintf("lt-%s-%s-%d", runID, tag, seq)
}

func externalID(round string, op int) string {
	return fmt.Sprintf("%s-%d", round, op)
}

func idempotencyKey(provider, ext string) string {
	return provider + ":" + ext
}
