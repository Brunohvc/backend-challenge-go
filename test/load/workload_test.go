//go:build load

package main

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

var (
	textIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	keyPattern    = regexp.MustCompile(`^[\x21-\x7E]{1,255}$`)
)

var defaultMix = mix{sqsShare: 20, replayShare: 5, crossShare: 5}

func TestPlanRound_SequencesFollowTheBusinessRules(t *testing.T) {
	t.Parallel()

	r := mixRand(1, 16, 0)
	for range 20_000 {
		p := planRound(r, defaultMix)
		bet := p.ops[0]
		if bet.kind != kindBet || bet.ref != -1 || bet.amount < minBet || bet.amount > maxBet {
			t.Fatalf("%s: first operation %+v, want a BET of %d to %d with no reference", p.shape, bet, minBet, maxBet)
		}
		for i := 1; i < len(p.ops); i++ {
			op := p.ops[i]
			if op.ref >= i {
				t.Fatalf("%s: operation %d references %d, which comes later", p.shape, i, op.ref)
			}
			ref := refKind(p.ops, i)
			switch op.kind {
			case kindLoss:
				if op.amount != 0 || op.ref != -1 {
					t.Fatalf("%s: LOSS %+v, want 0.00 with no reference", p.shape, op)
				}
			case kindWin:
				if ref != kindBet || op.amount < 1 || op.amount > 2*bet.amount {
					t.Fatalf("%s: WIN %+v of a %s, want 0.01 to twice the bet on a BET", p.shape, op, ref)
				}
			case kindRefund:
				if ref != kindBet || op.amount != p.ops[op.ref].amount {
					t.Fatalf("%s: REFUND %+v of a %s, want the amount of a BET", p.shape, op, ref)
				}
			case kindRollback:
				if !slices.Contains([]string{kindBet, kindWin, kindRefund}, ref) || op.amount != p.ops[op.ref].amount {
					t.Fatalf("%s: ROLLBACK %+v of a %s, want the amount of a BET, WIN or REFUND", p.shape, op, ref)
				}
			default:
				t.Fatalf("%s: unexpected follow-up %+v", p.shape, op)
			}
		}
		if p.resend != resendNone && (p.resendAt < 0 || p.resendAt >= len(p.ops)) {
			t.Fatalf("%s: resend at %d of %d operations", p.shape, p.resendAt, len(p.ops))
		}
		if p.resend == resendCross && p.sqs && p.resendAt != 0 {
			t.Fatalf("%s: an SQS round races operation %d over HTTP, want only the BET", p.shape, p.resendAt)
		}
		racing := p.resend == resendCross && p.sqs
		if p.raceDelay < 0 || p.raceDelay >= maxRaceDelay || (!racing && p.raceDelay != 0) {
			t.Fatalf("%s: race delay %s for a round that races %t", p.shape, p.raceDelay, racing)
		}
	}
}

func TestPlanRound_SharesFollowTheMix(t *testing.T) {
	t.Parallel()

	const n = 100_000
	r := mixRand(7, 64, 3)
	var sqs, replays, crosses int
	shapesSeen := map[string]int{}
	for range n {
		p := planRound(r, defaultMix)
		shapesSeen[p.shape]++
		if p.sqs {
			sqs++
		}
		switch p.resend {
		case resendReplay:
			replays++
		case resendCross:
			crosses++
		}
	}

	within := func(name string, got, wantPercent int) {
		t.Helper()
		if lo, hi := n*wantPercent/100-n/100, n*wantPercent/100+n/100; got < lo || got > hi {
			t.Errorf("%s: %d of %d rounds, want about %d%%", name, got, n, wantPercent)
		}
	}
	within("SQS rounds", sqs, defaultMix.sqsShare)
	within("replays", replays, defaultMix.replayShare)
	within("cross-channel resends", crosses, defaultMix.crossShare)
	for _, s := range shapes {
		within(s.name, shapesSeen[s.name], s.weight)
	}
}

func TestPlanRound_SameSeedSamePlans(t *testing.T) {
	t.Parallel()

	a, b := mixRand(42, 16, 5), mixRand(42, 16, 5)
	other := mixRand(42, 16, 6)
	differs := false
	for range 1000 {
		pa, pb, po := planRound(a, defaultMix), planRound(b, defaultMix), planRound(other, defaultMix)
		if pa.shape != pb.shape || pa.sqs != pb.sqs || pa.resend != pb.resend || !slices.Equal(pa.ops, pb.ops) {
			t.Fatalf("same seed and worker gave %+v and %+v", pa, pb)
		}
		if !slices.Equal(pa.ops, po.ops) {
			differs = true
		}
	}
	if !differs {
		t.Error("two workers drew the same 1000 rounds")
	}
}

func TestEffect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind, ref string
		want      int64
	}{
		{kindBet, "", -500},
		{kindWin, kindBet, 500},
		{kindWin, "", 500},
		{kindLoss, "", 0},
		{kindRefund, kindBet, 500},
		{kindRollback, kindBet, 500},
		{kindRollback, kindWin, -500},
		{kindRollback, kindRefund, -500},
	}
	for _, tc := range tests {
		if got := effect(tc.kind, tc.ref, 500); got != tc.want {
			t.Errorf("effect(%s of %s) = %d, want %d", tc.kind, tc.ref, got, tc.want)
		}
	}
}

func TestShapes_NetEffect(t *testing.T) {
	t.Parallel()

	const bet = 1000
	r := mixRand(3, 1, 0)
	for _, s := range shapes {
		ops := s.build(r, bet)
		var net, won int64
		for i, op := range ops {
			net += effect(op.kind, refKind(ops, i), op.amount)
			if op.kind == kindWin {
				won = op.amount
			}
		}
		want := map[string]int64{
			"bet-win":             won - bet,
			"bet-loss":            -bet,
			"bet-refund":          0,
			"bet-rollback":        0,
			"bet-win-rollback":    -bet,
			"bet-refund-rollback": -bet,
		}[s.name]
		if net != want {
			t.Errorf("%s: net effect %d, want %d", s.name, net, want)
		}
	}
}

func TestIdentifiers_MatchTheAPIPatterns(t *testing.T) {
	t.Parallel()

	round := roundID("0123456789ab", "c1024", 9_223_372_036_854_775_807)
	ext := externalID(round, 2)
	for _, id := range []string{round, ext} {
		if !textIDPattern.MatchString(id) {
			t.Errorf("%q does not match the text id pattern", id)
		}
	}
	for _, p := range providers {
		if key := idempotencyKey(p, ext); !keyPattern.MatchString(key) || !strings.HasPrefix(key, p+":") {
			t.Errorf("key %q does not match the idempotency key pattern", key)
		}
	}
	if gameID == "" || !textIDPattern.MatchString(gameID) {
		t.Errorf("game id %q does not match the text id pattern", gameID)
	}
}

func TestMovesMoney(t *testing.T) {
	t.Parallel()

	for _, k := range kinds {
		if got, want := movesMoney(k), k != kindLoss; got != want {
			t.Errorf("movesMoney(%s) = %t, want %t", k, got, want)
		}
	}
}
