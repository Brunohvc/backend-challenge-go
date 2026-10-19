//go:build load

package main

import (
	"strings"
	"testing"
)

func entry(tx, direction, amount, before, after string) ledgerItem {
	return ledgerItem{
		TransactionID: tx, Direction: direction, Amount: money{Amount: amount, Currency: currency},
		BalanceBefore: money{Amount: before, Currency: currency}, BalanceAfter: money{Amount: after, Currency: currency},
	}
}

func TestLedgerMatches(t *testing.T) {
	t.Parallel()

	opening := entry("open", "CREDIT", "100.00", "0.00", "100.00")
	bet := entry("t1", "DEBIT", "10.00", "100.00", "90.00")
	win := entry("t2", "CREDIT", "5.00", "90.00", "95.00")
	expect := func(ids ...string) *walletExpectation {
		e := &walletExpectation{movements: int64(len(ids)), txIDs: map[string]bool{}}
		for _, id := range ids {
			e.txIDs[id] = true
		}
		return e
	}

	tests := []struct {
		name    string
		items   []ledgerItem
		expect  *walletExpectation
		problem string
	}{
		{"matches", []ledgerItem{opening, bet, win}, expect("t1", "t2"), ""},
		{"only the opening", []ledgerItem{opening}, expect(), ""},
		{"an entry nobody answered", []ledgerItem{opening, bet, win}, expect("t1"), "no processed operation answered"},
		{"a processed operation without entry", []ledgerItem{opening, bet}, expect("t1", "t2"), "has no ledger entry"},
		{"two entries for one transaction", []ledgerItem{opening, bet, bet}, expect("t1", "t1x"), "two ledger entries"},
		{"no opening first", []ledgerItem{bet, opening, win}, expect("open", "t2"), "opening credit"},
		{"empty ledger", nil, expect(), "0 entries"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var problems []string
			ok := ledgerMatches(tc.items, 10000, tc.expect, &problems)
			if tc.problem == "" {
				if !ok || len(problems) > 0 {
					t.Errorf("ledgerMatches() = %t, %q, want a match", ok, problems)
				}
				return
			}
			if ok || !strings.Contains(strings.Join(problems, "; "), tc.problem) {
				t.Errorf("ledgerMatches() = %t, %q, want a problem mentioning %q", ok, problems, tc.problem)
			}
		})
	}
}

func TestResendStats_Landed(t *testing.T) {
	t.Parallel()

	var total resendStats
	total.add(resendStats{httpReplays: 2, sqsReplays: 1, crossToSQS: 3, racesWonHTTP: 1, racesWonSQS: 2, racesFailed: 4, ambiguous: 1})
	total.add(resendStats{httpReplays: 1, firstSendReplays: 2})
	if got := total.landed(); got != 12 {
		t.Errorf("landed() = %d, want 12: failed races leave no duplicate behind", got)
	}
	if total.ambiguous != 1 || total.racesFailed != 4 {
		t.Errorf("merged = %+v", total)
	}
}
