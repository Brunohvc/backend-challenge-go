package wager_test

import (
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

func TestKind_Values(t *testing.T) {
	tests := []struct {
		kind wager.Kind
		want string
	}{
		{wager.KindOpening, "OPENING"},
		{wager.KindBet, "BET"},
		{wager.KindWin, "WIN"},
		{wager.KindLoss, "LOSS"},
		{wager.KindRefund, "REFUND"},
		{wager.KindRollback, "ROLLBACK"},
	}
	for _, tc := range tests {
		if string(tc.kind) != tc.want {
			t.Errorf("kind = %q, want %q", tc.kind, tc.want)
		}
	}
}

func TestStatus_Values(t *testing.T) {
	tests := []struct {
		status wager.Status
		want   string
	}{
		{wager.StatusPending, "PENDING"},
		{wager.StatusPendingReference, "PENDING_REFERENCE"},
		{wager.StatusProcessed, "PROCESSED"},
		{wager.StatusRejected, "REJECTED"},
		{wager.StatusFailed, "FAILED"},
	}
	for _, tc := range tests {
		if string(tc.status) != tc.want {
			t.Errorf("status = %q, want %q", tc.status, tc.want)
		}
	}
}

func TestOrigin_Values(t *testing.T) {
	if got := string(wager.OriginInternal); got != "INTERNAL" {
		t.Errorf("OriginInternal = %q, want INTERNAL", got)
	}
	if got := string(wager.OriginExternal); got != "EXTERNAL" {
		t.Errorf("OriginExternal = %q, want EXTERNAL", got)
	}
}

func TestStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		status wager.Status
		want   bool
	}{
		{wager.StatusPending, false},
		{wager.StatusPendingReference, false},
		{wager.StatusProcessed, true},
		{wager.StatusRejected, true},
		{wager.StatusFailed, true},
		{wager.Status(""), false},
		{wager.Status("UNKNOWN"), false},
	}
	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			if got := tc.status.IsTerminal(); got != tc.want {
				t.Errorf("IsTerminal() = %v, want %v", got, tc.want)
			}
		})
	}
}
