package wager_test

import (
	"strings"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

func TestValidTextID(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{"every kind of character allowed", "Az09._:-", true},
		{"the longest", strings.Repeat("a", 128), true},
		{"empty", "", false},
		{"one too long", strings.Repeat("a", 129), false},
		{"a space", "round 1", false},
		{"a trailing newline", "round-1\n", false},
		{"a slash", "round/1", false},
		{"a letter outside ASCII", "rodada-é", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := wager.ValidTextID(tt.s); got != tt.want {
				t.Errorf("ValidTextID(%q) = %t, want %t", tt.s, got, tt.want)
			}
		})
	}
}
