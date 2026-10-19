//go:build load

package main

import (
	"io"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestParseConfig_Defaults(t *testing.T) {
	t.Parallel()

	c, err := parseConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if !slices.Equal(c.concurrency, []int{16, 64}) || !slices.Equal(c.phases, []string{phaseMixed, phaseContention}) {
		t.Errorf("levels %v and phases %v, want 16,64 and mixed,contention", c.concurrency, c.phases)
	}
	if len(c.metrics) != 3 || c.wallets != 200 || c.hotWallets != 5 || c.sqsShare != 20 {
		t.Errorf("defaults = %+v", c)
	}
	if c.warmup != 5*time.Second || c.duration != 10*time.Second || c.initialBalance != 1_000_000_000 {
		t.Errorf("warm-up %s, duration %s, initial balance %d", c.warmup, c.duration, c.initialBalance)
	}
}

func TestParseConfig_Overrides(t *testing.T) {
	t.Parallel()

	c, err := parseConfig([]string{
		"-concurrency", "4, 8,4", "-phases", "contention", "-warmup", "0s", "-duration", "5s",
		"-initial-balance", "50.00", "-metrics", "http://a/metrics",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if !slices.Equal(c.concurrency, []int{4, 8}) || !slices.Equal(c.phases, []string{phaseContention}) {
		t.Errorf("levels %v and phases %v", c.concurrency, c.phases)
	}
	if c.warmup != 0 || c.duration != 5*time.Second || c.initialBalance != 5000 || !slices.Equal(c.metrics, []string{"http://a/metrics"}) {
		t.Errorf("parsed = %+v", c)
	}
}

func TestParseConfig_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		args []string
		want string
	}{
		{[]string{"-concurrency", "0"}, "-concurrency"},
		{[]string{"-concurrency", "x"}, "-concurrency"},
		{[]string{"-phases", "soak"}, "unknown phase"},
		{[]string{"-duration", "0s"}, "-duration"},
		{[]string{"-sqs-share", "101"}, "percentages"},
		{[]string{"-replay-share", "60", "-cross-share", "50"}, "must not pass 100"},
		{[]string{"-initial-balance", "10"}, "-initial-balance"},
		{[]string{"-initial-balance", "0.00"}, "-initial-balance"},
		{[]string{"-metrics", " , "}, "-metrics"},
		{[]string{"extra"}, "unexpected arguments"},
	}
	for _, tc := range tests {
		_, err := parseConfig(tc.args, io.Discard)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("parseConfig(%q) error = %v, want one mentioning %q", tc.args, err, tc.want)
		}
	}
}
