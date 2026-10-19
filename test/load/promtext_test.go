//go:build load

package main

import (
	"maps"
	"strings"
	"testing"
)

func TestParseMicros(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in     string
		want   int64
		wantOK bool
	}{
		{"0", 0, true},
		{"1", 1_000_000, true},
		{"42", 42_000_000, true},
		{"1.5", 1_500_000, true},
		{"0.005", 5_000, true},
		{"0.0123", 12_300, true},
		{"1e+06", 1_000_000_000_000, true},
		{"1.5e-05", 15, true},
		{"2.5E3", 2_500_000_000, true},
		{"-3", -3_000_000, true},
		{"+7", 7_000_000, true},
		{"0.0000001", 0, true},
		{"1e-7", 0, true},
		{"1e-400", 0, true},
		{"0e+400", 0, true},
		{"123456.789012345678", 123_456_789_012, true},
		{"1e+30", 0, false},
		{"12345678901234567890", 0, false},
		{"NaN", 0, false},
		{"+Inf", 0, false},
		{"-Inf", 0, false},
		{"", 0, false},
		{".", 0, false},
		{"1e", 0, false},
		{"1.2.3", 0, false},
		{"0x10", 0, false},
	}
	for _, tc := range tests {
		got, ok := parseMicros(tc.in)
		if ok != tc.wantOK || got != tc.want {
			t.Errorf("parseMicros(%q) = %d, %t, want %d, %t", tc.in, got, ok, tc.want, tc.wantOK)
		}
	}
}

const exposition = `# HELP wagering_duplicates_total Duplicates.
# TYPE wagering_duplicates_total counter
wagering_duplicates_total{channel="http",source="idempotency"} 12
wagering_duplicates_total{channel="sqs",source="inbox"} 3
db_tx_retries_total{reason="lock_timeout"} 2 1700000000000
outbox_pending_events 0
outbox_oldest_pending_age_seconds 1.25e-01
odd_labels{b="x\"y",a="c\\d,e}f"} 4
http_request_seconds_bucket{route="POST /wagering/transactions",le="0.005"} 9
not_a_number NaN
`

func TestParseExposition(t *testing.T) {
	t.Parallel()

	keep := func(name string) bool { return !strings.HasSuffix(name, "_bucket") }
	got, err := parseExposition(strings.NewReader(exposition), keep)
	if err != nil {
		t.Fatalf("parseExposition() error = %v", err)
	}

	want := map[string]int64{
		`wagering_duplicates_total{channel="http",source="idempotency"}`: 12_000_000,
		`wagering_duplicates_total{channel="sqs",source="inbox"}`:        3_000_000,
		`db_tx_retries_total{reason="lock_timeout"}`:                     2_000_000,
		`outbox_pending_events`:                                          0,
		`outbox_oldest_pending_age_seconds`:                              125_000,
		`odd_labels{a="c\\d,e}f",b="x\"y"}`:                              4_000_000,
	}
	values := map[string]int64{}
	for k, s := range got {
		values[k] = s.micros
	}
	if !maps.Equal(values, want) {
		t.Errorf("parseExposition() = %v, want %v", values, want)
	}
	odd := got[`odd_labels{a="c\\d,e}f",b="x\"y"}`]
	if odd.labels["a"] != `c\d,e}f` || odd.labels["b"] != `x"y` {
		t.Errorf("labels = %q, want the unescaped values", odd.labels)
	}
}

func TestParseExposition_Malformed(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"name_without_value", `bad{a="unterminated} 1`, `bad{a=1} 1`, "bad{} "} {
		if _, err := parseExposition(strings.NewReader(in+"\n"), func(string) bool { return true }); err == nil {
			t.Errorf("parseExposition(%q) error = nil, want an error", in)
		}
	}
}

func scrape(t *testing.T, text string) series {
	t.Helper()
	s, err := parseExposition(strings.NewReader(text), func(string) bool { return true })
	if err != nil {
		t.Fatalf("parseExposition() error = %v", err)
	}
	return s
}

func TestCounterDelta(t *testing.T) {
	t.Parallel()

	before := scrape(t, `
db_tx_retries_total{reason="lock_timeout"} 5
db_tx_retries_total{reason="unique_race"} 9
wagering_duplicates_total{channel="http",source="idempotency"} 2
`)
	after := scrape(t, `
db_tx_retries_total{reason="lock_timeout"} 8
db_tx_retries_total{reason="unique_race"} 4
db_tx_retries_total{reason="deadlock"} 1
`)
	d := counterDelta(before, after)

	tests := []struct {
		reason string
		want   int64
	}{
		{"lock_timeout", 3},
		{"unique_race", 4},
		{"deadlock", 1},
	}
	for _, tc := range tests {
		if got := d.count("db_tx_retries_total", map[string]string{"reason": tc.reason}); got != tc.want {
			t.Errorf("delta of %s = %d, want %d", tc.reason, got, tc.want)
		}
	}
	if got := d.count("wagering_duplicates_total", nil); got != 0 {
		t.Errorf("a series gone from the second scrape counted %d, want 0", got)
	}
}

func TestDeltaAll_SumsReplicas(t *testing.T) {
	t.Parallel()

	before := []series{
		scrape(t, `wagering_transactions_total{channel="http",kind="BET",status="PROCESSED",failure_code=""} 10`),
		scrape(t, `wagering_transactions_total{channel="sqs",kind="BET",status="PROCESSED",failure_code=""} 1`),
	}
	after := []series{
		scrape(t, `wagering_transactions_total{channel="http",kind="BET",status="PROCESSED",failure_code=""} 25
wagering_transactions_total{channel="http",kind="WIN",status="PROCESSED",failure_code=""} 4`),
		scrape(t, `wagering_transactions_total{channel="sqs",kind="BET",status="PROCESSED",failure_code=""} 6
wagering_transactions_total{channel="http",kind="BET",status="PROCESSED",failure_code=""} 3`),
	}
	d := deltaAll(before, after)

	if got := d.count("wagering_transactions_total", nil); got != 27 {
		t.Errorf("total = %d, want 27", got)
	}
	if got := d.count("wagering_transactions_total", map[string]string{"channel": "http", "kind": "BET"}); got != 18 {
		t.Errorf("http BET = %d, want 18", got)
	}
	byChannel := d.byLabel("wagering_transactions_total", "channel")
	if byChannel["http"] != 22_000_000 || byChannel["sqs"] != 5_000_000 {
		t.Errorf("byLabel(channel) = %v, want http 22 and sqs 5 in micro-units", byChannel)
	}
}

func TestConsumedMessages(t *testing.T) {
	t.Parallel()

	snaps := []series{
		scrape(t, `sqs_messages_total{result="processed"} 7
sqs_messages_total{result="retry"} 4
sqs_messages_total{result="duplicate"} 2`),
		scrape(t, `sqs_messages_total{result="dlq"} 1
sqs_messages_total{result="processed"} 3`),
	}
	if got := consumedMessages(snaps); got != 13 {
		t.Errorf("consumedMessages() = %d, want 13: every result but retry", got)
	}
}
