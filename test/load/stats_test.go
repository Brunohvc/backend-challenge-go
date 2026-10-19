//go:build load

package main

import (
	"slices"
	"testing"
	"time"
)

func TestPercentile_NearestRank(t *testing.T) {
	t.Parallel()

	hundred := make([]int64, 100)
	for i := range hundred {
		hundred[i] = int64(i + 1)
	}
	ten := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}

	tests := []struct {
		name     string
		sorted   []int64
		permille int
		want     int64
	}{
		{"empty", nil, 500, 0},
		{"one sample is every percentile", []int64{7}, 990, 7},
		{"p50 of 1..100", hundred, 500, 50},
		{"p95 of 1..100", hundred, 950, 95},
		{"p99 of 1..100", hundred, 990, 99},
		{"p100 is the max", hundred, 1000, 100},
		{"p50 of ten rounds the rank up", ten, 500, 50},
		{"p95 of ten is the last", ten, 950, 100},
		{"p0 is the min", ten, 0, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := percentile(tc.sorted, tc.permille); got != tc.want {
				t.Errorf("percentile(%d) = %d, want %d", tc.permille, got, tc.want)
			}
		})
	}
}

func TestSummarize(t *testing.T) {
	t.Parallel()

	in := []int64{9, 1, 5, 3, 7, 2}
	got := summarize(in)
	want := latency{Count: 6, P50Us: 3, P95Us: 9, P99Us: 9, MaxUs: 9, MeanUs: 5}
	if got != want {
		t.Errorf("summarize() = %+v, want %+v", got, want)
	}
	if !slices.Equal(in, []int64{9, 1, 5, 3, 7, 2}) {
		t.Errorf("summarize() reordered its input: %v", in)
	}
	if got := summarize(nil); got != (latency{}) {
		t.Errorf("summarize(nil) = %+v, want zero", got)
	}
}

func TestSummarize_MeanRoundsHalfUp(t *testing.T) {
	t.Parallel()

	if got := summarize([]int64{1, 2}).MeanUs; got != 2 {
		t.Errorf("mean of 1 and 2 = %d, want 2", got)
	}
	if got := summarize([]int64{1, 1, 2}).MeanUs; got != 1 {
		t.Errorf("mean of 1, 1 and 2 = %d, want 1", got)
	}
}

func TestPerSecondTenths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		count   int64
		elapsed time.Duration
		want    string
	}{
		{"exact", 600, time.Minute, "10.0"},
		{"one decimal", 7, 2 * time.Second, "3.5"},
		{"truncates below half a tenth", 1, 3 * time.Second, "0.3"},
		{"rounds half a tenth up", 1, 4 * time.Second, "0.3"},
		{"large count", 1_234_567, time.Minute, "20576.1"},
		{"no time", 10, 0, "0.0"},
		{"nothing counted", 0, time.Minute, "0.0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formatTenths(perSecondTenths(tc.count, tc.elapsed)); got != tc.want {
				t.Errorf("rate of %d in %s = %s, want %s", tc.count, tc.elapsed, got, tc.want)
			}
		})
	}
}

func TestFormatMillis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		us   int64
		want string
	}{
		{0, "0.00"},
		{4, "0.00"},
		{5, "0.01"},
		{1234, "1.23"},
		{1235, "1.24"},
		{999_995, "1000.00"},
		{-1500, "-1.50"},
	}
	for _, tc := range tests {
		if got := formatMillis(tc.us); got != tc.want {
			t.Errorf("formatMillis(%d) = %s, want %s", tc.us, got, tc.want)
		}
	}
}
