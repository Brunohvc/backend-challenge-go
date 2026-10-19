//go:build load

package main

import (
	"fmt"
	"slices"
	"time"
)

type latency struct {
	Count  int   `json:"count"`
	P50Us  int64 `json:"p50Us"`
	P95Us  int64 `json:"p95Us"`
	P99Us  int64 `json:"p99Us"`
	MaxUs  int64 `json:"maxUs"`
	MeanUs int64 `json:"meanUs"`
}

func percentile(sorted []int64, permille int) int64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := (permille*n + 999) / 1000
	rank = max(rank, 1)
	return sorted[min(rank, n)-1]
}

func summarize(us []int64) latency {
	if len(us) == 0 {
		return latency{}
	}
	sorted := slices.Clone(us)
	slices.Sort(sorted)
	var sum int64
	for _, v := range sorted {
		sum += v
	}
	n := int64(len(sorted))
	return latency{
		Count:  len(sorted),
		P50Us:  percentile(sorted, 500),
		P95Us:  percentile(sorted, 950),
		P99Us:  percentile(sorted, 990),
		MaxUs:  sorted[len(sorted)-1],
		MeanUs: (sum + n/2) / n,
	}
}

func perSecondTenths(count int64, elapsed time.Duration) int64 {
	if elapsed <= 0 {
		return 0
	}
	ns := int64(elapsed)
	return (count*10*int64(time.Second) + ns/2) / ns
}

func formatTenths(v int64) string {
	return fmt.Sprintf("%d.%d", v/10, v%10)
}

func formatMillis(us int64) string {
	sign := ""
	if us < 0 {
		sign, us = "-", -us
	}
	hundredths := (us + 5) / 10
	return fmt.Sprintf("%s%d.%02d", sign, hundredths/100, hundredths%100)
}
