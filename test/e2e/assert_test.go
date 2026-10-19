//go:build e2e

package e2e

import "testing"

func TestSumSamples(t *testing.T) {
	const scrape = `# HELP wagering_duplicates_total Submissions answered from an earlier outcome.
# TYPE wagering_duplicates_total counter
wagering_duplicates_total{channel="http",source="idempotency"} 24
wagering_duplicates_total{channel="sqs",source="idempotency"} 23
wagering_duplicates_total{channel="sqs",source="inbox"} 2
wagering_duplicates_total_extra{channel="sqs"} 100
http_requests_total{code="200",method="GET",route="GET /wallets/{walletId}"} 7
http_requests_total{code="200",method="GET",route="GET /a \"quoted\", odd\\ route"} 3
outbox_publish_total 5
`
	tests := []struct {
		name   string
		metric string
		labels map[string]string
		want   int64
	}{
		{"every sample of the counter", "wagering_duplicates_total", nil, 49},
		{"one label", "wagering_duplicates_total", labels("channel", "sqs"), 25},
		{"two labels", "wagering_duplicates_total", labels("channel", "sqs", "source", "inbox"), 2},
		{"a label no sample has", "wagering_duplicates_total", labels("channel", "grpc"), 0},
		{"a counter not exposed yet", "sqs_dlq_total", nil, 0},
		{"a value with spaces and braces", "http_requests_total", labels("route", "GET /wallets/{walletId}"), 7},
		{"a value with escapes", "http_requests_total", labels("route", `GET /a "quoted", odd\ route`), 3},
		{"a counter without labels", "outbox_publish_total", nil, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sumSamples(scrape, tt.metric, tt.labels)
			if err != nil {
				t.Fatalf("sumSamples: %v", err)
			}
			if got != tt.want {
				t.Errorf("sumSamples(%s, %v) = %d, want %d", tt.metric, tt.labels, got, tt.want)
			}
		})
	}

	if _, err := sumSamples("wagering_processing_seconds_sum 0.25\n", "wagering_processing_seconds_sum", nil); err == nil {
		t.Error("a value that is not an integer was summed, want an error")
	}
}
