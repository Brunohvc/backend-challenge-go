// Package metrics holds the Prometheus collectors of the service and the admin server.
package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics records the business and operational metrics of one instance.
type Metrics struct {
	transactions  *prometheus.CounterVec
	processing    *prometheus.HistogramVec
	duplicates    *prometheus.CounterVec
	txRetries     *prometheus.CounterVec
	divergences   prometheus.Counter
	messages      *prometheus.CounterVec
	dlq           *prometheus.CounterVec
	published     *prometheus.CounterVec
	pending       prometheus.Gauge
	oldestPending prometheus.Gauge
	resolutions   *prometheus.CounterVec
	httpRequests  *prometheus.CounterVec
	httpDurations *prometheus.HistogramVec
}

// New returns the metrics and the private registry they are registered on.
func New() (*Metrics, *prometheus.Registry) {
	m := &Metrics{
		transactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_transactions_total",
			Help: "Wagering transactions decided, by channel, kind, final status and failure code.",
		}, []string{"channel", "kind", "status", "failure_code"}),
		processing: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wagering_processing_seconds",
			Help:    "Time to process a wagering transaction, by channel and kind.",
			Buckets: prometheus.DefBuckets,
		}, []string{"channel", "kind"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_duplicates_total",
			Help: "Wagering transactions answered from a previous outcome, by channel and where the duplicate was caught.",
		}, []string{"channel", "source"}),
		txRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "db_tx_retries_total",
			Help: "Database transactions retried, by retryable reason.",
		}, []string{"reason"}),
		divergences: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reconciliation_divergences_total",
			Help: "Wallets whose balance disagreed with the sum of their ledger entries.",
		}),
		messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_messages_total",
			Help: "Input queue messages handled, by result.",
		}, []string{"result"}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_dlq_total",
			Help: "Input queue messages sent to the dead-letter queue, by reason.",
		}, []string{"reason"}),
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_publish_total",
			Help: "Outbox events publish attempts, by result.",
		}, []string{"result"}),
		pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_pending_events",
			Help: "Outbox events not yet published.",
		}),
		oldestPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_oldest_pending_age_seconds",
			Help: "Age of the oldest outbox event not yet published.",
		}),
		resolutions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reference_resolutions_total",
			Help: "Background resolutions of transaction references, by result.",
		}, []string{"result"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests served, by route pattern, method and status code.",
		}, []string{"route", "method", "code"}),
		httpDurations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_seconds",
			Help:    "Time to serve an HTTP request, by route pattern and method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.transactions, m.processing, m.duplicates, m.txRetries, m.divergences,
		m.messages, m.dlq, m.published, m.pending, m.oldestPending,
		m.resolutions, m.httpRequests, m.httpDurations,
	)
	return m, reg
}

// Transaction records a decided wagering transaction and its duration.
func (m *Metrics) Transaction(channel, kind, status, failureCode string, d time.Duration) {
	m.transactions.WithLabelValues(channel, kind, status, failureCode).Inc()
	m.processing.WithLabelValues(channel, kind).Observe(d.Seconds())
}

// Duplicate records a transaction that was answered from a previous outcome.
func (m *Metrics) Duplicate(channel, source string) {
	m.duplicates.WithLabelValues(channel, source).Inc()
}

// TxRetry records a database transaction retried for reason.
func (m *Metrics) TxRetry(reason string) {
	m.txRetries.WithLabelValues(reason).Inc()
}

// ReconciliationDivergence records a wallet whose balance disagrees with its ledger.
func (m *Metrics) ReconciliationDivergence() {
	m.divergences.Inc()
}

// Message records an input queue message handled with result.
func (m *Metrics) Message(result string) {
	m.messages.WithLabelValues(result).Inc()
}

// DLQ records an input queue message sent to the dead-letter queue.
func (m *Metrics) DLQ(reason string) {
	m.dlq.WithLabelValues(reason).Inc()
}

// Published records an outbox publish attempt with result.
func (m *Metrics) Published(result string) {
	m.published.WithLabelValues(result).Inc()
}

// Pending records the outbox backlog size and the age of its oldest event.
func (m *Metrics) Pending(n int64, oldest time.Duration) {
	m.pending.Set(float64(n))
	m.oldestPending.Set(oldest.Seconds())
}

// Resolution records a background reference resolution with result.
func (m *Metrics) Resolution(result string) {
	m.resolutions.WithLabelValues(result).Inc()
}

// Observe records an HTTP request by its matched route pattern.
func (m *Metrics) Observe(route, method string, code int, d time.Duration) {
	m.httpRequests.WithLabelValues(route, method, strconv.Itoa(code)).Inc()
	m.httpDurations.WithLabelValues(route, method).Observe(d.Seconds())
}
