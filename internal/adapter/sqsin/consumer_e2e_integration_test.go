//go:build integration

package sqsin_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wallets"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

const (
	openingCorrelation = "corr-opening"
	databasePingEvery  = 500 * time.Millisecond
)

func database(t *testing.T) *testenv.Postgres {
	t.Helper()
	pgOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		sharedPG, errPG = testenv.StartPostgres(ctx)
	})
	if errPG != nil {
		t.Fatalf("StartPostgres: %v", errPG)
	}
	return sharedPG
}

type wageringEnv struct {
	*queueEnv
	pg      *testenv.Postgres
	pool    *pgxpool.Pool
	svc     *wagering.Service
	wallets *wallets.Service
	metrics *metrics.Metrics
	reg     *prometheus.Registry
	db      *pgx.Conn
}

func newWageringEnv(t *testing.T) *wageringEnv {
	t.Helper()
	ctx := t.Context()
	e := newQueueEnv(t)
	pg := database(t)
	if err := pg.Reset(ctx); err != nil {
		t.Fatalf("reset the database: %v", err)
	}
	pool, err := postgres.NewPool(ctx, config.DBConfig{
		URL:              pg.AppDSN,
		MaxConns:         8,
		LockTimeout:      2 * time.Second,
		StatementTimeout: 10 * time.Second,
		TxMaxAttempts:    3,
	}, "sqsin-it")
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)
	db, err := pgx.Connect(ctx, pg.AppDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })

	logs := &syncBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("service logs:\n%s", logs.String())
		}
	})
	log := logging.New(logs, slog.LevelDebug, "it")
	m, reg := metrics.New()
	uow := postgres.NewUnitOfWork(pool, tracing.Tracer{})
	txPolicy := retry.Policy{Attempts: 3, Base: 5 * time.Millisecond, Max: 50 * time.Millisecond}
	return &wageringEnv{
		queueEnv: e,
		pg:       pg,
		pool:     pool,
		svc: wagering.NewService(uow, clock.System{}, idgen.V7{}, m, tracing.Tracer{}, log, wagering.Config{
			Reference:             wager.ReferencePolicy{MaxAttempts: 10, TTL: 15 * time.Minute, BackoffBase: time.Second, BackoffMax: time.Minute},
			Tx:                    txPolicy,
			PermanentFailureLimit: 3,
		}),
		wallets: wallets.NewService(uow, clock.System{}, idgen.V7{}, m, log, txPolicy),
		metrics: m,
		reg:     reg,
		db:      db,
	}
}

type orderedMetrics struct {
	*fakeMetrics
	services *metrics.Metrics
}

func (o orderedMetrics) Message(result string) {
	o.services.Message(result)
	o.fakeMetrics.Message(result)
}

func (o orderedMetrics) DLQ(reason string) {
	o.services.DLQ(reason)
	o.fakeMetrics.DLQ(reason)
}

func (w *wageringEnv) startConsumer(t *testing.T, cfg config.ConsumerConfig, client sqsin.SQSAPI, sub sqsin.Submitter) (*sqsin.Consumer, *fakeMetrics, *syncBuffer) {
	t.Helper()
	return w.startConsumerMoving(t, cfg, client, w.app, sub)
}

func (w *wageringEnv) startConsumerMoving(t *testing.T, cfg config.ConsumerConfig, client sqsin.SQSAPI, dlqClient sqsin.DLQClient, sub sqsin.Submitter) (*sqsin.Consumer, *fakeMetrics, *syncBuffer) {
	t.Helper()
	record := &fakeMetrics{}
	c, logs := w.run(t, cfg, consumerRun{
		client:    client,
		dlqClient: dlqClient,
		queues:    w.ms.Queues,
		sub:       sub,
		metrics:   orderedMetrics{fakeMetrics: record, services: w.metrics},
		ping:      w.pool.Ping,
		pingEvery: databasePingEvery,
	})
	return c, record, logs
}

func (w *wageringEnv) openWallet(t *testing.T, initial string) wallet.Snapshot {
	t.Helper()
	amount, err := money.Parse(initial, money.BRL().Code())
	if err != nil {
		t.Fatalf("money.Parse(%q): %v", initial, err)
	}
	s, err := w.wallets.Open(t.Context(), wallets.OpenCommand{
		PlayerID:      idgen.V7{}.New(),
		Initial:       amount,
		CorrelationID: openingCorrelation,
	})
	if err != nil {
		t.Fatalf("open a wallet with %s: %v", initial, err)
	}
	return s
}

type outgoing struct {
	body          string
	group         string
	dedup         string
	token         string
	correlationID string
}

func (w *wageringEnv) send(t *testing.T, client *sqs.Client, msg outgoing) string {
	t.Helper()
	attrs := map[string]types.MessageAttributeValue{sqsin.ProviderTokenAttribute: stringAttr(msg.token)}
	if msg.correlationID != "" {
		attrs[sqsin.CorrelationIDAttribute] = stringAttr(msg.correlationID)
	}
	out, err := client.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(w.ms.Queues.Input),
		MessageBody:            aws.String(msg.body),
		MessageGroupId:         aws.String(msg.group),
		MessageDeduplicationId: aws.String(msg.dedup),
		MessageAttributes:      attrs,
	})
	if err != nil {
		t.Fatalf("SendMessage %s: %v", msg.dedup, err)
	}
	return aws.ToString(out.MessageId)
}

func (w *wageringEnv) sendAsProviderA(t *testing.T, wal wallet.Snapshot, dedup, body, correlationID string) string {
	t.Helper()
	return w.send(t, w.provider, outgoing{body: body, group: wal.ID.String(), dedup: dedup, token: w.token, correlationID: correlationID})
}

func wagerOn(t *testing.T, wal wallet.Snapshot, messageID string, edit func(data map[string]any)) string {
	t.Helper()
	return wagerBody(t, messageID, func(d map[string]any) {
		d["walletId"] = wal.ID.String()
		d["playerId"] = wal.PlayerID.String()
		if edit != nil {
			edit(d)
		}
	})
}

func inboxHashOf(t *testing.T, body string) string {
	t.Helper()
	env, _, err := sqsin.ParseEnvelope([]byte(body))
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}
	h, err := sqsin.InboxHash(env)
	if err != nil {
		t.Fatalf("InboxHash: %v", err)
	}
	return h
}

func awaitSettled(t *testing.T, c *sqsin.Consumer, m *fakeMetrics, n int) {
	t.Helper()
	waitWithin(t, 30*time.Second, "the messages to be settled", func() bool { return len(m.results()) >= n })
	stopConsumer(t, c)
}

func requireResults(t *testing.T, m *fakeMetrics, want []string, reasons ...string) {
	t.Helper()
	if got := m.results(); !slices.Equal(got, want) {
		t.Errorf("results = %v, want %v", got, want)
	}
	if got := m.dlqReasons(); !slices.Equal(got, reasons) {
		t.Errorf("DLQ reasons = %v, want %v", got, reasons)
	}
}

func (w *wageringEnv) requireQueuesEmpty(t *testing.T) {
	t.Helper()
	root := w.ms.RootClient()
	requireEmpty(t, root, w.ms.Queues.Input)
	requireEmpty(t, root, w.ms.Queues.InputDLQ)
}

func (w *wageringEnv) requireDeadLetter(t *testing.T, body, sqsID, reason, code string) {
	t.Helper()
	root := w.ms.RootClient()
	moved := receiveAll(t, root, w.ms.Queues.InputDLQ, 2*time.Second)
	if len(moved) != 1 {
		t.Fatalf("DLQ holds %d messages, want 1", len(moved))
	}
	if got := aws.ToString(moved[0].Body); got != body {
		t.Errorf("DLQ body = %s, want %s", got, body)
	}
	for name, want := range map[string]string{
		sqsin.FailureReasonAttribute:   reason,
		sqsin.FailureCodeAttribute:     code,
		sqsin.SourceMessageIDAttribute: sqsID,
	} {
		if got := aws.ToString(moved[0].MessageAttributes[name].StringValue); got != want {
			t.Errorf("DLQ attribute %s = %q, want %q", name, got, want)
		}
	}
	requireEmpty(t, root, w.ms.Queues.Input)
}

type storedTx struct {
	ID            uuid.UUID
	ExternalID    string
	Kind          string
	Status        string
	FailureCode   string
	CorrelationID string
}

func (w *wageringEnv) transactions(t *testing.T, id uuid.UUID) []storedTx {
	t.Helper()
	rows, err := w.db.Query(t.Context(),
		`SELECT id, external_transaction_id, kind, status, COALESCE(failure_code, ''), correlation_id
		   FROM wager_transactions
		  WHERE wallet_id = $1 AND origin = 'EXTERNAL'
		  ORDER BY created_at, id`, id)
	if err != nil {
		t.Fatalf("read the transactions: %v", err)
	}
	txs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[storedTx])
	if err != nil {
		t.Fatalf("read the transactions: %v", err)
	}
	return txs
}

func (w *wageringEnv) requireOneTransaction(t *testing.T, id uuid.UUID, want storedTx) storedTx {
	t.Helper()
	txs := w.transactions(t, id)
	if len(txs) != 1 {
		t.Fatalf("transactions = %+v, want one", txs)
	}
	got := txs[0]
	want.ID = got.ID
	if got != want {
		t.Fatalf("transaction = %+v, want %+v", got, want)
	}
	return got
}

func (w *wageringEnv) requireWallet(t *testing.T, id uuid.UUID, balanceMinor, version int64, entries ...string) {
	t.Helper()
	var gotBalance, gotVersion int64
	if err := w.db.QueryRow(t.Context(), "SELECT balance_minor, version FROM wallets WHERE id = $1", id).Scan(&gotBalance, &gotVersion); err != nil {
		t.Fatalf("read wallet %s: %v", id, err)
	}
	if gotBalance != balanceMinor || gotVersion != version {
		t.Errorf("wallet holds %d at version %d, want %d at version %d", gotBalance, gotVersion, balanceMinor, version)
	}
	rows, err := w.db.Query(t.Context(),
		"SELECT direction || ' ' || amount_minor FROM wallet_ledger_entries WHERE wallet_id = $1 ORDER BY seq", id)
	if err != nil {
		t.Fatalf("read the ledger: %v", err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read the ledger: %v", err)
	}
	if !slices.Equal(got, entries) {
		t.Errorf("ledger = %v, want %v", got, entries)
	}
}

type inboxEntry struct {
	Consumer  string
	MessageID string
	Hash      string
	Completed bool
	TxID      uuid.NullUUID
	Outcome   string
}

func completed(messageID, hash string, txID uuid.UUID, outcome string) inboxEntry {
	return inboxEntry{
		Consumer:  consumerName,
		MessageID: messageID,
		Hash:      hash,
		Completed: true,
		TxID:      uuid.NullUUID{UUID: txID, Valid: true},
		Outcome:   outcome,
	}
}

func (w *wageringEnv) requireInbox(t *testing.T, want ...inboxEntry) {
	t.Helper()
	rows, err := w.db.Query(t.Context(),
		`SELECT consumer_name, message_id, payload_hash, processed_at IS NOT NULL, transaction_id, COALESCE(outcome, '')
		   FROM inbox_messages
		  ORDER BY message_id`)
	if err != nil {
		t.Fatalf("read the inbox: %v", err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[inboxEntry])
	if err != nil {
		t.Fatalf("read the inbox: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("inbox = %+v, want %+v", got, want)
	}
}

type storedEvent struct {
	Type          string
	CorrelationID string
	CausationID   string
}

func (w *wageringEnv) requireEventsOf(t *testing.T, txID uuid.UUID, want ...storedEvent) {
	t.Helper()
	rows, err := w.db.Query(t.Context(),
		`SELECT event_type, correlation_id, COALESCE(causation_id, '')
		   FROM outbox_events
		  WHERE payload -> 'data' ->> 'transactionId' = $1
		  ORDER BY seq`, txID.String())
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[storedEvent])
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("events of %s = %+v, want %+v", txID, got, want)
	}
}

func (w *wageringEnv) eventCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := w.db.QueryRow(t.Context(), "SELECT count(*) FROM outbox_events").Scan(&n); err != nil {
		t.Fatalf("count the events: %v", err)
	}
	return n
}

func (w *wageringEnv) requireNothingWritten(t *testing.T, wal wallet.Snapshot) {
	t.Helper()
	if txs := w.transactions(t, wal.ID); len(txs) != 0 {
		t.Errorf("transactions = %+v, want none", txs)
	}
	w.requireInbox(t)
	var events int64
	if err := w.db.QueryRow(t.Context(), "SELECT count(*) FROM outbox_events WHERE correlation_id <> $1", openingCorrelation).Scan(&events); err != nil {
		t.Fatalf("count the events: %v", err)
	}
	if events != 0 {
		t.Errorf("%d events besides those of the opening, want none", events)
	}
	w.requireWallet(t, wal.ID, wal.Balance.Minor(), wal.Version, "CREDIT "+strconv.FormatInt(wal.Balance.Minor(), 10))
}

func (w *wageringEnv) count(t *testing.T, name string, labels map[string]string) int {
	t.Helper()
	families, err := w.reg.Gather()
	if err != nil {
		t.Fatalf("gather the metrics: %v", err)
	}
	n := 0
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, series := range f.GetMetric() {
			got := make(map[string]string, len(series.GetLabel()))
			for _, l := range series.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			matches := true
			for k, v := range labels {
				if got[k] != v {
					matches = false
				}
			}
			if matches {
				n += int(series.GetCounter().GetValue())
			}
		}
	}
	return n
}

func (w *wageringEnv) requireCount(t *testing.T, name string, labels map[string]string, want int) {
	t.Helper()
	if got := w.count(t, name, labels); got != want {
		t.Errorf("%s%v = %d, want %d", name, labels, got, want)
	}
}

type decision struct {
	kind, status, failureCode string
}

func (w *wageringEnv) requireCounted(t *testing.T, want decision, duplicates map[string]int) {
	t.Helper()
	labels := map[string]string{"channel": "sqs", "kind": want.kind, "status": want.status, "failure_code": want.failureCode}
	if got := w.count(t, "wagering_transactions_total", labels); got != 1 {
		t.Errorf("wagering_transactions_total%v = %d, want 1", labels, got)
	}
	if got := w.count(t, "wagering_transactions_total", map[string]string{"channel": "sqs"}); got != 1 {
		t.Errorf("wagering_transactions_total{channel=sqs} = %d in all, want 1", got)
	}
	for _, source := range []string{"inbox", "idempotency"} {
		labels := map[string]string{"channel": "sqs", "source": source}
		if got := w.count(t, "wagering_duplicates_total", labels); got != duplicates[source] {
			t.Errorf("wagering_duplicates_total%v = %d, want %d", labels, got, duplicates[source])
		}
	}
}

func TestConsumer_ProcessesBet(t *testing.T) {
	w := newWageringEnv(t)
	wal := w.openWallet(t, "100.00")
	body := wagerOn(t, wal, "msg-1", nil)
	w.sendAsProviderA(t, wal, "msg-1", body, "corr-bet-1")

	c, m, _ := w.startConsumer(t, integrationConfig(), w.app, w.svc)
	awaitSettled(t, c, m, 1)

	requireResults(t, m, []string{"processed"})
	w.requireQueuesEmpty(t)
	tx := w.requireOneTransaction(t, wal.ID, storedTx{
		ExternalID: "tx-msg-1", Kind: "BET", Status: "PROCESSED", CorrelationID: "corr-bet-1",
	})
	w.requireWallet(t, wal.ID, 7500, 2, "CREDIT 10000", "DEBIT 2500")
	w.requireInbox(t, completed("msg-1", inboxHashOf(t, body), tx.ID, "PROCESSED"))
	w.requireEventsOf(t, tx.ID,
		storedEvent{event.TypeWagerTransactionProcessed, "corr-bet-1", "msg-1"},
		storedEvent{event.TypeWalletBalanceChanged, "corr-bet-1", "msg-1"},
	)
	w.requireCounted(t, decision{"BET", "PROCESSED", ""}, nil)
}

func TestConsumer_DuplicateDelivery(t *testing.T) {
	w := newWageringEnv(t)
	wal := w.openWallet(t, "100.00")
	body := wagerOn(t, wal, "msg-1", nil)
	w.sendAsProviderA(t, wal, "msg-1", body, "")
	w.sendAsProviderA(t, wal, "msg-1-sent-again", body, "")

	c, m, _ := w.startConsumer(t, integrationConfig(), w.app, w.svc)
	awaitSettled(t, c, m, 2)

	requireResults(t, m, []string{"processed", "duplicate"})
	w.requireCount(t, "sqs_messages_total", map[string]string{"result": "duplicate"}, 1)
	w.requireQueuesEmpty(t)
	tx := w.requireOneTransaction(t, wal.ID, storedTx{
		ExternalID: "tx-msg-1", Kind: "BET", Status: "PROCESSED", CorrelationID: "msg-1",
	})
	w.requireWallet(t, wal.ID, 7500, 2, "CREDIT 10000", "DEBIT 2500")
	w.requireInbox(t, completed("msg-1", inboxHashOf(t, body), tx.ID, "PROCESSED"))
	w.requireEventsOf(t, tx.ID,
		storedEvent{event.TypeWagerTransactionProcessed, "msg-1", "msg-1"},
		storedEvent{event.TypeWalletBalanceChanged, "msg-1", "msg-1"},
	)
	w.requireCounted(t, decision{"BET", "PROCESSED", ""}, map[string]int{"inbox": 1})
}

func TestConsumer_SameKeyDifferentMessageIDs(t *testing.T) {
	w := newWageringEnv(t)
	wal := w.openWallet(t, "100.00")
	first := wagerOn(t, wal, "msg-1", nil)
	second := wagerOn(t, wal, "msg-2", func(d map[string]any) {
		d["externalTransactionId"] = "tx-msg-1"
		d["idempotencyKey"] = "provider-a:tx-msg-1"
	})
	w.sendAsProviderA(t, wal, "msg-1", first, "")
	w.sendAsProviderA(t, wal, "msg-2", second, "")

	c, m, _ := w.startConsumer(t, integrationConfig(), w.app, w.svc)
	awaitSettled(t, c, m, 2)

	requireResults(t, m, []string{"processed", "duplicate"})
	w.requireQueuesEmpty(t)
	tx := w.requireOneTransaction(t, wal.ID, storedTx{
		ExternalID: "tx-msg-1", Kind: "BET", Status: "PROCESSED", CorrelationID: "msg-1",
	})
	w.requireWallet(t, wal.ID, 7500, 2, "CREDIT 10000", "DEBIT 2500")
	w.requireInbox(t,
		completed("msg-1", inboxHashOf(t, first), tx.ID, "PROCESSED"),
		completed("msg-2", inboxHashOf(t, second), tx.ID, "REPLAY"),
	)
	w.requireEventsOf(t, tx.ID,
		storedEvent{event.TypeWagerTransactionProcessed, "msg-1", "msg-1"},
		storedEvent{event.TypeWalletBalanceChanged, "msg-1", "msg-1"},
	)
	w.requireCounted(t, decision{"BET", "PROCESSED", ""}, map[string]int{"idempotency": 1})
}

func TestConsumer_RejectionIsTerminal(t *testing.T) {
	w := newWageringEnv(t)
	wal := w.openWallet(t, "10.00")
	body := wagerOn(t, wal, "msg-1", nil)
	w.sendAsProviderA(t, wal, "msg-1", body, "")

	c, m, _ := w.startConsumer(t, integrationConfig(), w.app, w.svc)
	awaitSettled(t, c, m, 1)

	requireResults(t, m, []string{"processed"})
	w.requireQueuesEmpty(t)
	tx := w.requireOneTransaction(t, wal.ID, storedTx{
		ExternalID: "tx-msg-1", Kind: "BET", Status: "REJECTED",
		FailureCode: string(wager.FailureInsufficientFunds), CorrelationID: "msg-1",
	})
	w.requireWallet(t, wal.ID, 1000, 1, "CREDIT 1000")
	w.requireInbox(t, completed("msg-1", inboxHashOf(t, body), tx.ID, "REJECTED"))
	w.requireEventsOf(t, tx.ID, storedEvent{event.TypeWagerTransactionRejected, "msg-1", "msg-1"})
	w.requireCounted(t, decision{"BET", "REJECTED", string(wager.FailureInsufficientFunds)}, nil)
}

func TestConsumer_MessageIDReusedToDLQ(t *testing.T) {
	w := newWageringEnv(t)
	wal := w.openWallet(t, "100.00")
	opening := w.eventCount(t)
	first := wagerOn(t, wal, "msg-1", nil)
	reused := wagerOn(t, wal, "msg-1", func(d map[string]any) {
		d["externalTransactionId"] = "tx-other"
		d["idempotencyKey"] = "provider-a:tx-other"
		d["money"] = map[string]any{"amount": "30.00", "currency": "BRL"}
	})
	w.sendAsProviderA(t, wal, "msg-1", first, "")
	reusedID := w.sendAsProviderA(t, wal, "msg-1-reused", reused, "")

	c, m, _ := w.startConsumer(t, integrationConfig(), w.app, w.svc)
	awaitSettled(t, c, m, 2)

	requireResults(t, m, []string{"processed", "dlq"}, sqsin.ReasonMessageIDReused)
	w.requireCount(t, "sqs_messages_total", map[string]string{"result": "dlq"}, 1)
	w.requireCount(t, "sqs_dlq_total", map[string]string{"reason": sqsin.ReasonMessageIDReused}, 1)
	w.requireDeadLetter(t, reused, reusedID, sqsin.ReasonMessageIDReused, "")
	tx := w.requireOneTransaction(t, wal.ID, storedTx{
		ExternalID: "tx-msg-1", Kind: "BET", Status: "PROCESSED", CorrelationID: "msg-1",
	})
	w.requireWallet(t, wal.ID, 7500, 2, "CREDIT 10000", "DEBIT 2500")
	w.requireInbox(t, completed("msg-1", inboxHashOf(t, first), tx.ID, "PROCESSED"))
	w.requireEventsOf(t, tx.ID,
		storedEvent{event.TypeWagerTransactionProcessed, "msg-1", "msg-1"},
		storedEvent{event.TypeWalletBalanceChanged, "msg-1", "msg-1"},
	)
	if got, want := w.eventCount(t), opening+2; got != want {
		t.Errorf("the outbox holds %d events, want %d: those of the opening and the two of the first message", got, want)
	}
	w.requireCounted(t, decision{"BET", "PROCESSED", ""}, nil)
}

func TestConsumer_ProviderIdentityMismatchToDLQ(t *testing.T) {
	w := newWageringEnv(t)
	wal := w.openWallet(t, "100.00")
	providerB, err := w.ms.Client(t.Context(), "provider-b")
	if err != nil {
		t.Fatalf("provider-b client: %v", err)
	}
	tokenB, err := keycloak(t).Token(t.Context(), "provider-b", "provider-b-secret")
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	body := wagerOn(t, wal, "msg-1", nil)
	sqsID := w.send(t, providerB, outgoing{body: body, group: wal.ID.String(), dedup: "msg-1", token: tokenB})

	c, m, logs := w.startConsumer(t, integrationConfig(), w.app, w.svc)
	awaitSettled(t, c, m, 1)

	requireResults(t, m, []string{"dlq"}, sqsin.ReasonProviderIdentityMismatch)
	w.requireDeadLetter(t, body, sqsID, sqsin.ReasonProviderIdentityMismatch, "")
	w.requireNothingWritten(t, wal)
	security := slices.ContainsFunc(logs.lines(t), func(l map[string]any) bool {
		return l["level"] == "WARN" && l["security"] == true && l["providerId"] == "provider-b" && l["claimedProviderId"] == "provider-a"
	})
	if !security {
		t.Errorf("no security warning naming provider-b as the sender and provider-a as claimed in:\n%s", logs.String())
	}
}

func TestConsumer_OpeningKindToDLQ(t *testing.T) {
	w := newWageringEnv(t)
	wal := w.openWallet(t, "100.00")
	body := wagerOn(t, wal, "msg-1", func(d map[string]any) { d["kind"] = "OPENING" })
	sqsID := w.sendAsProviderA(t, wal, "msg-1", body, "")

	c, m, _ := w.startConsumer(t, integrationConfig(), w.app, w.svc)
	awaitSettled(t, c, m, 1)

	requireResults(t, m, []string{"dlq"}, sqsin.ReasonInvalidMessage)
	w.requireDeadLetter(t, body, sqsID, sqsin.ReasonInvalidMessage, string(wager.ValidationInvalidKind))
	w.requireNothingWritten(t, wal)
}

func TestConsumer_PendingReferenceCompletesMessage(t *testing.T) {
	w := newWageringEnv(t)
	wal := w.openWallet(t, "100.00")
	body := wagerOn(t, wal, "msg-1", func(d map[string]any) {
		d["kind"] = "REFUND"
		d["referenceExternalTransactionId"] = "tx-bet-not-yet-sent"
	})
	w.sendAsProviderA(t, wal, "msg-1", body, "")

	c, m, _ := w.startConsumer(t, integrationConfig(), w.app, w.svc)
	awaitSettled(t, c, m, 1)

	requireResults(t, m, []string{"processed"})
	w.requireQueuesEmpty(t)
	tx := w.requireOneTransaction(t, wal.ID, storedTx{
		ExternalID: "tx-msg-1", Kind: "REFUND", Status: "PENDING_REFERENCE", CorrelationID: "msg-1",
	})
	w.requireWallet(t, wal.ID, 10000, 1, "CREDIT 10000")
	w.requireInbox(t, completed("msg-1", inboxHashOf(t, body), tx.ID, "PENDING_REFERENCE"))
	w.requireEventsOf(t, tx.ID, storedEvent{event.TypeWagerTransactionPendingReference, "msg-1", "msg-1"})
	w.requireCounted(t, decision{"REFUND", "PENDING_REFERENCE", ""}, nil)
}

type holdFirst struct {
	sqsin.Submitter
	arrived, release chan struct{}
	calls            atomic.Int32
}

func newHoldFirst(sub sqsin.Submitter) *holdFirst {
	return &holdFirst{Submitter: sub, arrived: make(chan struct{}), release: make(chan struct{})}
}

func (h *holdFirst) Submit(ctx context.Context, cmd wager.Command, src wagering.Source) (wagering.Result, error) {
	if h.calls.Add(1) == 1 {
		close(h.arrived)
		select {
		case <-h.release:
		case <-ctx.Done():
		}
	}
	return h.Submitter.Submit(ctx, cmd, src)
}

func TestConsumer_TransientRetry(t *testing.T) {
	w := newWageringEnv(t)
	wal := w.openWallet(t, "100.00")
	body := wagerOn(t, wal, "msg-1", nil)
	w.sendAsProviderA(t, wal, "msg-1", body, "")
	cfg := integrationConfig()
	cfg.VisibilityTimeout = 10 * time.Second
	cfg.ProcessingTimeout = 6 * time.Second
	rec := &batchRecorder{SQSAPI: w.app}
	sub := newHoldFirst(w.svc)

	c, m, logs := w.startConsumer(t, cfg, rec, sub)
	paused := false
	t.Cleanup(func() {
		if paused {
			_ = w.pg.Unpause(context.Background())
		}
	})
	select {
	case <-sub.arrived:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the message to be submitted")
	}
	if err := w.pg.Pause(t.Context()); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	paused = true
	close(sub.release)

	waitWithin(t, 15*time.Second, "the circuit to open", func() bool { return strings.Contains(logs.String(), "polling paused") })
	time.Sleep(cfg.WaitTime + 500*time.Millisecond)
	receives := rec.receiveCalls()
	time.Sleep(4 * time.Second)
	if got := rec.receiveCalls(); got != receives {
		t.Errorf("%d receives while the database was down, want none", got-receives)
	}
	if got := m.results(); !slices.Equal(got, []string{"retry"}) {
		t.Errorf("results while the database was down = %v, want [retry]", got)
	}
	attrs, err := w.ms.RootClient().GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(w.ms.Queues.Input),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes: %v", err)
	}
	if n := attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]; n != "1" {
		t.Errorf("messages waiting in the input queue = %s, want the one that failed", n)
	}

	if err := w.pg.Unpause(t.Context()); err != nil {
		t.Fatalf("Unpause: %v", err)
	}
	paused = false
	awaitSettled(t, c, m, 2)

	requireResults(t, m, []string{"retry", "processed"})
	if got := rec.receiveCounts(t, "msg-1"); !slices.Equal(got, []string{"1", "2"}) {
		t.Errorf("receive counts of msg-1 = %v, want [1 2]: the outage must spend no receive", got)
	}
	if !strings.Contains(logs.String(), "polling resumed") {
		t.Errorf("no log of the polling resumed in:\n%s", logs.String())
	}
	if n := sub.calls.Load(); n != 2 {
		t.Errorf("submitted %d times, want 2", n)
	}
	w.requireQueuesEmpty(t)
	tx := w.requireOneTransaction(t, wal.ID, storedTx{
		ExternalID: "tx-msg-1", Kind: "BET", Status: "PROCESSED", CorrelationID: "msg-1",
	})
	w.requireWallet(t, wal.ID, 7500, 2, "CREDIT 10000", "DEBIT 2500")
	w.requireInbox(t, completed("msg-1", inboxHashOf(t, body), tx.ID, "PROCESSED"))
	w.requireCounted(t, decision{"BET", "PROCESSED", ""}, nil)
}

var errSevered = errors.New("the process that held this client is gone")

type severable struct {
	*sqs.Client
	severed atomic.Bool
}

func (s *severable) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if s.severed.Load() {
		return nil, errSevered
	}
	return s.Client.ReceiveMessage(ctx, in, optFns...)
}

func (s *severable) DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if s.severed.Load() {
		return nil, errSevered
	}
	return s.Client.DeleteMessage(ctx, in, optFns...)
}

func (s *severable) SendMessage(ctx context.Context, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if s.severed.Load() {
		return nil, errSevered
	}
	return s.Client.SendMessage(ctx, in, optFns...)
}

func (s *severable) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if s.severed.Load() {
		return nil, errSevered
	}
	return s.Client.ChangeMessageVisibility(ctx, in, optFns...)
}

func (s *severable) ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	if s.severed.Load() {
		return nil, errSevered
	}
	return s.Client.ChangeMessageVisibilityBatch(ctx, in, optFns...)
}

type crashAfterCommit struct {
	sqsin.Submitter
	conn  *severable
	calls atomic.Int32
}

func (c *crashAfterCommit) Submit(ctx context.Context, cmd wager.Command, src wagering.Source) (wagering.Result, error) {
	c.calls.Add(1)
	res, err := c.Submitter.Submit(ctx, cmd, src)
	if err == nil {
		c.conn.severed.Store(true)
	}
	return res, err
}

func TestConsumer_CrashBetweenCommitAndDelete(t *testing.T) {
	w := newWageringEnv(t)
	wal := w.openWallet(t, "100.00")
	body := wagerOn(t, wal, "msg-1", nil)
	sqsID := w.sendAsProviderA(t, wal, "msg-1", body, "")
	cfg := integrationConfig()
	cfg.VisibilityTimeout = 4 * time.Second
	cfg.ProcessingTimeout = 3 * time.Second
	conn := &severable{Client: w.app}
	crash := &crashAfterCommit{Submitter: w.svc, conn: conn}

	dead, deadMetrics, _ := w.startConsumerMoving(t, cfg, conn, conn, crash)
	awaitSettled(t, dead, deadMetrics, 1)
	if got := deadMetrics.results(); !slices.Equal(got, []string{"retry"}) {
		t.Fatalf("results of the consumer that died = %v, want [retry]: its delete was cut off", got)
	}

	rec := &batchRecorder{SQSAPI: w.app}
	fresh, freshMetrics, _ := w.startConsumer(t, cfg, rec, w.svc)
	awaitSettled(t, fresh, freshMetrics, 1)

	requireResults(t, freshMetrics, []string{"duplicate"})
	if got, want := rec.deliveries(), []string{sqsID + " receive 2"}; !slices.Equal(got, want) {
		t.Errorf("the fresh consumer received %v, want %v: the message the other one held", got, want)
	}
	if n := crash.calls.Load(); n != 1 {
		t.Errorf("the consumer that died submitted %d times, want 1", n)
	}
	w.requireQueuesEmpty(t)
	tx := w.requireOneTransaction(t, wal.ID, storedTx{
		ExternalID: "tx-msg-1", Kind: "BET", Status: "PROCESSED", CorrelationID: "msg-1",
	})
	w.requireWallet(t, wal.ID, 7500, 2, "CREDIT 10000", "DEBIT 2500")
	w.requireInbox(t, completed("msg-1", inboxHashOf(t, body), tx.ID, "PROCESSED"))
	w.requireEventsOf(t, tx.ID,
		storedEvent{event.TypeWagerTransactionProcessed, "msg-1", "msg-1"},
		storedEvent{event.TypeWalletBalanceChanged, "msg-1", "msg-1"},
	)
	w.requireCounted(t, decision{"BET", "PROCESSED", ""}, map[string]int{"inbox": 1})
}
