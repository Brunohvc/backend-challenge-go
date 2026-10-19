//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/failpoint"
)

var (
	apiOnly      = labels("CONSUMER_ENABLED", "false", "OUTBOX_RELAY_ENABLED", "false", "RESOLVER_ENABLED", "false")
	consumerOnly = labels("HTTP_ENABLED", "false", "OUTBOX_RELAY_ENABLED", "false", "RESOLVER_ENABLED", "false")
	relayOnly    = labels("HTTP_ENABLED", "false", "CONSUMER_ENABLED", "false", "RESOLVER_ENABLED", "false")
	resolverOnly = labels("HTTP_ENABLED", "false", "CONSUMER_ENABLED", "false", "OUTBOX_RELAY_ENABLED", "false")
)

const (
	killedStatus  = 137
	crashTimeout  = 30 * time.Second
	referenceTTL  = 3 * time.Second
	leaseTakeover = 3 * time.Second
)

func with(env map[string]string, pairs ...string) map[string]string {
	out := maps.Clone(env)
	maps.Copy(out, labels(pairs...))
	return out
}

func armed(env map[string]string, name string) map[string]string {
	return with(env, "FAILPOINTS", name+"=exit")
}

func requireCrash(t *testing.T, inst *Instance) {
	t.Helper()
	if code := inst.WaitExit(t, crashTimeout); code != killedStatus {
		t.Fatalf("%s exited with status %d, want %d from its failpoint", inst.Name, code, killedStatus)
	}
}

func requireLostAnswer(t *testing.T, c *ProviderClient, inst *Instance, body TxBody, key string) {
	t.Helper()
	status, resp, err := c.TryPostTx(t.Context(), inst.URL, body, key)
	if err == nil {
		t.Fatalf("%s answered %d %+v, want the connection to go down with the process", inst.Name, status, resp)
	}
	requireCrash(t, inst)
	t.Logf("the submission to %s got no answer: %v", inst.Name, err)
}

func TestE2E_ConsumerKilledAfterCommit(t *testing.T) {
	ctx := t.Context()
	api := StartInstance(t, "api", apiOnly)
	crashing := StartInstance(t, "consumer-1", armed(consumerOnly, failpoint.ConsumerAfterCommitBeforeDelete))
	provider := NewProviderClient(t, "provider-a")
	w := OpenWallet(t, api, "100.00")
	bet := Bet(w, uniq("bet"), "25.00")
	msgID := uniq("msg")
	provider.SendTx(ctx, bet, keyOf(bet), msgID)
	requireCrash(t, crashing)

	status, tx := GetTx(t, api, "provider-a", bet.ExternalTransactionID)
	if status != http.StatusOK || tx.Status != wager.StatusProcessed {
		t.Fatalf("the bet after the crash: %d %+v, want PROCESSED, committed before it", status, tx)
	}
	requireBalance(t, api, w.ID, "75.00", 2)
	if n := queueDepth(t, suite.env.AWS.Queues.Input); n != 1 {
		t.Fatalf("the input queue holds %d messages after the crash, want the one never deleted", n)
	}
	requireInbox(t, msgID, tx.TransactionID)

	consumer := StartInstance(t, "consumer-2", consumerOnly)
	began := time.Now()
	awaitMetric(t, []*Instance{consumer}, "wagering_duplicates_total", labels("channel", "sqs", "source", "inbox"), 1)
	AssertQueuesDrained(t)
	t.Logf("the message came back to %s and was recognized %s after it started", consumer.Name, time.Since(began).Round(time.Millisecond))
	requireMetric(t, []*Instance{consumer}, "sqs_messages_total", labels("result", "duplicate"), 1)
	requireMetric(t, []*Instance{consumer}, "wagering_transactions_total", nil, 0)
	requireInbox(t, msgID, tx.TransactionID)
	requireBalance(t, api, w.ID, "75.00", 2)
	requireDebits(t, api, w.ID, tx.TransactionID)
	AssertReconciled(t, api, w.ID)
}

func TestE2E_ConsumerKilledBeforeCommit(t *testing.T) {
	ctx := t.Context()
	api := StartInstance(t, "api", apiOnly)
	crashing := StartInstance(t, "consumer-1", armed(consumerOnly, failpoint.SubmitBeforeCommit))
	provider := NewProviderClient(t, "provider-a")
	w := OpenWallet(t, api, "100.00")
	bet := Bet(w, uniq("bet"), "25.00")
	msgID := uniq("msg")
	provider.SendTx(ctx, bet, keyOf(bet), msgID)
	requireCrash(t, crashing)

	if status, tx := GetTx(t, api, "provider-a", bet.ExternalTransactionID); status != http.StatusNotFound {
		t.Fatalf("the bet whose message crashed before its commit: %d %+v, want 404", status, tx)
	}
	requireBalance(t, api, w.ID, "100.00", 1)
	if n := inboxRowsOf(t, msgID); n != 0 {
		t.Fatalf("the inbox holds %d rows for message %s after the crash, want none: its claim was never committed", n, msgID)
	}
	if n := queueDepth(t, suite.env.AWS.Queues.Input); n != 1 {
		t.Fatalf("the input queue holds %d messages after the crash, want the one never deleted", n)
	}

	consumer := StartInstance(t, "consumer-2", consumerOnly)
	began := time.Now()
	tx := WaitForStatus(t, api, "provider-a", bet.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
	AssertQueuesDrained(t)
	t.Logf("the message came back to %s and was decided %s after it started", consumer.Name, time.Since(began).Round(time.Millisecond))
	requireMetric(t, []*Instance{consumer}, "sqs_messages_total", labels("result", "processed"), 1)
	requireMetric(t, []*Instance{consumer}, "wagering_transactions_total", nil, 1)
	requireMetric(t, []*Instance{consumer}, "wagering_duplicates_total", nil, 0)
	requireInbox(t, msgID, tx.TransactionID)
	requireBalance(t, api, w.ID, "75.00", 2)
	requireDebits(t, api, w.ID, tx.TransactionID)
	AssertReconciled(t, api, w.ID)
}

func TestE2E_SubmitKilledBeforeCommit(t *testing.T) {
	ctx := t.Context()
	api := StartInstance(t, "api-1", apiOnly)
	crashing := StartInstance(t, "api-2", armed(apiOnly, failpoint.SubmitBeforeCommit))
	provider := NewProviderClient(t, "provider-a")
	w := OpenWallet(t, api, "100.00")
	bet := Bet(w, uniq("bet"), "10.00")
	key := keyOf(bet)
	requireLostAnswer(t, provider, crashing, bet, key)

	if status, tx := GetTx(t, api, "provider-a", bet.ExternalTransactionID); status != http.StatusNotFound {
		t.Fatalf("the bet whose submission crashed before its commit: %d %+v, want 404", status, tx)
	}
	requireBalance(t, api, w.ID, "100.00", 1)

	status, first := provider.PostTx(ctx, api, bet, key)
	if status != http.StatusCreated || first.IdempotentReplay || first.Status != wager.StatusProcessed || first.Balance == nil || *first.Balance != BRL("90.00") {
		t.Fatalf("the same bet through %s: %d %+v, want 201 PROCESSED decided now, balance 90.00", api.Name, status, first)
	}
	status, again := provider.PostTx(ctx, api, bet, key)
	if status != http.StatusOK || !again.IdempotentReplay || again.TransactionID != first.TransactionID {
		t.Errorf("the bet once more: %d %+v, want a 200 replay of %s", status, again, first.TransactionID)
	}
	requireMetric(t, []*Instance{api}, "wagering_transactions_total", nil, 1)
	requireBalance(t, api, w.ID, "90.00", 2)
	requireDebits(t, api, w.ID, first.TransactionID)
	AssertReconciled(t, api, w.ID)
}

func TestE2E_SubmitKilledAfterCommit(t *testing.T) {
	ctx := t.Context()
	api := StartInstance(t, "api-1", apiOnly)
	crashing := StartInstance(t, "api-2", armed(apiOnly, failpoint.SubmitAfterCommit))
	provider := NewProviderClient(t, "provider-a")
	w := OpenWallet(t, api, "100.00")
	bet := Bet(w, uniq("bet"), "10.00")
	key := keyOf(bet)
	requireLostAnswer(t, provider, crashing, bet, key)

	status, committed := GetTx(t, api, "provider-a", bet.ExternalTransactionID)
	if status != http.StatusOK || committed.Status != wager.StatusProcessed {
		t.Fatalf("the bet whose submission crashed after its commit: %d %+v, want PROCESSED", status, committed)
	}
	requireBalance(t, api, w.ID, "90.00", 2)
	next := Bet(w, uniq("next"), "5.00")
	if status, resp := provider.PostTx(ctx, api, next, keyOf(next)); status != http.StatusCreated {
		t.Fatalf("the next bet: %d %+v, want 201", status, resp)
	}

	status, again := provider.PostTx(ctx, api, bet, key)
	if status != http.StatusOK || !again.IdempotentReplay || again.TransactionID != committed.TransactionID ||
		again.Status != wager.StatusProcessed || again.Balance == nil || *again.Balance != BRL("90.00") {
		t.Errorf("the same bet through %s: %d %+v, want a 200 replay of %s with the balance it left, 90.00",
			api.Name, status, again, committed.TransactionID)
	}
	requireMetric(t, []*Instance{api}, "wagering_duplicates_total", labels("channel", "http", "source", "idempotency"), 1)
	requireBalance(t, api, w.ID, "85.00", 3)
	if debits := debitsOf(Ledger(t, api, w.ID)); len(debits) != 2 || debits[0].TransactionID != committed.TransactionID {
		t.Errorf("the ledger debits %+v, want the crashed bet once, then the next one", debits)
	}
	AssertReconciled(t, api, w.ID)
}

func TestE2E_OutboxCrashAfterClaim(t *testing.T) {
	outboxCrash(t, failpoint.OutboxAfterClaim)
}

func TestE2E_OutboxCrashAfterPublishBeforeAck(t *testing.T) {
	outboxCrash(t, failpoint.OutboxAfterPublishBeforeAck)
}

func outboxCrash(t *testing.T, crashAt string) {
	ctx := t.Context()
	api := StartInstance(t, "api", apiOnly)
	provider := NewProviderClient(t, "provider-a")
	const wallets, betsPerWallet = 3, 2
	ids := make([]uuid.UUID, wallets)
	for i := range ids {
		w := OpenWallet(t, api, "100.00")
		ids[i] = w.ID
		for range betsPerWallet {
			bet := Bet(w, uniq("bet"), "10.00")
			if status, resp := provider.PostTx(ctx, api, bet, keyOf(bet)); status != http.StatusCreated {
				t.Fatalf("bet on %s: %d %+v, want 201", w.ID, status, resp)
			}
		}
	}
	rows := outboxOf(t, ids...)
	if want := wallets * (1 + betsPerWallet) * 2; len(rows) != want {
		t.Fatalf("the outbox holds %d events of the wallets, want %d", len(rows), want)
	}
	heads := headsOf(rows)

	leases := with(relayOnly, "OUTBOX_LEASE", leaseTakeover.String())
	crashing := Launch(t, "relay-1", with(armed(leases, crashAt), "OUTBOX_BATCH_SIZE", "500"))
	requireCrash(t, crashing)
	var leaseEnd time.Time
	for _, r := range outboxOf(t, ids...) {
		switch {
		case r.PublishedAt != nil:
			t.Errorf("event %s %s is marked published, by a relay that crashed before acknowledging anything", r.ID, r.EventType)
		case heads[r.ID] && (r.Attempts != 1 || r.LockedUntil == nil):
			t.Errorf("event %s %s, the oldest of its wallet: %d claims, leased until %v, want claimed once and leased", r.ID, r.EventType, r.Attempts, r.LockedUntil)
		case !heads[r.ID] && r.Attempts != 0:
			t.Errorf("event %s %s was claimed %d times before the one ahead of it was published", r.ID, r.EventType, r.Attempts)
		case heads[r.ID] && r.LockedUntil.After(leaseEnd):
			leaseEnd = *r.LockedUntil
		}
	}
	events := newEventReader(t, ids...)
	if crashAt == failpoint.OutboxAfterPublishBeforeAck {
		events.await(t, "the events the crashed relay published", func(got []eventCopy) bool {
			return receivedAll(got, heads)
		})
	}

	relay := StartInstance(t, "relay-2", leases)
	rows = awaitPublished(t, ids...)
	for _, r := range rows {
		want := 1
		if heads[r.ID] {
			want = 2
		}
		if r.Attempts != want {
			t.Errorf("event %s %s was claimed %d times, want %d", r.ID, r.EventType, r.Attempts, want)
		}
		if heads[r.ID] && r.PublishedAt.Before(leaseEnd) {
			t.Errorf("event %s %s was published at %s, before the lease of the crashed relay ran out at %s", r.ID, r.EventType, r.PublishedAt, leaseEnd)
		}
	}
	requireMetric(t, []*Instance{relay}, "outbox_publish_total", labels("result", "lease_lost"), 0)
	copies := events.awaitRows(t, rows)
	t.Logf("%d events published, %d of them claimed by the crashed relay under a lease until %s; %d copies received",
		len(rows), len(heads), leaseEnd.Format(time.RFC3339Nano), len(copies))
	AssertReconciled(t, api, ids...)
}

func TestE2E_PendingReferenceResolvedAndExpired(t *testing.T) {
	ctx := t.Context()
	cluster := StartCluster(t, instances, labels("REFERENCE_TTL", referenceTTL.String()))
	provider := NewProviderClient(t, "provider-a")

	w := OpenWallet(t, cluster[0], "100.00")
	bet := Bet(w, uniq("late-bet"), "10.00")
	rollback := reversal(bet, wager.KindRollback)
	provider.SendTx(ctx, rollback, keyOf(rollback), uniq("msg-rollback"))
	WaitForStatus(t, cluster[1], "provider-a", rollback.ExternalTransactionID, wager.StatusPendingReference, settleTimeout)
	status, placed := provider.PostTx(ctx, cluster[2], bet, keyOf(bet))
	if status != http.StatusCreated || placed.Status != wager.StatusProcessed {
		t.Fatalf("the bet: %d %+v, want 201 PROCESSED", status, placed)
	}
	resolved := WaitForStatus(t, cluster[0], "provider-a", rollback.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
	if resolved.ReferenceTransactionID == nil || *resolved.ReferenceTransactionID != placed.TransactionID {
		t.Errorf("the rollback refers to %v, want the bet %s", resolved.ReferenceTransactionID, placed.TransactionID)
	}
	requireBalance(t, cluster[1], w.ID, "100.00", 3)
	ledger := Ledger(t, cluster[2], w.ID)
	if len(ledger) != 3 || ledger[1].TransactionID != placed.TransactionID || ledger[1].Direction != "DEBIT" ||
		ledger[2].TransactionID != resolved.TransactionID || ledger[2].Direction != "CREDIT" {
		t.Errorf("the ledger is %+v, want the opening, the bet's debit and then the rollback's credit", ledger)
	}
	AssertQueuesDrained(t)

	lone := OpenWallet(t, cluster[1], "100.00")
	orphan := reversal(Bet(lone, uniq("missing-bet"), "10.00"), wager.KindRollback)
	began := time.Now()
	status, waiting := provider.PostTx(ctx, cluster[0], orphan, keyOf(orphan))
	if status != http.StatusAccepted || waiting.Status != wager.StatusPendingReference {
		t.Fatalf("the rollback of a bet never sent: %d %+v, want 202 PENDING_REFERENCE", status, waiting)
	}
	rejected := WaitForStatus(t, cluster[2], "provider-a", orphan.ExternalTransactionID, wager.StatusRejected, settleTimeout)
	if waited := time.Since(began); rejected.FailureCode != wager.FailureReferenceNotFound || waited < referenceTTL {
		t.Errorf("the rollback was rejected %s after %s, want %s once its %s wait ran out", rejected.FailureCode, waited, wager.FailureReferenceNotFound, referenceTTL)
	}
	requireBalance(t, cluster[0], lone.ID, "100.00", 1)
	rows := awaitPublished(t, lone.ID)
	copies := newEventReader(t, lone.ID).awaitRows(t, rows)
	n := len(rows)
	if n < 2 || rows[n-2].EventType != event.TypeWagerTransactionPendingReference || rows[n-1].EventType != event.TypeWagerTransactionRejected {
		t.Fatalf("the events of the wallet are %v, want the wait for the reference and then the rejection last", eventTypes(rows))
	}
	var data event.WagerTransactionRejectedData
	if err := json.Unmarshal(lastCopyOf(copies, rows[n-1].ID).Envelope.Data, &data); err != nil {
		t.Fatalf("decode the rejection: %v", err)
	}
	if data.TransactionID != rejected.TransactionID.String() || data.FailureCode != string(wager.FailureReferenceNotFound) {
		t.Errorf("the rejection published is %+v, want transaction %s rejected %s", data, rejected.TransactionID, wager.FailureReferenceNotFound)
	}

	requireMetric(t, cluster, "reference_resolutions_total", labels("result", "processed"), 1)
	requireMetric(t, cluster, "reference_resolutions_total", labels("result", "rejected"), 1)
	AssertReconciled(t, cluster[0], w.ID, lone.ID)
}

func TestE2E_ResolverKilledAfterClaim(t *testing.T) {
	ctx := t.Context()
	api := StartInstance(t, "api", with(apiOnly, "REFERENCE_TTL", "1m"))
	provider := NewProviderClient(t, "provider-a")
	w := OpenWallet(t, api, "100.00")
	bet := Bet(w, uniq("bet"), "10.00")
	rollback := reversal(bet, wager.KindRollback)
	if status, resp := provider.PostTx(ctx, api, rollback, keyOf(rollback)); status != http.StatusAccepted {
		t.Fatalf("the rollback before its bet: %d %+v, want 202", status, resp)
	}
	if status, resp := provider.PostTx(ctx, api, bet, keyOf(bet)); status != http.StatusCreated {
		t.Fatalf("the bet: %d %+v, want 201", status, resp)
	}
	_, pending := GetTx(t, api, "provider-a", rollback.ExternalTransactionID)

	leases := with(resolverOnly, "RESOLVER_LEASE", leaseTakeover.String(), "REFERENCE_TTL", "1m")
	crashing := Launch(t, "resolver-1", with(armed(leases, failpoint.ResolverAfterClaim), "RESOLVER_BATCH_SIZE", "500"))
	requireCrash(t, crashing)
	lease := leaseOf(t, pending.TransactionID)
	if !lease.Held {
		t.Fatalf("the rollback is not leased after the crash of the resolver that claimed it: its lease ended at %s", lease.End)
	}
	if status, tx := GetTx(t, api, "provider-a", rollback.ExternalTransactionID); status != http.StatusOK || tx.Status != wager.StatusPendingReference {
		t.Fatalf("the rollback after the crash: %d %+v, want it still PENDING_REFERENCE", status, tx)
	}

	resolver := StartInstance(t, "resolver-2", leases)
	for {
		status, leased := statusWithin(t, pending.TransactionID, lease.End)
		if !leased {
			break
		}
		if status != wager.StatusPendingReference {
			t.Fatalf("the rollback is %s while the lease of the crashed resolver runs until %s, want PENDING_REFERENCE", status, lease.End)
		}
		time.Sleep(pollEvery)
	}
	resumed := WaitForStatus(t, api, "provider-a", rollback.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
	resumedAt := processedEventAt(t, pending.TransactionID)
	if resumedAt.Before(lease.End) {
		t.Errorf("the rollback was resumed at %s, before the lease of the crashed resolver ran out at %s", resumedAt, lease.End)
	}
	t.Logf("%s resumed the rollback %s after the lease of the crashed resolver ran out, by the clock of the database",
		resolver.Name, resumedAt.Sub(lease.End).Round(time.Millisecond))
	if resumed.TransactionID != pending.TransactionID {
		t.Errorf("the resumed rollback is %s, want %s", resumed.TransactionID, pending.TransactionID)
	}
	requireMetric(t, []*Instance{resolver}, "reference_resolutions_total", labels("result", "processed"), 1)
	requireBalance(t, api, w.ID, "100.00", 3)
	AssertReconciled(t, api, w.ID)
}

func TestE2E_RestartPreservesEverything(t *testing.T) {
	ctx := t.Context()
	settings := labels("REFERENCE_TTL", "2m", "RESOLVER_LEASE", leaseTakeover.String(), "OUTBOX_LEASE", leaseTakeover.String())
	cluster := StartCluster(t, instances, settings)
	provider := NewProviderClient(t, "provider-a")
	const wallets, inFlightPerWallet = 4, 6
	ws := make([]Wallet, wallets)
	ids := make([]uuid.UUID, wallets)
	for i := range ws {
		ws[i] = OpenWallet(t, cluster[i%len(cluster)], "1000.00")
		ids[i] = ws[i].ID
	}

	type decided struct {
		bet   TxBody
		first TxResponse
	}
	var answered []decided
	var queued, lateBets, waiting []TxBody
	for i, w := range ws {
		bet := Bet(w, uniq(fmt.Sprintf("w%d-http", i)), "10.00")
		status, first := provider.PostTx(ctx, cluster[i%len(cluster)], bet, keyOf(bet))
		if status != http.StatusCreated {
			t.Fatalf("bet %s: %d %+v, want 201", bet.ExternalTransactionID, status, first)
		}
		answered = append(answered, decided{bet, first})
		viaQueue := Bet(w, uniq(fmt.Sprintf("w%d-sqs", i)), "10.00")
		provider.SendTx(ctx, viaQueue, keyOf(viaQueue), uniq("msg"))
		queued = append(queued, viaQueue)

		lateRefunded, lateRolledBack := Bet(w, uniq(fmt.Sprintf("w%d-refunded", i)), "20.00"), Bet(w, uniq(fmt.Sprintf("w%d-rolled-back", i)), "30.00")
		refund, rollback := reversal(lateRefunded, wager.KindRefund), reversal(lateRolledBack, wager.KindRollback)
		if status, resp := provider.PostTx(ctx, cluster[(i+1)%len(cluster)], refund, keyOf(refund)); status != http.StatusAccepted {
			t.Fatalf("refund %s before its bet: %d %+v, want 202", refund.ExternalTransactionID, status, resp)
		}
		provider.SendTx(ctx, rollback, keyOf(rollback), uniq("msg"))
		lateBets = append(lateBets, lateRefunded, lateRolledBack)
		waiting = append(waiting, refund, rollback)
	}
	for i, b := range queued {
		WaitForStatus(t, cluster[i%len(cluster)], "provider-a", b.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
	}
	for i, b := range waiting {
		WaitForStatus(t, cluster[i%len(cluster)], "provider-a", b.ExternalTransactionID, wager.StatusPendingReference, settleTimeout)
	}

	var overHTTP, throughQueue []TxBody
	for i := range wallets * inFlightPerWallet {
		bet := Bet(ws[i%wallets], uniq(fmt.Sprintf("w%d-flight%d", i%wallets, i)), "1.00")
		if i%2 == 0 {
			overHTTP = append(overHTTP, bet)
		} else {
			throughQueue = append(throughQueue, bet)
		}
	}
	type flight struct {
		status int
		resp   TxResponse
		err    error
	}
	flights := make([]flight, len(overHTTP))
	halfway, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for j, bet := range overHTTP {
			if j == len(overHTTP)/2 {
				close(halfway)
			}
			f := &flights[j]
			f.status, f.resp, f.err = provider.TryPostTx(ctx, cluster[j%len(cluster)].URL, bet, keyOf(bet))
		}
	}()
	for _, bet := range throughQueue {
		provider.SendTx(ctx, bet, keyOf(bet), uniq("msg"))
	}
	<-halfway
	var wg sync.WaitGroup
	for _, inst := range cluster {
		wg.Go(inst.Kill)
	}
	wg.Wait()
	<-done
	lost := 0
	for _, f := range flights {
		if f.err != nil {
			lost++
		}
	}
	queuedExts := make([]string, len(throughQueue))
	for i, b := range throughQueue {
		queuedExts[i] = b.ExternalTransactionID
	}
	decidedBefore := query(t, pgx.RowTo[int64],
		`SELECT count(*) FROM wager_transactions WHERE provider_id = 'provider-a' AND external_transaction_id = ANY ($1)`, queuedExts)
	t.Logf("killed every instance: %d of %d requests in flight got no answer, %d of %d messages were decided",
		lost, len(flights), decidedBefore[0], len(throughQueue))

	fresh := StartCluster(t, instances, settings)

	for i, d := range answered {
		status, again := provider.PostTx(ctx, fresh[i%len(fresh)], d.bet, keyOf(d.bet))
		if status != http.StatusOK || !again.IdempotentReplay || again.TransactionID != d.first.TransactionID ||
			again.Balance == nil || d.first.Balance == nil || *again.Balance != *d.first.Balance {
			t.Errorf("bet %s after the restart: %d %+v, want a 200 replay of %+v", d.bet.ExternalTransactionID, status, again, d.first)
		}
	}
	for _, b := range queued {
		provider.SendTx(ctx, b, keyOf(b), uniq("msg-again"))
	}
	for i, b := range lateBets {
		if status, resp := provider.PostTx(ctx, fresh[i%len(fresh)], b, keyOf(b)); status != http.StatusCreated {
			t.Errorf("late bet %s: %d %+v, want 201", b.ExternalTransactionID, status, resp)
		}
	}
	for i, b := range waiting {
		WaitForStatus(t, fresh[i%len(fresh)], "provider-a", b.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
	}
	for j, bet := range overHTTP {
		status, resp := provider.PostTx(ctx, fresh[j%len(fresh)], bet, keyOf(bet))
		f := flights[j]
		switch {
		case f.err == nil && (f.status != http.StatusCreated || status != http.StatusOK || resp.TransactionID != f.resp.TransactionID):
			t.Errorf("bet %s answered %d %+v before the kill and %d %+v after, want 201 then a 200 replay of it",
				bet.ExternalTransactionID, f.status, f.resp, status, resp)
		case f.err != nil && status != http.StatusCreated && status != http.StatusOK:
			t.Errorf("bet %s without an answer before the kill: %d %+v after, want 201, or 200 if it was committed", bet.ExternalTransactionID, status, resp)
		case resp.Status != wager.StatusProcessed:
			t.Errorf("bet %s after the restart: %+v, want PROCESSED", bet.ExternalTransactionID, resp)
		}
	}
	for i, b := range throughQueue {
		WaitForStatus(t, fresh[i%len(fresh)], "provider-a", b.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
	}
	awaitMetric(t, fresh, "wagering_duplicates_total", labels("channel", "sqs", "source", "idempotency"), wallets)
	AssertQueuesDrained(t)

	want := fmt.Sprintf("%d.00", 1000-10-10-inFlightPerWallet)
	for i, w := range ws {
		if got := GetWallet(t, fresh[i%len(fresh)], w.ID); got.Balance != BRL(want) {
			t.Errorf("wallet %s: balance %s, want %s", w.ID, got.Balance, want)
		}
		if debits := debitsOf(Ledger(t, fresh[(i+1)%len(fresh)], w.ID)); len(debits) != 4+inFlightPerWallet {
			t.Errorf("wallet %s has %d debits, want %d", w.ID, len(debits), 4+inFlightPerWallet)
		}
	}
	awaitPublished(t, ids...)
	AssertReconciled(t, fresh[0], ids...)
}

const (
	outage         = 10 * time.Second
	requestTimeout = 6 * time.Second
	loadInterval   = 50 * time.Millisecond
)

func TestE2E_PostgresAndSQSOutage(t *testing.T) {
	cluster := StartCluster(t, instances, labels(
		"HTTP_REQUEST_TIMEOUT", requestTimeout.String(),
		"OUTBOX_LEASE", "6s",
	))

	t.Run("postgres paused", func(t *testing.T) {
		ctx := t.Context()
		provider := NewProviderClient(t, "provider-a")
		ws, ids := openWallets(t, cluster, instances, "1000.00")
		load := startLoad(t, provider, urlsOf(cluster), ws, loadInterval)
		load.awaitAnswers(t, 10, http.StatusCreated)

		paused := pauseFor(t, "the database", suite.env.PG.Pause, suite.env.PG.Unpause)
		eventually(t, outage/2, "the readiness of an instance to report the outage", func() (bool, string) {
			r, err := call(ctx, http.MethodGet, cluster[0].URL+"/health/ready", "", nil, nil)
			return err == nil && r.status == http.StatusServiceUnavailable, fmt.Sprintf("%d %v", r.status, err)
		})
		load.awaitAnswers(t, 1, http.StatusServiceUnavailable)
		resumed := paused.end(t)
		load.awaitAnswers(t, 10, http.StatusCreated)
		posted, queued := load.results()

		refused := 0
		for _, s := range posted {
			switch {
			case s.err != nil:
				t.Errorf("bet %s got no answer: %v", s.bet.ExternalTransactionID, s.err)
			case s.status == http.StatusCreated && s.resp.Status == wager.StatusProcessed:
			case s.status == http.StatusServiceUnavailable && s.resp.Code == "TEMPORARILY_UNAVAILABLE" && s.resp.RetryAfter == "2":
				refused++
				if s.answered.Before(paused.at) || s.sent.After(resumed) {
					t.Errorf("bet %s sent at %s was answered 503 at %s, outside the outage from %s to %s",
						s.bet.ExternalTransactionID, s.sent.Format(time.StampMilli), s.answered.Format(time.StampMilli),
						paused.at.Format(time.StampMilli), resumed.Format(time.StampMilli))
				}
			default:
				t.Errorf("bet %s answered %d %+v, want 201 PROCESSED, or 503 TEMPORARILY_UNAVAILABLE with Retry-After 2",
					s.bet.ExternalTransactionID, s.status, s.resp)
			}
		}
		if refused == 0 {
			t.Fatalf("no request was answered 503 while the database was paused, of %d", len(posted))
		}
		for i, s := range posted {
			if s.status != http.StatusServiceUnavailable {
				continue
			}
			if status, resp := provider.PostTx(ctx, cluster[i%len(cluster)], s.bet, keyOf(s.bet)); status != http.StatusCreated && status != http.StatusOK {
				t.Errorf("bet %s sent again after the outage: %d %+v, want 201, or a 200 replay", s.bet.ExternalTransactionID, status, resp)
			}
		}
		for i, b := range queued {
			WaitForStatus(t, cluster[i%len(cluster)], "provider-a", b.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
		}
		AssertQueuesDrained(t)
		t.Logf("%d bets over HTTP, %d of them refused 503 during the outage and sent again; %d through the queue, every one processed",
			len(posted), refused, len(queued))
		requireSpent(t, cluster[0], ws, append(betsOf(posted), queued...))
		AssertReconciled(t, cluster[1], ids...)
	})

	t.Run("MiniStack paused", func(t *testing.T) {
		provider := NewProviderClient(t, "provider-a")
		ws, ids := openWallets(t, cluster, instances, "1000.00")
		load := startLoad(t, provider, urlsOf(cluster), ws, loadInterval)
		load.awaitAnswers(t, 10, http.StatusCreated)

		pausedAt := dbNow(t)
		paused := pauseFor(t, "MiniStack", suite.env.AWS.Pause, suite.env.AWS.Unpause)
		load.awaitAnswers(t, 10, http.StatusCreated)
		time.Sleep(time.Until(paused.at.Add(outage)))
		resumedAt := dbNow(t)
		paused.end(t)
		load.awaitAnswers(t, 10, http.StatusCreated)
		posted, queued := load.results()

		for _, s := range posted {
			if s.err != nil || s.status != http.StatusCreated || s.resp.Status != wager.StatusProcessed {
				t.Errorf("bet %s: %d %+v %v, want 201 PROCESSED, the API needing no queue", s.bet.ExternalTransactionID, s.status, s.resp, s.err)
			}
		}
		for i, b := range queued {
			WaitForStatus(t, cluster[i%len(cluster)], "provider-a", b.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
		}
		AssertQueuesDrained(t)

		rows := awaitPublished(t, ids...)
		heldBack := 0
		for _, r := range rows {
			if r.CreatedAt.Before(pausedAt) || r.CreatedAt.After(resumedAt) {
				continue
			}
			heldBack++
			if r.PublishedAt.Before(resumedAt) {
				t.Errorf("event %s %s was published at %s, while MiniStack was paused, until %s", r.ID, r.EventType, r.PublishedAt, resumedAt)
			}
		}
		if heldBack == 0 {
			t.Errorf("no event of the wallets was written while MiniStack was paused, from %s to %s", pausedAt, resumedAt)
		}
		failed := MetricSum(t, cluster, "outbox_publish_total", labels("result", "failed"))
		if failed == 0 {
			t.Error("no relay gave up on publishing while MiniStack was paused")
		}
		copies := newEventReader(t, ids...).awaitRows(t, rows)
		t.Logf("%d bets over HTTP and %d through the queue; %d events written during the outage, %d failed publications retried, %d events received in %d copies",
			len(posted), len(queued), heldBack, failed, len(rows), len(copies))
		requireSpent(t, cluster[0], ws, append(betsOf(posted), queued...))
		AssertReconciled(t, cluster[1], ids...)
	})
}

type pausedDependency struct {
	at      time.Time
	unpause func(context.Context) error
	once    sync.Once
}

func pauseFor(t *testing.T, what string, pause, unpause func(context.Context) error) *pausedDependency {
	t.Helper()
	if err := pause(t.Context()); err != nil {
		t.Fatalf("pause %s: %v", what, err)
	}
	p := &pausedDependency{at: time.Now(), unpause: unpause}
	t.Cleanup(func() {
		p.once.Do(func() {
			if err := unpause(context.WithoutCancel(t.Context())); err != nil {
				t.Errorf("unpause %s: %v", what, err)
			}
		})
	})
	t.Logf("paused %s", what)
	return p
}

func (p *pausedDependency) end(t *testing.T) time.Time {
	t.Helper()
	time.Sleep(time.Until(p.at.Add(outage)))
	var err error
	p.once.Do(func() { err = p.unpause(t.Context()) })
	if err != nil {
		t.Fatalf("unpause: %v", err)
	}
	return time.Now()
}

func keyOf(b TxBody) string {
	return "key-" + b.ExternalTransactionID
}

func reversal(bet TxBody, kind wager.Kind) TxBody {
	r := bet
	r.ExternalTransactionID = uniq(string(kind))
	r.Kind = kind
	r.ReferenceExternalTransactionID = bet.ExternalTransactionID
	return r
}

func requireInbox(t *testing.T, messageID string, txID uuid.UUID) {
	t.Helper()
	type inboxRow struct {
		Outcome       *string
		TransactionID *uuid.UUID
	}
	rows := query(t, pgx.RowToStructByPos[inboxRow],
		`SELECT outcome, transaction_id FROM inbox_messages WHERE consumer_name = 'wager-transactions' AND message_id = $1`, messageID)
	if len(rows) != 1 || rows[0].Outcome == nil || *rows[0].Outcome != string(wager.StatusProcessed) ||
		rows[0].TransactionID == nil || *rows[0].TransactionID != txID {
		t.Errorf("the inbox holds %+v for message %s, want it once, completed PROCESSED as %s", rows, messageID, txID)
	}
}

func inboxRowsOf(t *testing.T, messageID string) int64 {
	t.Helper()
	return query(t, pgx.RowTo[int64],
		`SELECT count(*) FROM inbox_messages WHERE consumer_name = 'wager-transactions' AND message_id = $1`, messageID)[0]
}

type resolverLease struct {
	End  time.Time
	Held bool
}

func leaseOf(t *testing.T, id uuid.UUID) resolverLease {
	t.Helper()
	leases := query(t, pgx.RowToStructByPos[resolverLease],
		`SELECT next_attempt_at, next_attempt_at > now() FROM wager_transactions WHERE id = $1`, id)
	if len(leases) != 1 {
		t.Fatalf("transaction %s not found", id)
	}
	return leases[0]
}

func statusWithin(t *testing.T, id uuid.UUID, until time.Time) (wager.Status, bool) {
	t.Helper()
	type read struct {
		Status wager.Status
		Before bool
	}
	reads := query(t, pgx.RowToStructByPos[read],
		`SELECT status, now() < $2 FROM wager_transactions WHERE id = $1`, id, until)
	if len(reads) != 1 {
		t.Fatalf("transaction %s not found", id)
	}
	return reads[0].Status, reads[0].Before
}

func processedEventAt(t *testing.T, id uuid.UUID) time.Time {
	t.Helper()
	at := query(t, pgx.RowTo[time.Time],
		`SELECT created_at FROM outbox_events WHERE event_type = $1 AND payload->'data'->>'transactionId' = $2`,
		event.TypeWagerTransactionProcessed, id.String())
	if len(at) != 1 {
		t.Fatalf("transaction %s has %d events announcing it processed, want 1", id, len(at))
	}
	return at[0]
}

type outboxRow struct {
	ID          uuid.UUID
	AggregateID string
	EventType   string
	Attempts    int
	LockedUntil *time.Time
	PublishedAt *time.Time
	CreatedAt   time.Time
}

func outboxOf(t *testing.T, ids ...uuid.UUID) []outboxRow {
	t.Helper()
	aggregates := make([]string, len(ids))
	for i, id := range ids {
		aggregates[i] = id.String()
	}
	return query(t, pgx.RowToStructByPos[outboxRow], `
SELECT id, aggregate_id, event_type, attempts, locked_until, published_at, created_at
  FROM outbox_events
 WHERE aggregate_id = ANY ($1)
 ORDER BY seq`, aggregates)
}

func awaitPublished(t *testing.T, ids ...uuid.UUID) []outboxRow {
	t.Helper()
	var rows []outboxRow
	eventually(t, settleTimeout, "every event of the wallets to be published", func() (bool, string) {
		rows = outboxOf(t, ids...)
		left := 0
		for _, r := range rows {
			if r.PublishedAt == nil {
				left++
			}
		}
		return left == 0, fmt.Sprintf("%d of %d not published", left, len(rows))
	})
	return rows
}

func headsOf(rows []outboxRow) map[uuid.UUID]bool {
	heads := make(map[uuid.UUID]bool)
	seen := make(map[string]bool)
	for _, r := range rows {
		if !seen[r.AggregateID] {
			seen[r.AggregateID] = true
			heads[r.ID] = true
		}
	}
	return heads
}

func eventTypes(rows []outboxRow) []string {
	types := make([]string, len(rows))
	for i, r := range rows {
		types[i] = r.EventType
	}
	return types
}

func query[T any](t *testing.T, row pgx.RowToFunc[T], sql string, args ...any) []T {
	t.Helper()
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, suite.env.PG.AppDSN)
	if err != nil {
		t.Fatalf("connect to the database: %v", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	out, err := pgx.CollectRows(rows, row)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return out
}

type eventCopy struct {
	GroupID, DeduplicationID, EventTypeAttribute string
	Envelope                                     struct {
		EventID   uuid.UUID       `json:"eventId"`
		EventType string          `json:"eventType"`
		Data      json.RawMessage `json:"data"`
	}
}

type eventReader struct {
	client *sqs.Client
	groups map[string]bool
	copies []eventCopy
}

func newEventReader(t *testing.T, walletIDs ...uuid.UUID) *eventReader {
	t.Helper()
	client, err := suite.env.AWS.Client(t.Context(), "events-reader")
	if err != nil {
		t.Fatalf("SQS client of events-reader: %v", err)
	}
	r := &eventReader{client: client, groups: make(map[string]bool)}
	for _, id := range walletIDs {
		r.groups[id.String()] = true
	}
	return r
}

func (r *eventReader) await(t *testing.T, what string, done func([]eventCopy) bool) {
	t.Helper()
	ctx := t.Context()
	eventually(t, settleTimeout, what+" to arrive on the events queue", func() (bool, string) {
		if done(r.copies) {
			return true, ""
		}
		out, err := r.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(suite.env.AWS.Queues.Events),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             1,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
			MessageAttributeNames:       []string{"All"},
		})
		if err != nil {
			t.Fatalf("receive from the events queue: %v", err)
		}
		for _, m := range out.Messages {
			r.keep(t, m)
			if _, err := r.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl: aws.String(suite.env.AWS.Queues.Events), ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				t.Fatalf("delete from the events queue: %v", err)
			}
		}
		return done(r.copies), fmt.Sprintf("%d copies of the wallets' events so far", len(r.copies))
	})
}

func (r *eventReader) keep(t *testing.T, m types.Message) {
	t.Helper()
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if !r.groups[group] {
		return
	}
	c := eventCopy{
		GroupID:         group,
		DeduplicationID: m.Attributes[string(types.MessageSystemAttributeNameMessageDeduplicationId)],
	}
	if a, ok := m.MessageAttributes["eventType"]; ok {
		c.EventTypeAttribute = aws.ToString(a.StringValue)
	}
	if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &c.Envelope); err != nil {
		t.Fatalf("decode event of %s: %v", group, err)
	}
	r.copies = append(r.copies, c)
}

func (r *eventReader) awaitRows(t *testing.T, rows []outboxRow) []eventCopy {
	t.Helper()
	want := make(map[uuid.UUID]bool, len(rows))
	byID := make(map[uuid.UUID]outboxRow, len(rows))
	for _, row := range rows {
		want[row.ID] = true
		byID[row.ID] = row
	}
	r.await(t, fmt.Sprintf("the %d events of the outbox", len(rows)), func(got []eventCopy) bool {
		return receivedAll(got, want)
	})

	arrived := make(map[string][]uuid.UUID)
	seen := make(map[uuid.UUID]bool)
	for _, c := range r.copies {
		row, ok := byID[c.Envelope.EventID]
		switch {
		case !ok:
			t.Errorf("event %s %s on the queue has no row in the outbox of its wallet", c.Envelope.EventID, c.Envelope.EventType)
		case c.DeduplicationID != row.ID.String() || c.GroupID != row.AggregateID ||
			c.Envelope.EventType != row.EventType || c.EventTypeAttribute != row.EventType:
			t.Errorf("a copy of event %s arrived as %+v, want deduplicated by its id, in the group of wallet %s, typed %s",
				row.ID, c, row.AggregateID, row.EventType)
		}
		if !seen[c.Envelope.EventID] {
			seen[c.Envelope.EventID] = true
			arrived[c.GroupID] = append(arrived[c.GroupID], c.Envelope.EventID)
		}
	}
	written := make(map[string][]uuid.UUID)
	for _, row := range rows {
		written[row.AggregateID] = append(written[row.AggregateID], row.ID)
	}
	for wallet, ids := range written {
		if !slices.Equal(arrived[wallet], ids) {
			t.Errorf("the events of wallet %s arrived as %v, want them in the order written, %v", wallet, arrived[wallet], ids)
		}
	}
	return r.copies
}

func receivedAll(got []eventCopy, want map[uuid.UUID]bool) bool {
	n := 0
	seen := make(map[uuid.UUID]bool)
	for _, c := range got {
		if want[c.Envelope.EventID] && !seen[c.Envelope.EventID] {
			seen[c.Envelope.EventID] = true
			n++
		}
	}
	return n == len(want)
}

func lastCopyOf(copies []eventCopy, id uuid.UUID) eventCopy {
	var last eventCopy
	for _, c := range copies {
		if c.Envelope.EventID == id {
			last = c
		}
	}
	return last
}

type submission struct {
	bet            TxBody
	status         int
	resp           TxResponse
	err            error
	sent, answered time.Time
}

type betLoad struct {
	stop chan struct{}
	wg   sync.WaitGroup

	mu     sync.Mutex
	next   int
	posted []submission
	queued []TxBody
}

func startLoad(t *testing.T, c *ProviderClient, targets []string, wallets []Wallet, interval time.Duration) *betLoad {
	t.Helper()
	ctx := t.Context()
	l := &betLoad{stop: make(chan struct{})}
	for _, target := range targets {
		l.wg.Go(func() {
			l.every(interval, func() {
				s := submission{bet: l.bet(wallets), sent: time.Now()}
				s.status, s.resp, s.err = c.TryPostTx(ctx, target, s.bet, keyOf(s.bet))
				s.answered = time.Now()
				l.mu.Lock()
				l.posted = append(l.posted, s)
				l.mu.Unlock()
			})
		})
	}
	l.wg.Go(func() {
		l.every(interval, func() {
			b := l.bet(wallets)
			c.SendTx(ctx, b, keyOf(b), uniq("msg"))
			l.mu.Lock()
			l.queued = append(l.queued, b)
			l.mu.Unlock()
		})
	})
	t.Cleanup(l.end)
	return l
}

func (l *betLoad) every(interval time.Duration, send func()) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		send()
		select {
		case <-l.stop:
			return
		case <-tick.C:
		}
	}
}

func (l *betLoad) bet(wallets []Wallet) TxBody {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.next++
	return Bet(wallets[l.next%len(wallets)], uniq("load"+strconv.Itoa(l.next)), "1.00")
}

func (l *betLoad) end() {
	l.mu.Lock()
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
	l.mu.Unlock()
	l.wg.Wait()
}

func (l *betLoad) results() ([]submission, []TxBody) {
	l.end()
	return l.posted, l.queued
}

func (l *betLoad) awaitAnswers(t *testing.T, n, status int) {
	t.Helper()
	count := func() int {
		l.mu.Lock()
		defer l.mu.Unlock()
		k := 0
		for _, s := range l.posted {
			if s.err == nil && s.status == status {
				k++
			}
		}
		return k
	}
	from := count()
	eventually(t, settleTimeout, fmt.Sprintf("%d more answers %d to the load", n, status), func() (bool, string) {
		k := count() - from
		return k >= n, fmt.Sprint(k)
	})
}

func openWallets(t *testing.T, cluster []*Instance, n int, amount string) ([]Wallet, []uuid.UUID) {
	t.Helper()
	ws := make([]Wallet, n)
	ids := make([]uuid.UUID, n)
	for i := range ws {
		ws[i] = OpenWallet(t, cluster[i%len(cluster)], amount)
		ids[i] = ws[i].ID
	}
	return ws, ids
}

func urlsOf(cluster []*Instance) []string {
	urls := make([]string, len(cluster))
	for i, inst := range cluster {
		urls[i] = inst.URL
	}
	return urls
}

func requireSpent(t *testing.T, inst *Instance, ws []Wallet, bets []TxBody) {
	t.Helper()
	per := make(map[uuid.UUID]int)
	for _, b := range bets {
		per[b.WalletID]++
	}
	for _, w := range ws {
		n := per[w.ID]
		if got := GetWallet(t, inst, w.ID); got.Balance != BRL(fmt.Sprintf("%d.00", 1000-n)) || got.Version != int64(n+1) {
			t.Errorf("wallet %s: balance %s at version %d, want %d.00 at version %d after %d bets of 1.00",
				w.ID, got.Balance, got.Version, 1000-n, n+1, n)
		}
		if debits := debitsOf(Ledger(t, inst, w.ID)); len(debits) != n {
			t.Errorf("wallet %s has %d debits, want %d", w.ID, len(debits), n)
		}
	}
}

func betsOf(posted []submission) []TxBody {
	bets := make([]TxBody, len(posted))
	for i, s := range posted {
		bets[i] = s.bet
	}
	return bets
}

func dbNow(t *testing.T) time.Time {
	t.Helper()
	return query(t, pgx.RowTo[time.Time], "SELECT clock_timestamp()")[0]
}
