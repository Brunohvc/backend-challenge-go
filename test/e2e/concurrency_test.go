//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
)

const (
	instances     = 3
	settleTimeout = 30 * time.Second
	lockTimeout   = 2 * time.Second
	txAttempts    = 3
)

func TestE2E_TwoBetsOf80_HTTPandSQS(t *testing.T) {
	ctx := t.Context()
	cluster := StartCluster(t, instances, nil)
	provider := NewProviderClient(t, "provider-a")
	w := OpenWallet(t, cluster[0], "100.00")
	betA, betB := Bet(w, uniq("bet-a"), "80.00"), Bet(w, uniq("bet-b"), "80.00")
	keyA, keyB := "key-"+betA.ExternalTransactionID, "key-"+betB.ExternalTransactionID

	lock := LockWallet(t, w.ID)
	provider.SendTx(ctx, betB, keyB, uniq("msg-b"))
	consumer := lock.AwaitWaiters(t, 1)[0]
	api := otherThan(cluster, consumer)
	answerA := postAsync(ctx, provider, api, betA, keyA)
	if waiting := lock.AwaitWaiters(t, 2); waiting[0] == waiting[1] {
		t.Fatalf("both bets wait in %s, want two instances", waiting[0])
	}
	lock.Release(t)

	a := <-answerA
	var winner uuid.UUID
	var txB TxView
	switch {
	case a.status == http.StatusCreated && a.resp.Status == wager.StatusProcessed:
		winner = a.resp.TransactionID
		txB = WaitForStatus(t, cluster[0], "provider-a", betB.ExternalTransactionID, wager.StatusRejected, settleTimeout)
	case a.status == http.StatusUnprocessableEntity && a.resp.Status == wager.StatusRejected:
		requireFailure(t, "bet A", a.resp.FailureCode)
		txB = WaitForStatus(t, cluster[0], "provider-a", betB.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
		winner = txB.TransactionID
	default:
		t.Fatalf("bet A through %s: %d %+v, want 201 PROCESSED or 422 REJECTED", api.Name, a.status, a.resp)
	}
	if txB.Status == wager.StatusRejected {
		requireFailure(t, "bet B", txB.FailureCode)
	}
	t.Logf("bet B waited in %s, bet A in %s; the winner is %s", consumer, api.Name, winner)
	requireBalance(t, cluster[1], w.ID, "20.00", 2)
	requireDebits(t, cluster[2], w.ID, winner)

	provider.SendTx(ctx, betA, keyA, uniq("msg-a-again"))
	status, againB := provider.PostTx(ctx, otherThan(cluster, consumer), betB, keyB)
	wantStatus := http.StatusOK
	if txB.Status == wager.StatusRejected {
		wantStatus = http.StatusUnprocessableEntity
	}
	if status != wantStatus || !againB.IdempotentReplay || againB.TransactionID != txB.TransactionID || againB.Status != txB.Status {
		t.Errorf("bet B again over HTTP: %d %+v, want %d replaying %s %s", status, againB, wantStatus, txB.TransactionID, txB.Status)
	}
	awaitMetric(t, cluster, "wagering_duplicates_total", labels("channel", "sqs", "source", "idempotency"), 1)
	AssertQueuesDrained(t)
	if status, txA := GetTx(t, cluster[2], "provider-a", betA.ExternalTransactionID); status != http.StatusOK || txA.Status != a.resp.Status || txA.TransactionID != a.resp.TransactionID {
		t.Errorf("bet A after its replay: %d %+v, want %s %s", status, txA, a.resp.TransactionID, a.resp.Status)
	}
	requireMetric(t, cluster, "wagering_transactions_total", nil, 2)
	requireMetric(t, cluster, "wagering_duplicates_total", nil, 2)
	requireBalance(t, cluster[0], w.ID, "20.00", 2)
	requireDebits(t, cluster[0], w.ID, winner)
	AssertReconciled(t, cluster[0], w.ID)
}

func TestE2E_SameBet50Times(t *testing.T) {
	const perChannel = 25
	ctx := t.Context()
	cluster := StartCluster(t, instances, nil)
	provider := NewProviderClient(t, "provider-a")
	w := OpenWallet(t, cluster[0], "100.00")
	bet := Bet(w, uniq("same-bet"), "10.00")
	key := "key-" + bet.ExternalTransactionID

	start := make(chan struct{})
	answers := make([]answer, perChannel)
	var wg sync.WaitGroup
	for i := range perChannel {
		wg.Go(func() {
			<-start
			answers[i].status, answers[i].resp = provider.PostTx(ctx, cluster[i%len(cluster)], bet, key)
		})
		wg.Go(func() {
			<-start
			provider.SendTx(ctx, bet, key, fmt.Sprintf("msg-%02d-%s", i, bet.ExternalTransactionID))
		})
	}
	close(start)
	wg.Wait()
	awaitMetric(t, cluster, "wagering_duplicates_total", nil, 2*perChannel-1)
	AssertQueuesDrained(t)

	tx := WaitForStatus(t, cluster[0], "provider-a", bet.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
	created := 0
	for i, a := range answers {
		replay := a.status == http.StatusOK
		if a.status == http.StatusCreated {
			created++
		}
		if (!replay && a.status != http.StatusCreated) || a.resp.IdempotentReplay != replay ||
			a.resp.TransactionID != tx.TransactionID || a.resp.Status != wager.StatusProcessed ||
			a.resp.Balance == nil || *a.resp.Balance != BRL("90.00") {
			t.Errorf("request %d: %d %+v, want 201 or a 200 replay of %s with balance 90.00", i, a.status, a.resp, tx.TransactionID)
		}
	}
	if created > 1 {
		t.Errorf("%d requests were answered 201, want at most one", created)
	}
	requireMetric(t, cluster, "wagering_duplicates_total", labels("channel", "http"), int64(perChannel-created))
	requireMetric(t, cluster, "wagering_duplicates_total", labels("channel", "sqs"), int64(perChannel-1+created))
	requireMetric(t, cluster, "wagering_duplicates_total", nil, 2*perChannel-1)
	requireMetric(t, cluster, "wagering_transactions_total", nil, 1)
	requireBalance(t, cluster[1], w.ID, "90.00", 2)
	requireDebits(t, cluster[2], w.ID, tx.TransactionID)
	AssertReconciled(t, cluster[0], w.ID)
}

func TestE2E_ParallelWallets(t *testing.T) {
	cluster := StartCluster(t, instances, map[string]string{
		"DB_LOCK_TIMEOUT":    lockTimeout.String(),
		"DB_TX_MAX_ATTEMPTS": strconv.Itoa(txAttempts),
	})

	t.Run("every wallet completes", func(t *testing.T) {
		const wallets, betsPerWallet = 20, 10
		ctx := t.Context()
		provider := NewProviderClient(t, "provider-a")
		ws := make([]Wallet, wallets)
		ids := make([]uuid.UUID, wallets)
		for i := range ws {
			ws[i] = OpenWallet(t, cluster[i%len(cluster)], "100.00")
			ids[i] = ws[i].ID
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, w := range ws {
			for j := range betsPerWallet {
				bet := Bet(w, uniq(fmt.Sprintf("w%02d-b%d", i, j)), "5.00")
				key := "key-" + bet.ExternalTransactionID
				if j%2 == 1 {
					wg.Go(func() { <-start; provider.SendTx(ctx, bet, key, "msg-"+bet.ExternalTransactionID) })
					continue
				}
				inst := cluster[(i+j)%len(cluster)]
				wg.Go(func() {
					<-start
					if status, resp := provider.PostTx(ctx, inst, bet, key); status != http.StatusCreated || resp.Status != wager.StatusProcessed {
						t.Errorf("bet %s through %s: %d %+v, want 201 PROCESSED", bet.ExternalTransactionID, inst.Name, status, resp)
					}
				})
			}
		}
		began := time.Now()
		close(start)
		wg.Wait()
		awaitMetric(t, cluster, "wagering_transactions_total", labels("status", string(wager.StatusProcessed)), wallets*betsPerWallet)
		t.Logf("%d bets on %d wallets processed in %s", wallets*betsPerWallet, wallets, time.Since(began))
		AssertQueuesDrained(t)

		for i, w := range ws {
			requireBalance(t, cluster[i%len(cluster)], w.ID, "50.00", betsPerWallet+1)
			if debits := debitsOf(Ledger(t, cluster[(i+1)%len(cluster)], w.ID)); len(debits) != betsPerWallet {
				t.Errorf("wallet %s has %d debits, want %d", w.ID, len(debits), betsPerWallet)
			}
		}
		requireMetric(t, cluster, "wagering_transactions_total", nil, wallets*betsPerWallet)
		requireMetric(t, cluster, "wagering_duplicates_total", nil, 0)
		AssertReconciled(t, cluster[1], ids...)
	})

	t.Run("a held wallet delays only its own bets", func(t *testing.T) {
		const others = 6
		ctx := t.Context()
		provider := NewProviderClient(t, "provider-a")
		held := OpenWallet(t, cluster[0], "100.00")
		ws := make([]Wallet, others)
		ids := []uuid.UUID{held.ID}
		for i := range ws {
			ws[i] = OpenWallet(t, cluster[i%len(cluster)], "100.00")
			ids = append(ids, ws[i].ID)
		}

		lock := LockWallet(t, held.ID)
		heldHTTP, heldSQS := Bet(held, uniq("held-http"), "5.00"), Bet(held, uniq("held-sqs"), "5.00")
		heldKey := "key-" + heldHTTP.ExternalTransactionID
		answerHeld := postAsync(ctx, provider, cluster[0], heldHTTP, heldKey)
		provider.SendTx(ctx, heldSQS, "key-"+heldSQS.ExternalTransactionID, uniq("msg-held"))
		t.Logf("waiting on the held wallet: %v", lock.AwaitWaiters(t, 2))

		var wg sync.WaitGroup
		queued := make([]string, 0, others)
		for i, w := range ws {
			for _, inst := range cluster {
				bet := Bet(w, uniq(fmt.Sprintf("other%d-%s", i, inst.Name)), "5.00")
				wg.Go(func() {
					began := time.Now()
					status, resp := provider.PostTx(ctx, inst, bet, "key-"+bet.ExternalTransactionID)
					if took := time.Since(began); status != http.StatusCreated || resp.Status != wager.StatusProcessed || took > time.Second {
						t.Errorf("bet on %s through %s while another wallet was held: %d %+v in %s, want 201 PROCESSED within 1s",
							w.ID, inst.Name, status, resp, took)
					}
				})
			}
			bet := Bet(w, uniq(fmt.Sprintf("other%d-sqs", i)), "5.00")
			provider.SendTx(ctx, bet, "key-"+bet.ExternalTransactionID, "msg-"+bet.ExternalTransactionID)
			queued = append(queued, bet.ExternalTransactionID)
		}
		wg.Wait()
		settled := time.Now().Add(lockTimeout / 2)
		for i, ext := range queued {
			WaitForStatus(t, cluster[i%len(cluster)], "provider-a", ext, wager.StatusProcessed, time.Until(settled))
		}
		select {
		case a := <-answerHeld:
			t.Fatalf("the bet on the held wallet was answered %d %+v before its %d attempts of %s could have run out",
				a.status, a.resp, txAttempts, lockTimeout)
		default:
		}
		requireBalance(t, cluster[1], held.ID, "100.00", 1)

		a := <-answerHeld
		if gaveUp := txAttempts * lockTimeout; a.status != http.StatusServiceUnavailable || a.resp.Code != "TEMPORARILY_UNAVAILABLE" || a.took < gaveUp {
			t.Errorf("bet on the held wallet: %d %+v after %s, want 503 TEMPORARILY_UNAVAILABLE after at least %s", a.status, a.resp, a.took, gaveUp)
		}
		if status, _ := GetTx(t, cluster[2], "provider-a", heldHTTP.ExternalTransactionID); status != http.StatusNotFound {
			t.Errorf("the bet that gave up: status %d, want 404", status)
		}
		requireBalance(t, cluster[2], held.ID, "100.00", 1)

		lock.Release(t)
		WaitForStatus(t, cluster[1], "provider-a", heldSQS.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
		if status, resp := provider.PostTx(ctx, cluster[0], heldHTTP, heldKey); status != http.StatusCreated || resp.IdempotentReplay {
			t.Errorf("the bet sent again after the lock: %d %+v, want 201 decided now", status, resp)
		}
		requireBalance(t, cluster[0], held.ID, "90.00", 3)
		AssertQueuesDrained(t)
		AssertReconciled(t, cluster[2], ids...)
	})
}

func TestE2E_CrossChannelSameOperation(t *testing.T) {
	cluster := StartCluster(t, instances, nil)
	ran := 0

	t.Run("HTTP then SQS", func(t *testing.T) {
		ran++
		ctx := t.Context()
		provider := NewProviderClient(t, "provider-a")
		w := OpenWallet(t, cluster[0], "100.00")
		bet := Bet(w, uniq("http-first"), "10.00")
		key := "key-" + bet.ExternalTransactionID

		status, first := provider.PostTx(ctx, cluster[0], bet, key)
		if status != http.StatusCreated || first.Status != wager.StatusProcessed {
			t.Fatalf("first over HTTP: %d %+v, want 201 PROCESSED", status, first)
		}
		provider.SendTx(ctx, bet, key, uniq("msg-http-first"))
		awaitMetric(t, cluster, "wagering_duplicates_total", labels("channel", "sqs", "source", "idempotency"), 1)
		AssertQueuesDrained(t)
		if status, tx := GetTx(t, cluster[1], "provider-a", bet.ExternalTransactionID); status != http.StatusOK || tx.TransactionID != first.TransactionID {
			t.Errorf("after the message: %d %+v, want %s", status, tx, first.TransactionID)
		}
		requireBalance(t, cluster[2], w.ID, "90.00", 2)
		requireDebits(t, cluster[0], w.ID, first.TransactionID)
		AssertReconciled(t, cluster[1], w.ID)
	})

	t.Run("SQS then HTTP", func(t *testing.T) {
		ran++
		ctx := t.Context()
		provider := NewProviderClient(t, "provider-a")
		w := OpenWallet(t, cluster[0], "100.00")
		bet := Bet(w, uniq("sqs-first"), "10.00")
		key := "key-" + bet.ExternalTransactionID

		provider.SendTx(ctx, bet, key, uniq("msg-sqs-first"))
		first := WaitForStatus(t, cluster[0], "provider-a", bet.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
		status, again := provider.PostTx(ctx, cluster[1], bet, key)
		if status != http.StatusOK || !again.IdempotentReplay || again.TransactionID != first.TransactionID ||
			again.Balance == nil || *again.Balance != BRL("90.00") {
			t.Errorf("again over HTTP: %d %+v, want a 200 replay of %s with balance 90.00", status, again, first.TransactionID)
		}
		requireMetric(t, cluster, "wagering_duplicates_total", labels("channel", "http", "source", "idempotency"), 1)
		AssertQueuesDrained(t)
		requireBalance(t, cluster[2], w.ID, "90.00", 2)
		requireDebits(t, cluster[0], w.ID, first.TransactionID)
		AssertReconciled(t, cluster[1], w.ID)
	})

	if ran == 2 {
		requireMetric(t, cluster, "wagering_transactions_total", nil, 2)
		requireMetric(t, cluster, "wagering_duplicates_total", nil, 2)
	}
}

type answer struct {
	status int
	resp   TxResponse
	took   time.Duration
}

func postAsync(ctx context.Context, c *ProviderClient, inst *Instance, body TxBody, key string) <-chan answer {
	ch := make(chan answer, 1)
	done := make(chan struct{})
	c.t.Cleanup(func() { <-done })
	go func() {
		defer close(done)
		began := time.Now()
		status, resp := c.PostTx(ctx, inst, body, key)
		ch <- answer{status: status, resp: resp, took: time.Since(began)}
	}()
	return ch
}

func otherThan(cluster []*Instance, name string) *Instance {
	for _, inst := range cluster {
		if inst.Name != name {
			return inst
		}
	}
	return nil
}

func uniq(prefix string) string {
	return prefix + "-" + idgen.V7{}.New().String()
}

func requireFailure(t *testing.T, what string, got wager.FailureCode) {
	t.Helper()
	if got != wager.FailureInsufficientFunds {
		t.Errorf("%s rejected with %q, want %s", what, got, wager.FailureInsufficientFunds)
	}
}

func requireBalance(t *testing.T, inst *Instance, id uuid.UUID, amount string, version int64) {
	t.Helper()
	if w := GetWallet(t, inst, id); w.Balance != BRL(amount) || w.Version != version {
		t.Errorf("wallet %s: balance %v at version %d, want %s at version %d", id, w.Balance, w.Version, amount, version)
	}
}

func requireDebits(t *testing.T, inst *Instance, id uuid.UUID, txIDs ...uuid.UUID) {
	t.Helper()
	var got []uuid.UUID
	for _, e := range debitsOf(Ledger(t, inst, id)) {
		got = append(got, e.TransactionID)
	}
	if !slices.Equal(got, txIDs) {
		t.Errorf("wallet %s debits the transactions %v, want %v", id, got, txIDs)
	}
}

func debitsOf(items []LedgerItem) []LedgerItem {
	var debits []LedgerItem
	for _, e := range items {
		if e.Direction == string(wallet.DirectionDebit) {
			debits = append(debits, e)
		}
	}
	return debits
}

func requireMetric(t *testing.T, cluster []*Instance, name string, l map[string]string, want int64) {
	t.Helper()
	if got := MetricSum(t, cluster, name, l); got != want {
		t.Errorf("%s over the cluster = %d, want %d", series(name, l), got, want)
	}
}

func awaitMetric(t *testing.T, cluster []*Instance, name string, l map[string]string, want int64) {
	t.Helper()
	eventually(t, settleTimeout, fmt.Sprintf("%s to reach %d", series(name, l), want), func() (bool, string) {
		got := MetricSum(t, cluster, name, l)
		if got > want {
			t.Fatalf("%s over the cluster = %d, past %d", series(name, l), got, want)
		}
		return got == want, fmt.Sprint(got)
	})
}

type WalletLock struct {
	conn  *pgx.Conn
	tx    pgx.Tx
	probe *pgx.Conn
	done  bool
}

func LockWallet(t *testing.T, id uuid.UUID) *WalletLock {
	t.Helper()
	ctx := t.Context()
	l := &WalletLock{}
	var err error
	if l.conn, err = pgx.Connect(ctx, suite.env.PG.AppDSN); err != nil {
		t.Fatalf("connect to lock wallet %s: %v", id, err)
	}
	t.Cleanup(func() { l.Release(t) })
	if l.probe, err = pgx.Connect(ctx, suite.env.PG.AppDSN); err != nil {
		t.Fatalf("connect to watch wallet %s: %v", id, err)
	}
	if l.tx, err = l.conn.Begin(ctx); err != nil {
		t.Fatalf("begin to lock wallet %s: %v", id, err)
	}
	var locked uuid.UUID
	if err := l.tx.QueryRow(ctx, "SELECT id FROM wallets WHERE id = $1 FOR UPDATE", id).Scan(&locked); err != nil {
		t.Fatalf("lock wallet %s: %v", id, err)
	}
	return l
}

func (l *WalletLock) AwaitWaiters(t *testing.T, n int) []string {
	t.Helper()
	var waiters []string
	eventually(t, 10*time.Second, fmt.Sprintf("%d sessions to wait on the wallet", n), func() (bool, string) {
		waiters = l.waiters(t)
		return len(waiters) == n, fmt.Sprint(waiters)
	})
	return waiters
}

func (l *WalletLock) waiters(t *testing.T) []string {
	t.Helper()
	rows, err := l.probe.Query(t.Context(), `
WITH RECURSIVE queued(pid) AS (
    SELECT a.pid FROM pg_stat_activity a WHERE $1::int = ANY (pg_blocking_pids(a.pid))
    UNION
    SELECT a.pid FROM pg_stat_activity a JOIN queued q ON q.pid = ANY (pg_blocking_pids(a.pid))
)
SELECT a.application_name FROM pg_stat_activity a JOIN queued USING (pid) ORDER BY 1`, l.conn.PgConn().PID())
	if err != nil {
		t.Fatalf("sessions waiting on the wallet: %v", err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("sessions waiting on the wallet: %v", err)
	}
	return names
}

func (l *WalletLock) Release(t *testing.T) {
	t.Helper()
	if l.done {
		return
	}
	l.done = true
	ctx := context.WithoutCancel(t.Context())
	if l.tx != nil {
		if err := l.tx.Rollback(ctx); err != nil {
			t.Errorf("release the wallet: %v", err)
		}
	}
	for _, c := range []*pgx.Conn{l.conn, l.probe} {
		if c != nil {
			_ = c.Close(ctx)
		}
	}
}
