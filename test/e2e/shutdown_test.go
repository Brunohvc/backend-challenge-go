//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

const stopBudget = 60 * time.Second

func TestE2E_SIGTERMUnderLoadThroughNginx(t *testing.T) {
	cluster := StartCluster(t, instances, BehindNginx(labels("SHUTDOWN_TIMEOUT", stopBudget.String(), "SHUTDOWN_DRAIN_DELAY", "2s")))
	lb := StartNginx(t, cluster)
	provider := NewProviderClient(t, "provider-a")
	ws, ids := openWallets(t, cluster, 2*instances, "1000.00")
	clients := []string{lb.URL, lb.URL, lb.URL, lb.URL}
	load := startLoad(t, provider, clients, ws, loadInterval/2)
	load.awaitAnswers(t, 40, http.StatusCreated)

	before := lb.Log(t)
	if failed := ConnectFailures(before); len(failed) > 0 {
		t.Errorf("nginx failed %d connections to instances that were all up:\n%s", len(failed), strings.Join(failed, "\n"))
	}
	stopping := cluster[1]
	began := time.Now()
	stopping.Terminate(t, stopBudget)
	took := time.Since(began)
	load.awaitAnswers(t, 80, http.StatusCreated)
	posted, queued := load.results()

	for _, s := range posted {
		switch {
		case s.err != nil:
			t.Errorf("bet %s through nginx got no answer: %v", s.bet.ExternalTransactionID, s.err)
		case s.status/100 == 2 && s.resp.Status == wager.StatusProcessed:
		default:
			t.Errorf("bet %s through nginx, sent at %s, answered %d %+v after %s, want 2xx PROCESSED",
				s.bet.ExternalTransactionID, s.sent.Format(time.StampMilli), s.status, s.resp, s.answered.Sub(s.sent))
		}
	}
	for i, b := range queued {
		WaitForStatus(t, cluster[2*(i%2)], "provider-a", b.ExternalTransactionID, wager.StatusProcessed, settleTimeout)
	}
	AssertQueuesDrained(t)
	requireSpent(t, cluster[0], ws, append(betsOf(posted), queued...))

	out := stopping.Output()
	served, handled := strings.Count(out, `"route":"POST /wagering/transactions"`), strings.Count(out, `"msg":"message handled"`)
	if served == 0 || handled == 0 {
		t.Errorf("%s served %d bets over HTTP and handled %d messages before it stopped, want some of each", stopping.Name, served, handled)
	}
	_, stopped, _ := strings.Cut(out, `"msg":"shutting down: reporting not ready"`)
	servedStopping := strings.Count(stopped, `"route":"POST /wagering/transactions"`)
	if !strings.Contains(out, `"msg":"consumer stopped"`) || strings.Contains(out, "did not stop in time") {
		t.Errorf("%s did not stop its consumer and workers cleanly", stopping.Name)
	}
	resent := resentRequests(t, lb.Log(t)) - resentRequests(t, before)
	t.Logf("%s stopped in %s; it served %d bets, %d of them after the signal, and handled %d messages; nginx resent %d of the requests to another instance after the signal; %d bets over HTTP and %d through the queue in all",
		stopping.Name, took.Round(time.Millisecond), served, servedStopping, handled, resent, len(posted), len(queued))
	AssertReconciled(t, cluster[0], ids...)
}

func resentRequests(t *testing.T, log string) int {
	t.Helper()
	n := 0
	for line := range strings.Lines(log) {
		var entry struct {
			UpstreamAddr string `json:"upstreamAddr"`
		}
		if json.Unmarshal([]byte(line), &entry) == nil && strings.Contains(entry.UpstreamAddr, ", ") {
			n++
		}
	}
	return n
}
