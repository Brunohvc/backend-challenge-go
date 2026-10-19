//go:build e2e

package e2e

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

// AssertReconciled fails unless every wallet's balance matches its ledger and is not negative.
func AssertReconciled(t *testing.T, inst *Instance, walletIDs ...uuid.UUID) {
	t.Helper()
	for _, id := range walletIDs {
		var rec struct {
			StoredBalance     Money `json:"storedBalance"`
			CalculatedBalance Money `json:"calculatedBalance"`
			Difference        Money `json:"difference"`
			Consistent        bool  `json:"consistent"`
		}
		status := internalCall(t, http.MethodPost, inst.URL+"/wallets/"+id.String()+"/reconciliation", nil, &rec)
		switch {
		case status != http.StatusOK:
			t.Errorf("reconciliation of %s: status %d, want 200", id, status)
		case !rec.Consistent || rec.StoredBalance != rec.CalculatedBalance || rec.Difference.Amount != "0.00":
			t.Errorf("wallet %s does not reconcile: stored %v, ledger %v, difference %v",
				id, rec.StoredBalance, rec.CalculatedBalance, rec.Difference)
		case strings.HasPrefix(rec.StoredBalance.Amount, "-"):
			t.Errorf("wallet %s has a negative balance: %v", id, rec.StoredBalance)
		}
	}
}

var terminal = []wager.Status{wager.StatusProcessed, wager.StatusRejected, wager.StatusFailed}

// WaitForStatus waits until the transaction reaches status want and returns it.
func WaitForStatus(t *testing.T, inst *Instance, providerID, externalID string, want wager.Status, timeout time.Duration) TxView {
	t.Helper()
	var tx TxView
	eventually(t, timeout, fmt.Sprintf("transaction %s of %s to be %s", externalID, providerID, want), func() (bool, string) {
		status, got := GetTx(t, inst, providerID, externalID)
		if status != http.StatusOK {
			return false, fmt.Sprintf("status %d", status)
		}
		tx = got
		if got.Status != want && slices.Contains(terminal, got.Status) {
			t.Fatalf("transaction %s of %s settled %s %s, want %s", externalID, providerID, got.Status, got.FailureCode, want)
		}
		return got.Status == want, string(got.Status)
	})
	return tx
}

// AssertQueuesDrained waits for the input queue to empty and fails unless its DLQ is empty.
func AssertQueuesDrained(t *testing.T) {
	t.Helper()
	queues := suite.env.AWS.Queues
	eventually(t, 30*time.Second, "the input queue to drain", func() (bool, string) {
		n := queueDepth(t, queues.Input)
		return n == 0, fmt.Sprintf("%d messages", n)
	})
	if n := queueDepth(t, queues.InputDLQ); n != 0 {
		t.Errorf("the dead-letter queue holds %d messages, want none", n)
	}
}

var emptiedFor string

func freshQueues(t *testing.T) {
	t.Helper()
	test, _, _ := strings.Cut(t.Name(), "/")
	if emptiedFor == test {
		return
	}
	emptiedFor = test
	queues := suite.env.AWS.Queues
	for _, url := range []string{queues.Input, queues.InputDLQ} {
		if _, err := suite.env.AWS.RootClient().PurgeQueue(t.Context(), &sqs.PurgeQueueInput{QueueUrl: aws.String(url)}); err != nil {
			t.Fatalf("purge %s: %v", url, err)
		}
		eventually(t, 30*time.Second, "the purge of "+url, func() (bool, string) {
			n := queueDepth(t, url)
			return n == 0, fmt.Sprintf("%d messages", n)
		})
	}
}

func queueDepth(t *testing.T, url string) int {
	t.Helper()
	out, err := suite.env.AWS.RootClient().GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatalf("attributes of %s: %v", url, err)
	}
	total := 0
	for _, name := range []types.QueueAttributeName{
		types.QueueAttributeNameApproximateNumberOfMessages,
		types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
	} {
		n, err := strconv.Atoi(out.Attributes[string(name)])
		if err != nil {
			t.Fatalf("attribute %s of %s: %v", name, url, err)
		}
		total += n
	}
	return total
}

// Metric returns the sum of the samples of counter name whose labels include labels.
func Metric(t *testing.T, inst *Instance, name string, labels map[string]string) int64 {
	t.Helper()
	r, err := call(t.Context(), http.MethodGet, inst.AdminURL+"/metrics", "", nil, nil)
	if err != nil || r.status != http.StatusOK {
		t.Fatalf("scrape %s: status %d, error %v", inst.Name, r.status, err)
	}
	total, err := sumSamples(string(r.body), name, labels)
	if err != nil {
		t.Fatalf("scrape of %s: %v", inst.Name, err)
	}
	return total
}

// MetricSum returns Metric summed over every instance of cluster.
func MetricSum(t *testing.T, cluster []*Instance, name string, labels map[string]string) int64 {
	t.Helper()
	var total int64
	for _, inst := range cluster {
		total += Metric(t, inst, name, labels)
	}
	return total
}

func sumSamples(scrape, name string, labels map[string]string) (int64, error) {
	var total int64
	for line := range strings.Lines(scrape) {
		line = strings.TrimRight(line, "\n")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		end := strings.IndexAny(line, "{ ")
		if end < 0 || line[:end] != name {
			continue
		}
		got, rest := map[string]string{}, line[end:]
		if strings.HasPrefix(rest, "{") {
			var err error
			if got, rest, err = parseLabels(rest[1:]); err != nil {
				return 0, fmt.Errorf("sample %q: %w", line, err)
			}
		}
		if !includes(got, labels) {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			return 0, fmt.Errorf("sample %q has no value", line)
		}
		n, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("sample %q: value is not an integer: %w", line, err)
		}
		total += n
	}
	return total, nil
}

func parseLabels(s string) (map[string]string, string, error) {
	labels := map[string]string{}
	for {
		if rest, ok := strings.CutPrefix(s, "}"); ok {
			return labels, rest, nil
		}
		name, rest, ok := strings.Cut(s, `="`)
		if !ok {
			return nil, "", errors.New("label without a quoted value")
		}
		var value strings.Builder
		for {
			if rest == "" {
				return nil, "", fmt.Errorf("label %s is not closed", name)
			}
			c := rest[0]
			rest = rest[1:]
			if c == '"' {
				break
			}
			if c == '\\' && rest != "" {
				c, rest = unescape(rest[0]), rest[1:]
			}
			value.WriteByte(c)
		}
		labels[name] = value.String()
		s = strings.TrimPrefix(rest, ",")
	}
}

func unescape(c byte) byte {
	if c == 'n' {
		return '\n'
	}
	return c
}

func includes(got, want map[string]string) bool {
	for name, value := range want {
		if v, ok := got[name]; !ok || v != value {
			return false
		}
	}
	return true
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, last := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s; last seen: %s", what, timeout, last)
		}
		time.Sleep(pollEvery)
	}
}

func series(name string, l map[string]string) string {
	if len(l) == 0 {
		return name
	}
	pairs := make([]string, 0, len(l))
	for _, k := range slices.Sorted(maps.Keys(l)) {
		pairs = append(pairs, fmt.Sprintf("%s=%q", k, l[k]))
	}
	return name + "{" + strings.Join(pairs, ",") + "}"
}

func labels(pairs ...string) map[string]string {
	m := make(map[string]string, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return m
}
