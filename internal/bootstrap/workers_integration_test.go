//go:build integration

package bootstrap_test

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsout"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
)

const waitFor = 15 * time.Second

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, waitFor)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func openWallet(t *testing.T, api *client, internal string) (walletID, playerID string) {
	t.Helper()
	playerID = idgen.V7{}.New().String()
	code, wallet := api.call(http.MethodPost, "/wallets", internal, map[string]any{
		"playerId":       playerID,
		"initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"},
	}, nil)
	if code != http.StatusCreated {
		t.Fatalf("POST /wallets = %d %v, want 201", code, wallet)
	}
	walletID, _ = wallet["id"].(string)
	return walletID, playerID
}

func (c *client) read(path, token string) (int, map[string]any) {
	c.t.Helper()
	req := c.request(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	code, raw := c.send(req)
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		c.t.Fatalf("GET %s = %d: decode %q: %v", path, code, raw, err)
	}
	return code, decoded
}

func sqsClient(t *testing.T, profile string) *sqs.Client {
	t.Helper()
	c, err := env.AWS.Client(t.Context(), profile)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type publishedEvent struct {
	eventID, dedupID string
	eventType        string
	transactionID    string
}

func receiveEvents(t *testing.T, walletID, txID string, done func([]publishedEvent) bool) []publishedEvent {
	t.Helper()
	reader := sqsClient(t, "events-reader")
	var got []publishedEvent
	eventually(t, "the publication of the events of "+txID, func() bool {
		out, err := reader.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(env.AWS.Queues.Events),
			MaxNumberOfMessages:   10,
			WaitTimeSeconds:       1,
			MessageAttributeNames: []string{sqsout.EventTypeAttribute},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameMessageGroupId, types.MessageSystemAttributeNameMessageDeduplicationId,
			},
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		for _, m := range out.Messages {
			if _, err := reader.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{
				QueueUrl: aws.String(env.AWS.Queues.Events), ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				t.Fatalf("DeleteMessage: %v", err)
			}
			if m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)] != walletID {
				continue
			}
			var body struct {
				EventID   string `json:"eventId"`
				EventType string `json:"eventType"`
				Data      struct {
					TransactionID string `json:"transactionId"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &body); err != nil {
				t.Fatalf("event body: %v", err)
			}
			if body.Data.TransactionID != txID {
				continue
			}
			if attr := aws.ToString(m.MessageAttributes[sqsout.EventTypeAttribute].StringValue); attr != body.EventType {
				t.Errorf("event %s: eventType attribute %q, body %q", body.EventID, attr, body.EventType)
			}
			got = append(got, publishedEvent{
				eventID:       body.EventID,
				dedupID:       m.Attributes[string(types.MessageSystemAttributeNameMessageDeduplicationId)],
				eventType:     body.EventType,
				transactionID: body.Data.TransactionID,
			})
		}
		return done(got)
	})
	return got
}

func outboxEvents(t *testing.T, txID string) []publishedEvent {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), env.PG.AppDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	rows, err := conn.Query(t.Context(),
		`SELECT id::text, event_type FROM outbox_events WHERE payload->'data'->>'transactionId' = $1 ORDER BY seq`, txID)
	if err != nil {
		t.Fatalf("query the outbox: %v", err)
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (publishedEvent, error) {
		e := publishedEvent{transactionID: txID}
		err := row.Scan(&e.eventID, &e.eventType)
		e.dedupID = e.eventID
		return e, err
	})
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	return events
}

func wagerMessage(t *testing.T, playerID, walletID, kind string) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"messageId":  "message-" + playerID,
		"type":       sqsin.MessageType,
		"occurredAt": "2026-10-16T12:00:00.000Z",
		"data": map[string]any{
			"providerId":            "provider-a",
			"externalTransactionId": "external-" + playerID,
			"idempotencyKey":        "key-" + playerID,
			"playerId":              playerID,
			"walletId":              walletID,
			"roundId":               "round-1",
			"gameId":                "game-1",
			"kind":                  kind,
			"money":                 map[string]string{"amount": "25.00", "currency": "BRL"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func sendToInput(t *testing.T, body, providerToken, group, dedupID string) string {
	t.Helper()
	out, err := sqsClient(t, "provider-a").SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(env.AWS.Queues.Input),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: aws.String(dedupID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			sqsin.ProviderTokenAttribute: {DataType: aws.String("String"), StringValue: aws.String(providerToken)},
		},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	return aws.ToString(out.MessageId)
}

func requireInputQueueEmpty(t *testing.T) {
	t.Helper()
	root := env.AWS.RootClient()
	eventually(t, "the input queue to empty", func() bool {
		attrs, err := root.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(env.AWS.Queues.Input),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
				types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if err != nil {
			t.Fatalf("GetQueueAttributes: %v", err)
		}
		return attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)] == "0" &&
			attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)] == "0"
	})
}

func receiveDeadLetter(t *testing.T, sourceID string) types.Message {
	t.Helper()
	root := env.AWS.RootClient()
	var found types.Message
	eventually(t, "the message "+sourceID+" in the dead-letter queue", func() bool {
		out, err := root.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(env.AWS.Queues.InputDLQ),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             1,
			VisibilityTimeout:           1,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
			MessageAttributeNames:       []string{"All"},
		})
		if err != nil {
			t.Fatalf("ReceiveMessage from the dead-letter queue: %v", err)
		}
		for _, m := range out.Messages {
			if aws.ToString(m.MessageAttributes[sqsin.SourceMessageIDAttribute].StringValue) != sourceID {
				continue
			}
			if _, err := root.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{
				QueueUrl: aws.String(env.AWS.Queues.InputDLQ), ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				t.Fatalf("DeleteMessage from the dead-letter queue: %v", err)
			}
			found = m
			return true
		}
		return false
	})
	return found
}

func TestApp_ConsumesAndPublishes(t *testing.T) {
	internal := token(t, "wallet-service", "wallet-service-secret")
	provider := token(t, "provider-a", "provider-a-secret")
	app := newApp(t, appConfig(t, nil))
	app.RequireStart()
	defer app.RequireStop()
	api := newClient(t, "http://"+app.api.Addr())
	defer api.http.CloseIdleConnections()
	admin := newClient(t, "http://"+app.admin.Addr())
	defer admin.http.CloseIdleConnections()
	walletID, playerID := openWallet(t, api, internal)

	external := "external-" + playerID
	sendToInput(t, wagerMessage(t, playerID, walletID, "BET"), provider, walletID, "message-"+playerID)

	var tx map[string]any
	eventually(t, "the decision of the bet sent to the queue", func() bool {
		var code int
		code, tx = api.read("/providers/provider-a/wagering/transactions/"+external, provider)
		return code == http.StatusOK
	})
	if tx["status"] != "PROCESSED" {
		t.Fatalf("bet sent to the queue = %v, want PROCESSED", tx)
	}
	if balance, _ := tx["balance"].(map[string]any); balance["amount"] != "75.00" {
		t.Errorf("balance after the bet = %v, want 75.00", tx["balance"])
	}
	requireInputQueueEmpty(t)
	if code, exposition := admin.get("/metrics"); code != http.StatusOK ||
		!strings.Contains(exposition, `sqs_messages_total{result="processed"} 1`) {
		t.Errorf("GET /metrics = %d, want the message counted as processed:\n%s", code, exposition)
	}

	txID, _ := tx["transactionId"].(string)
	want := outboxEvents(t, txID)
	var kinds []string
	for _, e := range want {
		kinds = append(kinds, e.eventType)
	}
	slices.Sort(kinds)
	if wantKinds := []string{event.TypeWagerTransactionProcessed, event.TypeWalletBalanceChanged}; !slices.Equal(kinds, wantKinds) {
		t.Fatalf("outbox events of the bet = %v, want one of each of %v", want, wantKinds)
	}
	got := receiveEvents(t, walletID, txID, func(got []publishedEvent) bool { return len(got) >= len(want) })
	if !slices.Equal(got, want) {
		t.Errorf("events published = %v, want the outbox's, under the same ids and in the same order: %v", got, want)
	}
}

func TestApp_MovesInvalidMessagesToTheInputDLQ(t *testing.T) {
	provider := token(t, "provider-a", "provider-a-secret")
	app := newApp(t, appConfig(t, nil))
	app.RequireStart()
	defer app.RequireStop()
	tests := []struct {
		name       string
		body       func(t *testing.T, playerID, walletID string) string
		wantReason string
		wantCode   string
	}{
		{
			name: "a kind only the service may open",
			body: func(t *testing.T, playerID, walletID string) string {
				return wagerMessage(t, playerID, walletID, "OPENING")
			},
			wantReason: sqsin.ReasonInvalidMessage,
			wantCode:   "INVALID_KIND",
		},
		{
			name: "a body that is not a message",
			body: func(_ *testing.T, playerID, _ string) string {
				return `{"messageId": "message-` + playerID + `", "type":`
			},
			wantReason: sqsin.ReasonInvalidMessage,
			wantCode:   "INVALID_REQUEST",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			playerID := idgen.V7{}.New().String()
			walletID := idgen.V7{}.New().String()
			body := tt.body(t, playerID, walletID)

			sourceID := sendToInput(t, body, provider, walletID, "message-"+playerID)

			moved := receiveDeadLetter(t, sourceID)
			if got := aws.ToString(moved.Body); got != body {
				t.Errorf("dead letter body = %q, want the body that was sent, %q", got, body)
			}
			if got := moved.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]; got != walletID {
				t.Errorf("dead letter message group = %q, want %q, the group it was sent in", got, walletID)
			}
			for name, want := range map[string]string{
				sqsin.FailureReasonAttribute: tt.wantReason,
				sqsin.FailureCodeAttribute:   tt.wantCode,
			} {
				if got := aws.ToString(moved.MessageAttributes[name].StringValue); got != want {
					t.Errorf("dead letter %s = %q, want %q", name, got, want)
				}
			}
			if _, ok := moved.MessageAttributes[sqsin.ProviderTokenAttribute]; ok {
				t.Error("the provider token was copied to the dead-letter queue")
			}
			requireInputQueueEmpty(t)
		})
	}
}

func TestApp_ResolvesPendingReference(t *testing.T) {
	internal := token(t, "wallet-service", "wallet-service-secret")
	provider := token(t, "provider-a", "provider-a-secret")
	app := newApp(t, appConfig(t, nil))
	app.RequireStart()
	defer app.RequireStop()
	api := newClient(t, "http://"+app.api.Addr())
	defer api.http.CloseIdleConnections()
	admin := newClient(t, "http://"+app.admin.Addr())
	defer admin.http.CloseIdleConnections()
	walletID, playerID := openWallet(t, api, internal)
	submit := func(kind, external, reference string) (int, map[string]any) {
		t.Helper()
		return api.call(http.MethodPost, "/wagering/transactions", provider, map[string]any{
			"providerId":                     "provider-a",
			"externalTransactionId":          external,
			"playerId":                       playerID,
			"walletId":                       walletID,
			"roundId":                        "round-1",
			"gameId":                         "game-1",
			"kind":                           kind,
			"money":                          map[string]string{"amount": "25.00", "currency": "BRL"},
			"referenceExternalTransactionId": reference,
		}, map[string]string{"Idempotency-Key": "key-" + external})
	}

	code, refund := submit("REFUND", "refund-"+playerID, "bet-"+playerID)
	if code != http.StatusAccepted || refund["status"] != "PENDING_REFERENCE" {
		t.Fatalf("refund before its bet = %d %v, want 202 PENDING_REFERENCE", code, refund)
	}
	code, bet := submit("BET", "bet-"+playerID, "")
	if code != http.StatusCreated || bet["status"] != "PROCESSED" {
		t.Fatalf("bet = %d %v, want 201 PROCESSED", code, bet)
	}

	refundID, _ := refund["transactionId"].(string)
	var resolved map[string]any
	eventually(t, "the resolution of the refund", func() bool {
		var code int
		code, resolved = api.read("/wagering/transactions/"+refundID, provider)
		if code != http.StatusOK {
			t.Fatalf("GET the refund = %d %v, want 200", code, resolved)
		}
		return resolved["status"] != "PENDING_REFERENCE"
	})
	if resolved["status"] != "PROCESSED" || resolved["referenceTransactionId"] != bet["transactionId"] {
		t.Fatalf("refund once resolved = %v, want PROCESSED, referring to the bet %v", resolved, bet["transactionId"])
	}
	if balance, _ := resolved["balance"].(map[string]any); balance["amount"] != "100.00" {
		t.Errorf("balance after the refund = %v, want 100.00", resolved["balance"])
	}
	if code, exposition := admin.get("/metrics"); code != http.StatusOK ||
		!strings.Contains(exposition, `reference_resolutions_total{result="processed"} 1`) {
		t.Errorf("GET /metrics = %d, want the refund counted as resolved:\n%s", code, exposition)
	}
}
