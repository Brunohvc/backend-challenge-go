//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
)

const internalClient = "wallet-service"

var clientSecrets = map[string]string{
	"provider-a":   "provider-a-secret",
	"provider-b":   "provider-b-secret",
	internalClient: "wallet-service-secret",
}

const tokenRenewal = time.Minute

type tokenSource struct {
	clientID string

	mu      sync.Mutex
	token   string
	expires time.Time
}

func newTokenSource(clientID string) *tokenSource {
	return &tokenSource{clientID: clientID}
}

func (s *tokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && time.Until(s.expires) > tokenRenewal {
		return s.token, nil
	}
	token, err := suite.env.KC.Token(ctx, s.clientID, clientSecrets[s.clientID])
	if err != nil {
		return "", err
	}
	expires, err := expiryOf(token)
	if err != nil {
		return "", err
	}
	s.token, s.expires = token, expires
	return token, nil
}

func expiryOf(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, errors.New("e2e: access token is not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("e2e: access token payload: %w", err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == 0 {
		return time.Time{}, fmt.Errorf("e2e: access token has no exp claim: %w", err)
	}
	return time.Unix(claims.Exp, 0), nil
}

// Money is an amount with its currency as the API writes it.
type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// String renders m as "25.00 BRL".
func (m Money) String() string {
	return m.Amount + " " + m.Currency
}

// BRL returns amount in reais.
func BRL(amount string) Money {
	return Money{Amount: amount, Currency: "BRL"}
}

// TxBody is a wagering transaction as a provider sends it.
type TxBody struct {
	ProviderID                     string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	PlayerID                       uuid.UUID  `json:"playerId"`
	WalletID                       uuid.UUID  `json:"walletId"`
	RoundID                        string     `json:"roundId"`
	GameID                         string     `json:"gameId"`
	Kind                           wager.Kind `json:"kind"`
	Money                          Money      `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
}

// Bet returns a BET of amount on w under the external id ext.
func Bet(w Wallet, ext, amount string) TxBody {
	return TxBody{
		ProviderID:            "provider-a",
		ExternalTransactionID: ext,
		PlayerID:              w.PlayerID,
		WalletID:              w.ID,
		RoundID:               "round-" + ext,
		GameID:                "fortune-chimp",
		Kind:                  wager.KindBet,
		Money:                 BRL(amount),
	}
}

// TxResponse is the answer to a submission: the outcome or the problem code.
type TxResponse struct {
	TransactionID    uuid.UUID         `json:"transactionId"`
	Status           wager.Status      `json:"status"`
	FailureCode      wager.FailureCode `json:"failureCode"`
	Balance          *Money            `json:"balance"`
	IdempotentReplay bool              `json:"idempotentReplay"`
	Code             string            `json:"-"`
	RetryAfter       string            `json:"-"`
}

// TxView is a transaction as the API reads it back.
type TxView struct {
	TransactionID          uuid.UUID         `json:"transactionId"`
	Status                 wager.Status      `json:"status"`
	Kind                   wager.Kind        `json:"kind"`
	ExternalTransactionID  string            `json:"externalTransactionId"`
	WalletID               uuid.UUID         `json:"walletId"`
	Money                  Money             `json:"money"`
	FailureCode            wager.FailureCode `json:"failureCode"`
	Balance                *Money            `json:"balance"`
	ReferenceTransactionID *uuid.UUID        `json:"referenceTransactionId"`
}

// ProviderClient sends one provider's transactions over the API and the input queue.
type ProviderClient struct {
	t          *testing.T
	providerID string
	tokens     *tokenSource
	sqs        *sqs.Client
}

// NewProviderClient returns the client of providerID.
func NewProviderClient(t *testing.T, providerID string) *ProviderClient {
	t.Helper()
	client, err := suite.env.AWS.Client(t.Context(), providerID)
	if err != nil {
		t.Fatalf("SQS client of %s: %v", providerID, err)
	}
	return &ProviderClient{t: t, providerID: providerID, tokens: newTokenSource(providerID), sqs: client}
}

// PostTx submits body to inst under key and returns the status and the answer.
func (c *ProviderClient) PostTx(ctx context.Context, inst *Instance, body TxBody, key string) (int, TxResponse) {
	c.t.Helper()
	status, resp, err := c.TryPostTx(ctx, inst.URL, body, key)
	if err != nil {
		c.t.Errorf("POST /wagering/transactions to %s: %v", inst.Name, err)
	}
	return status, resp
}

// TryPostTx submits body to baseURL and returns the status and the answer, or the error.
func (c *ProviderClient) TryPostTx(ctx context.Context, baseURL string, body TxBody, key string) (int, TxResponse, error) {
	token, err := c.tokens.Token(ctx)
	if err != nil {
		return 0, TxResponse{}, fmt.Errorf("token of %s: %w", c.providerID, err)
	}
	r, err := call(ctx, http.MethodPost, baseURL+"/wagering/transactions", token,
		map[string]string{"Idempotency-Key": key}, body)
	if err != nil {
		return 0, TxResponse{}, err
	}
	resp := TxResponse{RetryAfter: r.retryAfter}
	var problem struct {
		Code string `json:"code"`
	}
	if r.problem {
		err = json.Unmarshal(r.body, &problem)
		resp.Code = problem.Code
	} else {
		err = json.Unmarshal(r.body, &resp)
	}
	if err != nil {
		c.t.Errorf("POST /wagering/transactions to %s: decode %d %s: %v", baseURL, r.status, r.body, err)
	}
	return r.status, resp, nil
}

type inputMessage struct {
	MessageID  string          `json:"messageId"`
	Type       string          `json:"type"`
	OccurredAt event.Timestamp `json:"occurredAt"`
	Data       inputData       `json:"data"`
}

type inputData struct {
	TxBody
	IdempotencyKey string `json:"idempotencyKey"`
}

// SendTx sends body to the input queue under key as the message messageID.
func (c *ProviderClient) SendTx(ctx context.Context, body TxBody, key, messageID string) {
	c.t.Helper()
	token, err := c.tokens.Token(ctx)
	if err != nil {
		c.t.Errorf("token of %s: %v", c.providerID, err)
		return
	}
	msg, err := json.Marshal(inputMessage{
		MessageID:  messageID,
		Type:       sqsin.MessageType,
		OccurredAt: event.Timestamp(time.Now()),
		Data:       inputData{TxBody: body, IdempotencyKey: key},
	})
	if err != nil {
		c.t.Errorf("encode message %s: %v", messageID, err)
		return
	}
	_, err = c.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(suite.env.AWS.Queues.Input),
		MessageBody:            aws.String(string(msg)),
		MessageGroupId:         aws.String(body.WalletID.String()),
		MessageDeduplicationId: aws.String(messageID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			sqsin.ProviderTokenAttribute: stringAttribute(token),
			sqsin.CorrelationIDAttribute: stringAttribute(messageID),
		},
	})
	if err != nil {
		c.t.Errorf("send message %s: %v", messageID, err)
	}
}

func stringAttribute(v string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
}

// Wallet is a wallet the test opened.
type Wallet struct {
	ID       uuid.UUID
	PlayerID uuid.UUID
}

// WalletView is a wallet as the API reads it back.
type WalletView struct {
	ID      uuid.UUID `json:"id"`
	Balance Money     `json:"balance"`
	Version int64     `json:"version"`
}

// LedgerItem is a ledger entry as the API reads it back.
type LedgerItem struct {
	TransactionID uuid.UUID `json:"transactionId"`
	Direction     string    `json:"direction"`
	Amount        Money     `json:"amount"`
}

// OpenWallet opens a BRL wallet with amount through inst.
func OpenWallet(t *testing.T, inst *Instance, amount string) Wallet {
	t.Helper()
	player := idgen.V7{}.New()
	var w WalletView
	status := internalCall(t, http.MethodPost, inst.URL+"/wallets",
		map[string]any{"playerId": player, "initialBalance": BRL(amount)}, &w)
	if status != http.StatusCreated {
		t.Fatalf("open a wallet with %s through %s: status %d, want 201", amount, inst.Name, status)
	}
	return Wallet{ID: w.ID, PlayerID: player}
}

// GetWallet reads the wallet id through inst.
func GetWallet(t *testing.T, inst *Instance, id uuid.UUID) WalletView {
	t.Helper()
	var w WalletView
	if status := internalCall(t, http.MethodGet, inst.URL+"/wallets/"+id.String(), nil, &w); status != http.StatusOK {
		t.Fatalf("GET wallet %s through %s: status %d, want 200", id, inst.Name, status)
	}
	return w
}

// Ledger reads the whole ledger of the wallet id through inst, oldest entry first.
func Ledger(t *testing.T, inst *Instance, id uuid.UUID) []LedgerItem {
	t.Helper()
	var items []LedgerItem
	path := "/wallets/" + id.String() + "/ledger?limit=200"
	for {
		var page struct {
			Items      []LedgerItem `json:"items"`
			NextCursor *string      `json:"nextCursor"`
		}
		if status := internalCall(t, http.MethodGet, inst.URL+path, nil, &page); status != http.StatusOK {
			t.Fatalf("GET ledger of %s through %s: status %d, want 200", id, inst.Name, status)
		}
		items = append(items, page.Items...)
		if page.NextCursor == nil {
			return items
		}
		path = "/wallets/" + id.String() + "/ledger?limit=200&cursor=" + *page.NextCursor
	}
}

// GetTx returns the status and, on 200, the transaction of providerID under externalID.
func GetTx(t *testing.T, inst *Instance, providerID, externalID string) (int, TxView) {
	t.Helper()
	var tx TxView
	status := internalCall(t, http.MethodGet,
		inst.URL+"/providers/"+providerID+"/wagering/transactions/"+externalID, nil, &tx)
	return status, tx
}

func internalCall(t *testing.T, method, url string, body, out any) int {
	t.Helper()
	token, err := suite.internal.Token(t.Context())
	if err != nil {
		t.Fatalf("token of the back office: %v", err)
	}
	r, err := call(t.Context(), method, url, token, nil, body)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	if r.status/100 == 2 && out != nil {
		if err := json.Unmarshal(r.body, out); err != nil {
			t.Fatalf("%s %s: decode %s: %v", method, url, r.body, err)
		}
	}
	return r.status
}

type reply struct {
	status     int
	body       []byte
	problem    bool
	retryAfter string
}

func call(ctx context.Context, method, url, token string, headers map[string]string, body any) (reply, error) {
	var payload io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return reply{}, fmt.Errorf("encode body: %w", err)
		}
		payload = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, payload)
	if err != nil {
		return reply{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := suite.http.Do(req)
	if err != nil {
		return reply{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return reply{}, fmt.Errorf("read answer: %w", err)
	}
	return reply{
		status:     resp.StatusCode,
		body:       raw,
		problem:    resp.Header.Get("Content-Type") == "application/problem+json",
		retryAfter: resp.Header.Get("Retry-After"),
	}, nil
}
