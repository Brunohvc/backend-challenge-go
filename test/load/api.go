//go:build load

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const internalClient = "wallet-service"

type txBody struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Money                          money  `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

type submitReply struct {
	status        int
	code          string
	retryAfter    bool
	transactionID string
	txStatus      string
	failureCode   string
	replay        bool
	took          time.Duration
}

type txView struct {
	TransactionID string `json:"transactionId"`
	Status        string `json:"status"`
	Kind          string `json:"kind"`
	FailureCode   string `json:"failureCode"`
}

type reconciliation struct {
	StoredBalance  money `json:"storedBalance"`
	Consistent     bool  `json:"consistent"`
	CheckedEntries int64 `json:"checkedEntries"`
}

type ledgerItem struct {
	TransactionID string `json:"transactionId"`
	Direction     string `json:"direction"`
	Amount        money  `json:"amount"`
	BalanceBefore money  `json:"balanceBefore"`
	BalanceAfter  money  `json:"balanceAfter"`
}

type apiClient struct {
	base     string
	http     *http.Client
	tokens   map[string]*tokenSource
	internal *tokenSource
}

type reply struct {
	status     int
	body       []byte
	problem    bool
	retryAfter bool
	took       time.Duration
}

func (c *apiClient) call(ctx context.Context, method, path, token string, headers map[string]string, body any) (reply, error) {
	var payload io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return reply{}, fmt.Errorf("encode body: %w", err)
		}
		payload = bytes.NewReader(b)
	}
	header := http.Header{}
	if body != nil {
		header.Set("Content-Type", "application/json")
	}
	if token != "" {
		header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		header.Set(k, v)
	}
	started := time.Now()
	resp, err := send(ctx, c.http, method, c.base+path, header, payload)
	if err != nil {
		return reply{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return reply{}, fmt.Errorf("read answer of %s %s: %w", method, path, err)
	}
	return reply{
		status:     resp.StatusCode,
		body:       raw,
		problem:    strings.HasPrefix(resp.Header.Get("Content-Type"), "application/problem+json"),
		retryAfter: resp.Header.Get("Retry-After") != "",
		took:       time.Since(started),
	}, nil
}

func (c *apiClient) submit(ctx context.Context, body txBody, key, correlationID string) (submitReply, error) {
	token, err := c.tokens[body.ProviderID].Token(ctx)
	if err != nil {
		return submitReply{}, err
	}
	r, err := c.call(ctx, http.MethodPost, "/wagering/transactions", token,
		map[string]string{"Idempotency-Key": key, "X-Correlation-Id": correlationID}, body)
	if err != nil {
		return submitReply{}, err
	}
	out := submitReply{status: r.status, retryAfter: r.retryAfter, took: r.took}
	if r.problem {
		var p struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal(r.body, &p); err != nil {
			return out, fmt.Errorf("decode problem %d: %w", r.status, err)
		}
		out.code = p.Code
		return out, nil
	}
	var res struct {
		TransactionID    string `json:"transactionId"`
		Status           string `json:"status"`
		FailureCode      string `json:"failureCode"`
		IdempotentReplay bool   `json:"idempotentReplay"`
	}
	if err := json.Unmarshal(r.body, &res); err != nil {
		return out, fmt.Errorf("decode answer %d: %w", r.status, err)
	}
	out.transactionID, out.txStatus, out.failureCode, out.replay = res.TransactionID, res.Status, res.FailureCode, res.IdempotentReplay
	return out, nil
}

func (c *apiClient) internalCall(ctx context.Context, method, path string, body, out any, want int) (int, error) {
	token, err := c.internal.Token(ctx)
	if err != nil {
		return 0, err
	}
	r, err := c.call(ctx, method, path, token, nil, body)
	if err != nil {
		return 0, err
	}
	if r.status != want {
		return r.status, nil
	}
	if out != nil {
		if err := json.Unmarshal(r.body, out); err != nil {
			return r.status, fmt.Errorf("decode %s %s: %w", method, path, err)
		}
	}
	return r.status, nil
}

func (c *apiClient) openWallet(ctx context.Context, playerID string, initial int64) (string, error) {
	var w struct {
		ID string `json:"id"`
	}
	req := map[string]any{"playerId": playerID, "initialBalance": brl(initial)}
	status, err := c.internalCall(ctx, http.MethodPost, "/wallets", req, &w, http.StatusCreated)
	if err != nil {
		return "", err
	}
	if status != http.StatusCreated {
		return "", fmt.Errorf("open wallet: status %d, want 201", status)
	}
	return w.ID, nil
}

func (c *apiClient) transactionByExternalID(ctx context.Context, provider, ext string) (txView, bool, error) {
	var v txView
	path := "/providers/" + url.PathEscape(provider) + "/wagering/transactions/" + url.PathEscape(ext)
	status, err := c.internalCall(ctx, http.MethodGet, path, nil, &v, http.StatusOK)
	switch {
	case err != nil:
		return txView{}, false, err
	case status == http.StatusNotFound:
		return txView{}, false, nil
	case status != http.StatusOK:
		return txView{}, false, fmt.Errorf("GET %s: status %d", path, status)
	}
	return v, true, nil
}

func (c *apiClient) reconcile(ctx context.Context, walletID string) (reconciliation, error) {
	var r reconciliation
	status, err := c.internalCall(ctx, http.MethodPost, "/wallets/"+walletID+"/reconciliation", nil, &r, http.StatusOK)
	if err != nil {
		return reconciliation{}, err
	}
	if status != http.StatusOK {
		return reconciliation{}, fmt.Errorf("reconcile wallet %s: status %d", walletID, status)
	}
	return r, nil
}

func (c *apiClient) ledger(ctx context.Context, walletID string) ([]ledgerItem, error) {
	var items []ledgerItem
	cursor := ""
	for {
		path := "/wallets/" + walletID + "/ledger?limit=200"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		var page struct {
			Items      []ledgerItem `json:"items"`
			NextCursor *string      `json:"nextCursor"`
		}
		status, err := c.internalCall(ctx, http.MethodGet, path, nil, &page, http.StatusOK)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("ledger of wallet %s: status %d", walletID, status)
		}
		items = append(items, page.Items...)
		if page.NextCursor == nil {
			return items, nil
		}
		cursor = *page.NextCursor
	}
}

func (c *apiClient) ready(ctx context.Context) error {
	r, err := c.call(ctx, http.MethodGet, "/health/ready", "", nil, nil)
	if err != nil {
		return err
	}
	if r.status != http.StatusOK {
		return fmt.Errorf("readiness: status %d: %s", r.status, r.body)
	}
	return nil
}

var errNotReady = errors.New("stack not ready")

func send(ctx context.Context, client *http.Client, method, target string, header http.Header, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, body) //nolint:gosec // targets come from the operator's flags
	if err != nil {
		return nil, err
	}
	if header != nil {
		req.Header = header
	}
	return client.Do(req) //nolint:gosec // targets come from the operator's flags
}
