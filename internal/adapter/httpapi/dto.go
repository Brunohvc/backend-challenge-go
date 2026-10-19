package httpapi

import (
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wallets"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type openWalletRequest struct {
	PlayerID       string   `json:"playerId"`
	InitialBalance moneyDTO `json:"initialBalance"`
}

type openWalletResponse struct {
	ID       uuid.UUID   `json:"id"`
	PlayerID uuid.UUID   `json:"playerId"`
	Balance  money.Money `json:"balance"`
	Version  int64       `json:"version"`
}

type walletResponse struct {
	ID        uuid.UUID       `json:"id"`
	PlayerID  uuid.UUID       `json:"playerId"`
	Balance   money.Money     `json:"balance"`
	Version   int64           `json:"version"`
	CreatedAt event.Timestamp `json:"createdAt"`
	UpdatedAt event.Timestamp `json:"updatedAt"`
}

func newWalletResponse(s wallet.Snapshot) walletResponse {
	return walletResponse{
		ID: s.ID, PlayerID: s.PlayerID, Balance: s.Balance, Version: s.Version,
		CreatedAt: event.Timestamp(s.CreatedAt), UpdatedAt: event.Timestamp(s.UpdatedAt),
	}
}

type ledgerResponse struct {
	Items      []ledgerItem `json:"items"`
	NextCursor *string      `json:"nextCursor"`
}

type ledgerItem struct {
	ID            uuid.UUID       `json:"id"`
	TransactionID uuid.UUID       `json:"transactionId"`
	Direction     string          `json:"direction"`
	Amount        money.Money     `json:"amount"`
	BalanceBefore money.Money     `json:"balanceBefore"`
	BalanceAfter  money.Money     `json:"balanceAfter"`
	CreatedAt     event.Timestamp `json:"createdAt"`
}

func newLedgerResponse(p wallets.LedgerPage) ledgerResponse {
	resp := ledgerResponse{Items: make([]ledgerItem, 0, len(p.Records))}
	for _, r := range p.Records {
		resp.Items = append(resp.Items, newLedgerItem(r))
	}
	if p.NextAfterSeq != nil {
		next := encodeCursor(*p.NextAfterSeq)
		resp.NextCursor = &next
	}
	return resp
}

func newLedgerItem(r app.LedgerRecord) ledgerItem {
	e := r.Entry
	return ledgerItem{
		ID: e.ID(), TransactionID: e.TransactionID(), Direction: string(e.Direction()),
		Amount: e.Amount(), BalanceBefore: e.BalanceBefore(), BalanceAfter: e.BalanceAfter(),
		CreatedAt: event.Timestamp(e.CreatedAt()),
	}
}

type reconciliationResponse struct {
	WalletID          uuid.UUID   `json:"walletId"`
	StoredBalance     money.Money `json:"storedBalance"`
	CalculatedBalance money.Money `json:"calculatedBalance"`
	Difference        money.Money `json:"difference"`
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int64       `json:"checkedEntries"`
}

func newReconciliationResponse(r wallets.Reconciliation) reconciliationResponse {
	return reconciliationResponse{
		WalletID: r.WalletID, StoredBalance: r.Stored, CalculatedBalance: r.Calculated,
		Difference: r.Difference, Consistent: r.Consistent, CheckedEntries: r.CheckedEntries,
	}
}

type submitRequest struct {
	ProviderID                     string   `json:"providerId"`
	ExternalTransactionID          string   `json:"externalTransactionId"`
	PlayerID                       string   `json:"playerId"`
	WalletID                       string   `json:"walletId"`
	RoundID                        string   `json:"roundId"`
	GameID                         string   `json:"gameId"`
	Kind                           string   `json:"kind"`
	Money                          moneyDTO `json:"money"`
	ReferenceExternalTransactionID string   `json:"referenceExternalTransactionId"`
}

func (r submitRequest) commandInput(key string) wager.CommandInput {
	return wager.CommandInput{
		ProviderID: r.ProviderID, ExternalTransactionID: r.ExternalTransactionID, IdempotencyKey: key,
		PlayerID: r.PlayerID, WalletID: r.WalletID, RoundID: r.RoundID, GameID: r.GameID, Kind: r.Kind,
		Amount: r.Money.Amount, Currency: r.Money.Currency, ReferenceExternalTransactionID: r.ReferenceExternalTransactionID,
	}
}

type submitResponse struct {
	TransactionID    uuid.UUID    `json:"transactionId"`
	Status           string       `json:"status"`
	FailureCode      string       `json:"failureCode,omitempty"`
	Balance          *money.Money `json:"balance,omitempty"`
	IdempotentReplay bool         `json:"idempotentReplay"`
}

func newSubmitResponse(s wager.Snapshot, replay bool) submitResponse {
	return submitResponse{
		TransactionID: s.ID, Status: string(s.Status), FailureCode: string(s.FailureCode),
		Balance: s.ResultBalance, IdempotentReplay: replay,
	}
}

type transactionResponse struct {
	TransactionID                  uuid.UUID        `json:"transactionId"`
	Status                         string           `json:"status"`
	Kind                           string           `json:"kind"`
	ProviderID                     string           `json:"providerId,omitempty"`
	ExternalTransactionID          string           `json:"externalTransactionId,omitempty"`
	WalletID                       uuid.UUID        `json:"walletId"`
	PlayerID                       uuid.UUID        `json:"playerId"`
	RoundID                        string           `json:"roundId,omitempty"`
	GameID                         string           `json:"gameId,omitempty"`
	Money                          money.Money      `json:"money"`
	ReferenceExternalTransactionID string           `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID       `json:"referenceTransactionId,omitempty"`
	FailureCode                    string           `json:"failureCode,omitempty"`
	Balance                        *money.Money     `json:"balance,omitempty"`
	WalletVersion                  int64            `json:"walletVersion,omitempty"`
	Attempts                       int              `json:"attempts"`
	NextAttemptAt                  *event.Timestamp `json:"nextAttemptAt,omitempty"`
	ExpiresAt                      *event.Timestamp `json:"expiresAt,omitempty"`
	CreatedAt                      event.Timestamp  `json:"createdAt"`
	UpdatedAt                      event.Timestamp  `json:"updatedAt"`
	ProcessedAt                    *event.Timestamp `json:"processedAt,omitempty"`
}

func newTransactionResponse(s wager.Snapshot) transactionResponse {
	resp := transactionResponse{
		TransactionID: s.ID, Status: string(s.Status), Kind: string(s.Kind),
		ProviderID: s.ProviderID, ExternalTransactionID: s.ExternalTransactionID,
		WalletID: s.WalletID, PlayerID: s.PlayerID, RoundID: s.RoundID, GameID: s.GameID, Money: s.Money,
		ReferenceExternalTransactionID: s.ReferenceExternalTransactionID, FailureCode: string(s.FailureCode),
		Balance: s.ResultBalance, WalletVersion: s.ResultWalletVersion, Attempts: s.ReferenceAttempts,
		NextAttemptAt: timestampPtr(s.NextAttemptAt), ExpiresAt: timestampPtr(s.ExpiresAt),
		CreatedAt: event.Timestamp(s.CreatedAt), UpdatedAt: event.Timestamp(s.UpdatedAt),
		ProcessedAt: timestampPtr(s.ProcessedAt),
	}
	if s.ReferenceTransactionID.Valid {
		id := s.ReferenceTransactionID.UUID
		resp.ReferenceTransactionID = &id
	}
	return resp
}

func timestampPtr(t *time.Time) *event.Timestamp {
	if t == nil {
		return nil
	}
	ts := event.Timestamp(*t)
	return &ts
}
