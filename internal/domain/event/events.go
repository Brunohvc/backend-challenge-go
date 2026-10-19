package event

import "github.com/Brunohvc/backend-challenge-go/internal/domain/money"

// The event types, as written in the eventType field.
const (
	TypeWagerTransactionProcessed        = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         = "WagerTransactionRejected"
	TypeWalletBalanceChanged             = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

const (
	versionWagerTransactionProcessed        = 1
	versionWagerTransactionRejected         = 1
	versionWalletBalanceChanged             = 1
	versionWagerTransactionPendingReference = 1
)

// WagerTransactionProcessedData is the payload of WagerTransactionProcessed.
type WagerTransactionProcessedData struct {
	TransactionID string      `json:"transactionId"`
	WalletID      string      `json:"walletId"`
	PlayerID      string      `json:"playerId"`
	Kind          string      `json:"kind"`
	Money         money.Money `json:"money"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`

	ProviderID             string `json:"providerId,omitempty"`
	ExternalTransactionID  string `json:"externalTransactionId,omitempty"`
	RoundID                string `json:"roundId,omitempty"`
	GameID                 string `json:"gameId,omitempty"`
	ReferenceTransactionID string `json:"referenceTransactionId,omitempty"`
}

// WagerTransactionRejectedData is the payload of WagerTransactionRejected.
type WagerTransactionRejectedData struct {
	TransactionID         string      `json:"transactionId"`
	ProviderID            string      `json:"providerId"`
	ExternalTransactionID string      `json:"externalTransactionId"`
	WalletID              string      `json:"walletId"`
	Kind                  string      `json:"kind"`
	Money                 money.Money `json:"money"`
	FailureCode           string      `json:"failureCode"`
}

// WalletBalanceChangedData is the payload of WalletBalanceChanged.
type WalletBalanceChangedData struct {
	WalletID      string      `json:"walletId"`
	TransactionID string      `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
}

// WagerTransactionPendingReferenceData is the payload of WagerTransactionPendingReference.
type WagerTransactionPendingReferenceData struct {
	TransactionID                  string    `json:"transactionId"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	WalletID                       string    `json:"walletId"`
	Kind                           string    `json:"kind"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
	NextAttemptAt                  Timestamp `json:"nextAttemptAt"`
	ExpiresAt                      Timestamp `json:"expiresAt"`
}

// NewWagerTransactionProcessed builds a WagerTransactionProcessed event.
func NewWagerTransactionProcessed(m Meta, d WagerTransactionProcessedData) (Envelope, error) {
	return newEnvelope(TypeWagerTransactionProcessed, versionWagerTransactionProcessed, m, d)
}

// NewWagerTransactionRejected builds a WagerTransactionRejected event.
func NewWagerTransactionRejected(m Meta, d WagerTransactionRejectedData) (Envelope, error) {
	return newEnvelope(TypeWagerTransactionRejected, versionWagerTransactionRejected, m, d)
}

// NewWalletBalanceChanged builds a WalletBalanceChanged event.
func NewWalletBalanceChanged(m Meta, d WalletBalanceChangedData) (Envelope, error) {
	return newEnvelope(TypeWalletBalanceChanged, versionWalletBalanceChanged, m, d)
}

// NewWagerTransactionPendingReference builds a WagerTransactionPendingReference event.
func NewWagerTransactionPendingReference(m Meta, d WagerTransactionPendingReferenceData) (Envelope, error) {
	return newEnvelope(TypeWagerTransactionPendingReference, versionWagerTransactionPendingReference, m, d)
}
