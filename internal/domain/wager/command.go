// Package wager is the domain of wagering transactions: commands, states and decision rules.
package wager

import (
	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
)

// CommandInput is a wagering command as received, with every field as raw text.
type CommandInput struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
}

// Command is a validated wagering command.
type Command struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          money.Money
	ReferenceExternalTransactionID string
}

// NewCommand validates in and builds a Command, reporting every invalid field.
func NewCommand(in CommandInput) (Command, error) {
	var v validator

	v.textID(pathProviderID, in.ProviderID)
	v.textID(pathExternalTransactionID, in.ExternalTransactionID)
	v.idempotencyKey(in.IdempotencyKey)
	playerID := v.uuid(pathPlayerID, in.PlayerID)
	walletID := v.uuid(pathWalletID, in.WalletID)
	v.textID(pathRoundID, in.RoundID)
	v.textID(pathGameID, in.GameID)
	kind := v.kind(in.Kind)
	amount := v.moneyFor(kind, in.Amount, in.Currency)
	v.reference(kind, in.ExternalTransactionID, in.ReferenceExternalTransactionID)

	if err := v.err(); err != nil {
		return Command{}, err
	}
	return Command{
		ProviderID:                     in.ProviderID,
		ExternalTransactionID:          in.ExternalTransactionID,
		IdempotencyKey:                 in.IdempotencyKey,
		PlayerID:                       playerID,
		WalletID:                       walletID,
		RoundID:                        in.RoundID,
		GameID:                         in.GameID,
		Kind:                           kind,
		Money:                          amount,
		ReferenceExternalTransactionID: in.ReferenceExternalTransactionID,
	}, nil
}
