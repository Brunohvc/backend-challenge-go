package wager

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// PayloadHash returns the SHA-256 hex of the canonical JSON of the command.
func (c Command) PayloadHash() string {
	payload := map[string]any{
		"providerId":            c.ProviderID,
		"externalTransactionId": c.ExternalTransactionID,
		"playerId":              c.PlayerID.String(),
		"walletId":              c.WalletID.String(),
		"roundId":               c.RoundID,
		"gameId":                c.GameID,
		"kind":                  string(c.Kind),
		"money": map[string]any{
			"amount":   c.Money.String(),
			"currency": c.Money.Currency().Code(),
		},
	}
	if c.ReferenceExternalTransactionID != "" {
		payload["referenceExternalTransactionId"] = c.ReferenceExternalTransactionID
	}

	hash, err := CanonicalHash(payload)
	if err != nil {
		panic(fmt.Errorf("wager: encode canonical payload: %w", err))
	}
	return hash
}

// CanonicalHash returns the SHA-256 hex of v encoded as canonical JSON.
func CanonicalHash(v map[string]any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", fmt.Errorf("encode canonical json: %w", err)
	}
	sum := sha256.Sum256(bytes.TrimSuffix(buf.Bytes(), []byte{'\n'}))
	return hex.EncodeToString(sum[:]), nil
}
