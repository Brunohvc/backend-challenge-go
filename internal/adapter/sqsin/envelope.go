// Package sqsin consumes the wagering commands that providers send to the input queue.
package sqsin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

// MessageType is the only type of message the input queue carries.
const MessageType = "WagerTransactionRequested"

const occurredAtLayout = "2006-01-02T15:04:05.000Z"

const maxEchoedKeyBytes = 64

var messageIDPattern = regexp.MustCompile(`^[\x21-\x7E]{1,255}$`)

var (
	envelopeFields = []string{"messageId", "type", "occurredAt", "data"}
	dataFields     = []string{
		"providerId", "externalTransactionId", "idempotencyKey", "playerId", "walletId",
		"roundId", "gameId", "kind", "money", "referenceExternalTransactionId",
	}
	moneyFields = []string{"amount", "currency"}
)

// Envelope is a parsed input message.
type Envelope struct {
	MessageID string
	Type      string
	Data      map[string]json.RawMessage
}

// InvalidMessageError reports a message that is not in the documented shape.
type InvalidMessageError struct {
	Reason string
}

func (e *InvalidMessageError) Error() string {
	return "sqsin: invalid message: " + e.Reason
}

func invalid(format string, args ...any) error {
	return &InvalidMessageError{Reason: fmt.Sprintf(format, args...)}
}

// ParseEnvelope parses body strictly into an Envelope and the command input it carries.
func ParseEnvelope(body []byte) (Envelope, wager.CommandInput, error) {
	top, err := object(body, "", envelopeFields)
	if err != nil {
		return Envelope{}, wager.CommandInput{}, err
	}

	var env Envelope
	for _, f := range []struct {
		name string
		dst  *string
	}{{"messageId", &env.MessageID}, {"type", &env.Type}} {
		raw, ok := top[f.name]
		if !ok {
			return Envelope{}, wager.CommandInput{}, invalid("%s is required", f.name)
		}
		if *f.dst, err = str(raw, f.name); err != nil {
			return Envelope{}, wager.CommandInput{}, err
		}
	}
	if !messageIDPattern.MatchString(env.MessageID) {
		return Envelope{}, wager.CommandInput{}, invalid("messageId must be 1 to 255 printable ASCII characters")
	}
	if env.Type != MessageType {
		return Envelope{}, wager.CommandInput{}, invalid("type must be %s", MessageType)
	}
	if err = validateOccurredAt(top); err != nil {
		return Envelope{}, wager.CommandInput{}, err
	}

	raw, ok := top["data"]
	if !ok {
		return Envelope{}, wager.CommandInput{}, invalid("data is required")
	}
	if env.Data, err = object(raw, "data", dataFields); err != nil {
		return Envelope{}, wager.CommandInput{}, err
	}
	in, err := commandInput(env.Data)
	if err != nil {
		return Envelope{}, wager.CommandInput{}, err
	}
	return env, in, nil
}

// InboxHash returns the fingerprint the inbox stores for a message.
func InboxHash(env Envelope) (string, error) {
	data := make(map[string]any, len(env.Data))
	for k, raw := range env.Data {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return "", fmt.Errorf("sqsin: inbox hash: decode data.%s: %w", k, err)
		}
		data[k] = v
	}
	h, err := wager.CanonicalHash(map[string]any{"type": env.Type, "data": data})
	if err != nil {
		return "", fmt.Errorf("sqsin: inbox hash: %w", err)
	}
	return h, nil
}

func object(raw []byte, path string, allowed []string) (map[string]json.RawMessage, error) {
	what := "message"
	if path != "" {
		what = path
	}
	notObject := func() error {
		if path == "" {
			return invalid("not a JSON object")
		}
		return invalid("%s must be an object", what)
	}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, notObject()
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	var m map[string]json.RawMessage
	if err := dec.Decode(&m); err != nil {
		return nil, notObject()
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return nil, notObject()
	}
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if !slices.Contains(allowed, k) {
			return nil, invalid("unknown field %q", truncate(join(path, k), maxEchoedKeyBytes))
		}
	}
	return m, nil
}

func str(raw json.RawMessage, path string) (string, error) {
	var s string
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '"' || json.Unmarshal(trimmed, &s) != nil {
		return "", invalid("%s must be a string", path)
	}
	return s, nil
}

func validateOccurredAt(top map[string]json.RawMessage) error {
	raw, ok := top["occurredAt"]
	if !ok {
		return invalid("occurredAt is required")
	}
	s, err := str(raw, "occurredAt")
	if err != nil {
		return err
	}
	if t, err := time.Parse(occurredAtLayout, s); err != nil || t.Format(occurredAtLayout) != s {
		return invalid("occurredAt must be a UTC timestamp like 2006-01-02T15:04:05.000Z")
	}
	return nil
}

func commandInput(data map[string]json.RawMessage) (wager.CommandInput, error) {
	var in wager.CommandInput
	fields := map[string]*string{
		"providerId":                     &in.ProviderID,
		"externalTransactionId":          &in.ExternalTransactionID,
		"idempotencyKey":                 &in.IdempotencyKey,
		"playerId":                       &in.PlayerID,
		"walletId":                       &in.WalletID,
		"roundId":                        &in.RoundID,
		"gameId":                         &in.GameID,
		"kind":                           &in.Kind,
		"referenceExternalTransactionId": &in.ReferenceExternalTransactionID,
	}
	for _, k := range slices.Sorted(maps.Keys(fields)) {
		raw, ok := data[k]
		if !ok {
			continue
		}
		s, err := str(raw, join("data", k))
		if err != nil {
			return wager.CommandInput{}, err
		}
		*fields[k] = s
	}

	raw, ok := data["money"]
	if !ok {
		return in, nil
	}
	m, err := object(raw, "data.money", moneyFields)
	if err != nil {
		return wager.CommandInput{}, err
	}
	for _, f := range []struct {
		name string
		dst  *string
	}{{"amount", &in.Amount}, {"currency", &in.Currency}} {
		raw, ok := m[f.name]
		if !ok {
			continue
		}
		if *f.dst, err = str(raw, join("data.money", f.name)); err != nil {
			return wager.CommandInput{}, err
		}
	}
	return in, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func join(path, field string) string {
	return strings.TrimPrefix(path+"."+field, ".")
}
