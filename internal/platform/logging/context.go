package logging

import (
	"cmp"
	"context"
	"log/slog"
)

type ctxKey struct{}

// Fields are the identifiers that tie a log line to a request, message or wallet operation.
type Fields struct {
	CorrelationID string
	MessageID     string
	TransactionID string
	WalletID      string
	ProviderID    string
}

// With returns a context carrying f on top of the fields already in ctx.
func With(ctx context.Context, f Fields) context.Context {
	return context.WithValue(ctx, ctxKey{}, FieldsFrom(ctx).merge(f))
}

// FieldsFrom returns the fields carried by ctx, or the zero value if none.
func FieldsFrom(ctx context.Context) Fields {
	f, _ := ctx.Value(ctxKey{}).(Fields)
	return f
}

func (f Fields) merge(over Fields) Fields {
	return Fields{
		CorrelationID: cmp.Or(over.CorrelationID, f.CorrelationID),
		MessageID:     cmp.Or(over.MessageID, f.MessageID),
		TransactionID: cmp.Or(over.TransactionID, f.TransactionID),
		WalletID:      cmp.Or(over.WalletID, f.WalletID),
		ProviderID:    cmp.Or(over.ProviderID, f.ProviderID),
	}
}

func (f Fields) attrs() []slog.Attr {
	attrs := make([]slog.Attr, 0, 7)
	add := func(key, value string) {
		if value != "" {
			attrs = append(attrs, slog.String(key, value))
		}
	}
	add("correlationId", f.CorrelationID)
	add("messageId", f.MessageID)
	add("transactionId", f.TransactionID)
	add("walletId", f.WalletID)
	add("providerId", f.ProviderID)
	return attrs
}
