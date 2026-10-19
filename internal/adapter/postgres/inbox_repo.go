package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
)

type inboxRepo struct {
	q querier
}

var _ app.InboxRepository = inboxRepo{}

func (r repos) Inbox() app.InboxRepository { return inboxRepo{q: r.tx} }

func (r inboxRepo) Claim(ctx context.Context, consumer, messageID, hash string, now time.Time) (app.InboxClaim, error) {
	tag, err := r.q.Exec(ctx,
		`INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		consumer, messageID, hash, now)
	if err != nil {
		return app.InboxClaim{}, fmt.Errorf("postgres: claim inbox message %q of %s: %w", messageID, consumer, classify(err))
	}
	if tag.RowsAffected() == 1 {
		return app.InboxClaim{New: true}, nil
	}

	var c app.InboxClaim
	err = r.q.QueryRow(ctx,
		`SELECT payload_hash, transaction_id
		   FROM inbox_messages
		  WHERE consumer_name = $1 AND message_id = $2`,
		consumer, messageID).Scan(&c.ExistingHash, &c.TransactionID)
	if err != nil {
		return app.InboxClaim{}, fmt.Errorf("postgres: read inbox message %q of %s: %w", messageID, consumer, classify(err))
	}
	return c, nil
}

func (r inboxRepo) Complete(ctx context.Context, consumer, messageID string, txID uuid.NullUUID, outcome string, now time.Time) error {
	tag, err := r.q.Exec(ctx,
		`UPDATE inbox_messages
		    SET processed_at = $3, transaction_id = $4, outcome = $5
		  WHERE consumer_name = $1 AND message_id = $2 AND processed_at IS NULL`,
		consumer, messageID, now, txID, outcome)
	if err != nil {
		return fmt.Errorf("postgres: complete inbox message %q of %s: %w", messageID, consumer, classify(err))
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres: complete inbox message %q of %s: %w: it was not claimed or is completed already",
			messageID, consumer, app.ErrPermanent)
	}
	return nil
}
