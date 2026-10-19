//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const msgConsumer = "wager-transactions"

var errMsgRollback = errors.New("roll back")

func msgHash(b byte) string { return strings.Repeat(fmt.Sprintf("%02x", b), 32) }

func msgMessageID() string { return "msg-" + newID().String() }

func msgOpening(t *testing.T, uow app.UnitOfWork) uuid.NullUUID {
	t.Helper()
	_, opening := openWallet(t, uow, 1000)
	return uuid.NullUUID{UUID: opening.TransactionID(), Valid: true}
}

func msgTryInboxClaim(ctx context.Context, uow app.UnitOfWork, consumer, messageID, hash string) (app.InboxClaim, error) {
	var c app.InboxClaim
	err := uow.Do(ctx, func(ctx context.Context, r app.Repos) error {
		var err error
		c, err = r.Inbox().Claim(ctx, consumer, messageID, hash, t0)
		return err
	})
	return c, err
}

func msgInboxClaim(t *testing.T, uow app.UnitOfWork, consumer, messageID, hash string) app.InboxClaim {
	t.Helper()
	c, err := msgTryInboxClaim(t.Context(), uow, consumer, messageID, hash)
	if err != nil {
		t.Fatalf("Claim(%s, %s) error = %v", consumer, messageID, err)
	}
	return c
}

func msgProcess(t *testing.T, uow app.UnitOfWork, messageID string, txID uuid.NullUUID, outcome string) {
	t.Helper()
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		c, err := r.Inbox().Claim(ctx, msgConsumer, messageID, msgHash(1), t0)
		if err != nil {
			return err
		}
		if c != (app.InboxClaim{New: true}) {
			return fmt.Errorf("first claim of %s = %+v, want it new", messageID, c)
		}
		return r.Inbox().Complete(ctx, msgConsumer, messageID, txID, outcome, t1)
	})
	if err != nil {
		t.Fatalf("process %s: %v", messageID, err)
	}
}

func msgHoldInboxClaim(t *testing.T, uow app.UnitOfWork, messageID string, txID uuid.NullUUID) (end func(commit bool)) {
	t.Helper()
	claimed, decision := make(chan struct{}), make(chan bool, 1)
	done := make(chan error, 1)
	go func() {
		done <- uow.Do(context.WithoutCancel(t.Context()), func(ctx context.Context, r app.Repos) error {
			if _, err := r.Inbox().Claim(ctx, msgConsumer, messageID, msgHash(1), t0); err != nil {
				return err
			}
			if err := r.Inbox().Complete(ctx, msgConsumer, messageID, txID, "PROCESSED", t1); err != nil {
				return err
			}
			close(claimed)
			if !<-decision {
				return errMsgRollback
			}
			return nil
		})
	}()
	select {
	case <-claimed:
	case err := <-done:
		t.Fatalf("claim %s: %v", messageID, err)
	}

	var once sync.Once
	end = func(commit bool) {
		once.Do(func() {
			decision <- commit
			if err := <-done; err != nil && !errors.Is(err, errMsgRollback) {
				t.Errorf("unit of work holding the claim of %s: %v", messageID, err)
			}
		})
	}
	t.Cleanup(func() { end(false) })
	return end
}

func msgAwaitLockWait(t *testing.T, db execer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for queryInt64(t, db, "SELECT count(*) FROM pg_locks WHERE NOT granted") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no session waited for a lock within 5s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type msgInboxRow struct {
	hash        string
	receivedAt  time.Time
	processedAt *time.Time
	txID        uuid.NullUUID
	outcome     *string
}

func msgReadMessage(t *testing.T, db execer, consumer, messageID string) msgInboxRow {
	t.Helper()
	var r msgInboxRow
	err := db.QueryRow(t.Context(),
		`SELECT payload_hash, received_at, processed_at, transaction_id, outcome
		   FROM inbox_messages
		  WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).
		Scan(&r.hash, &r.receivedAt, &r.processedAt, &r.txID, &r.outcome)
	if err != nil {
		t.Fatalf("read inbox message %s of %s: %v", messageID, consumer, err)
	}
	return r
}

func TestInbox_ClaimNewThenDuplicate(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	txID := msgOpening(t, uow)
	processed, rejected := msgMessageID(), msgMessageID()

	msgProcess(t, uow, processed, txID, "PROCESSED")
	r := msgReadMessage(t, owner, msgConsumer, processed)
	if r.hash != msgHash(1) || !r.receivedAt.Equal(t0) || r.processedAt == nil || !r.processedAt.Equal(t1) ||
		r.txID != txID || r.outcome == nil || *r.outcome != "PROCESSED" {
		t.Fatalf("stored %+v, want received at t0, completed at t1 with %v and PROCESSED", r, txID.UUID)
	}

	want := app.InboxClaim{ExistingHash: msgHash(1), TransactionID: txID}
	for _, hash := range []string{msgHash(1), msgHash(2)} {
		if got := msgInboxClaim(t, uow, msgConsumer, processed, hash); got != want {
			t.Fatalf("Claim(%s) with hash %s = %+v, want %+v", processed, hash[:4], got, want)
		}
	}
	if r := msgReadMessage(t, owner, msgConsumer, processed); r.hash != msgHash(1) || !r.receivedAt.Equal(t0) {
		t.Fatalf("a duplicate changed what was received: %+v", r)
	}

	msgProcess(t, uow, rejected, uuid.NullUUID{}, "REJECTED")
	if got := msgInboxClaim(t, uow, msgConsumer, rejected, msgHash(1)); got != (app.InboxClaim{ExistingHash: msgHash(1)}) {
		t.Fatalf("Claim(%s) = %+v, want a duplicate without a transaction", rejected, got)
	}

	if got := msgInboxClaim(t, uow, "another-consumer", processed, msgHash(1)); got != (app.InboxClaim{New: true}) {
		t.Fatalf("Claim(%s) for another consumer = %+v, want it new", processed, got)
	}
}

func TestInbox_ConcurrentClaimWaitsForTheFirst(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	txID := msgOpening(t, uow)

	for _, tc := range []struct {
		name   string
		commit bool
		want   app.InboxClaim
	}{
		{"the first commits: a duplicate", true, app.InboxClaim{ExistingHash: msgHash(1), TransactionID: txID}},
		{"the first rolls back: taken on", false, app.InboxClaim{New: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			messageID := msgMessageID()
			end := msgHoldInboxClaim(t, uow, messageID, txID)

			type result struct {
				claim app.InboxClaim
				err   error
			}
			second := make(chan result, 1)
			go func() {
				c, err := msgTryInboxClaim(context.WithoutCancel(t.Context()), uow, msgConsumer, messageID, msgHash(1))
				second <- result{c, err}
			}()
			msgAwaitLockWait(t, owner)
			end(tc.commit)

			got := <-second
			if got.err != nil {
				t.Fatalf("second Claim() error = %v", got.err)
			}
			if got.claim != tc.want {
				t.Fatalf("second Claim() = %+v, want %+v", got.claim, tc.want)
			}
		})
	}
}

func TestInbox_ClaimWaitIsBounded(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	impatientCfg := dbConfig()
	impatientCfg.LockTimeout = 100 * time.Millisecond
	impatient := postgres.NewUnitOfWork(newPool(t, impatientCfg), tracing.Tracer{})
	messageID := msgMessageID()
	end := msgHoldInboxClaim(t, uow, messageID, msgOpening(t, uow))

	_, err := msgTryInboxClaim(t.Context(), impatient, msgConsumer, messageID, msgHash(1))
	requireClass(t, err, app.ErrRetryable)
	requireSQLState(t, err, "55P03", "")
	end(true)
}

func TestInbox_CompleteIsFinal(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	txID := msgOpening(t, uow)
	messageID := msgMessageID()

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		if _, err := r.Inbox().Claim(ctx, msgConsumer, messageID, msgHash(1), t0); err != nil {
			return err
		}
		if err := r.Inbox().Complete(ctx, msgConsumer, messageID, txID, "PROCESSED", t1); err != nil {
			return err
		}
		err := r.Inbox().Complete(ctx, msgConsumer, messageID, uuid.NullUUID{}, "REJECTED", t1.Add(time.Second))
		requireClass(t, err, app.ErrPermanent)
		return nil
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}

	for name, call := range map[string]func(context.Context, app.InboxRepository) error{
		"completed in an earlier unit of work": func(ctx context.Context, in app.InboxRepository) error {
			return in.Complete(ctx, msgConsumer, messageID, txID, "REPLAY", t1)
		},
		"never claimed": func(ctx context.Context, in app.InboxRepository) error {
			return in.Complete(ctx, msgConsumer, msgMessageID(), txID, "PROCESSED", t1)
		},
		"an outcome the inbox does not know": func(ctx context.Context, in app.InboxRepository) error {
			id := msgMessageID()
			if _, err := in.Claim(ctx, msgConsumer, id, msgHash(1), t0); err != nil {
				return err
			}
			return in.Complete(ctx, msgConsumer, id, txID, "LOST", t1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error { return call(ctx, r.Inbox()) })
			requireClass(t, err, app.ErrPermanent)
		})
	}

	r := msgReadMessage(t, owner, msgConsumer, messageID)
	if r.processedAt == nil || !r.processedAt.Equal(t1) || r.txID != txID || r.outcome == nil || *r.outcome != "PROCESSED" {
		t.Fatalf("the first completion changed: %+v", r)
	}
}
