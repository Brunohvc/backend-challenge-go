//go:build integration

package wagering_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

const consumer = "wager-transactions"

func fromQueue(messageID, payload string) wagering.Source {
	return wagering.Source{
		Channel:       wagering.ChannelSQS,
		CorrelationID: "corr-" + messageID,
		CausationID:   messageID,
		Inbox:         &wagering.InboxMessage{Consumer: consumer, MessageID: messageID, Hash: messageHash(payload)},
	}
}

func messageHash(payload string) string {
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func submitMessage(t *testing.T, svc *wagering.Service, in wager.CommandInput, src wagering.Source) wagering.Result {
	t.Helper()
	res, err := svc.Submit(t.Context(), mustCommand(t, in), src)
	if err != nil {
		t.Fatalf("Submit(%s %s) from message %s error = %v", in.Kind, in.Amount, src.Inbox.MessageID, err)
	}
	return res
}

type inboxRow struct {
	Hash        string
	ReceivedAt  time.Time
	ProcessedAt *time.Time
	TxID        uuid.NullUUID
	Outcome     *string
}

func inboxMessage(t *testing.T, db *pgx.Conn, messageID string) (inboxRow, bool) {
	t.Helper()
	var r inboxRow
	err := db.QueryRow(t.Context(),
		`SELECT payload_hash, received_at, processed_at, transaction_id, outcome
		   FROM inbox_messages
		  WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).
		Scan(&r.Hash, &r.ReceivedAt, &r.ProcessedAt, &r.TxID, &r.Outcome)
	if errors.Is(err, pgx.ErrNoRows) {
		return inboxRow{}, false
	}
	if err != nil {
		t.Fatalf("read inbox message %s: %v", messageID, err)
	}
	return r, true
}

func requireCompleted(t *testing.T, db *pgx.Conn, messageID string, txID uuid.UUID, outcome string) inboxRow {
	t.Helper()
	r, found := inboxMessage(t, db, messageID)
	switch {
	case !found:
		t.Fatalf("message %s is not in the inbox", messageID)
	case r.Outcome == nil || *r.Outcome != outcome || r.TxID != (uuid.NullUUID{UUID: txID, Valid: true}):
		t.Fatalf("message %s completed with %v in %v, want %s in %s", messageID, deref(r.Outcome), r.TxID, outcome, txID)
	case !r.ReceivedAt.Equal(t0) || r.ProcessedAt == nil || !r.ProcessedAt.Equal(t0):
		t.Fatalf("message %s received at %v and completed at %v, want both at %v", messageID, r.ReceivedAt, deref(r.ProcessedAt), t0)
	}
	return r
}

func requireNotInInbox(t *testing.T, db *pgx.Conn, messageID string) {
	t.Helper()
	if r, found := inboxMessage(t, db, messageID); found {
		t.Fatalf("message %s is in the inbox (%+v), want it left out", messageID, r)
	}
}

func TestSubmit_InboxDuplicate(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	in := input(w, "BET", "25.00")
	src := fromQueue("msg-1", "bet of 25.00")
	first := submitMessage(t, h.svc, in, src).Transaction

	again := submitMessage(t, h.svc, in, src)

	if !again.DuplicateMessage || again.Replay {
		t.Fatalf("result = %+v, want a duplicate message", again)
	}
	if again.Transaction.ID != first.ID {
		t.Fatalf("duplicate answered with transaction %s, want %s", again.Transaction.ID, first.ID)
	}
	requireOutcome(t, again.Transaction, wager.StatusProcessed, "")
	requireResultBalance(t, again.Transaction, "75.00")

	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)
	if n := countRows(t, db, "SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'"); n != 1 {
		t.Fatalf("%d transactions stored, want 1", n)
	}
	requireEvents(t, db, first, "msg-1", event.TypeWagerTransactionProcessed, event.TypeWalletBalanceChanged)
	requireCompleted(t, db, "msg-1", first.ID, "PROCESSED")

	txs, duplicates, _ := h.metrics.snapshot()
	if want := []recordedTransaction{{"sqs", "BET", "PROCESSED", ""}}; !slices.Equal(txs, want) {
		t.Fatalf("transaction metrics = %v, want %v", txs, want)
	}
	if want := []string{"sqs/inbox"}; !slices.Equal(duplicates, want) {
		t.Fatalf("duplicates = %v, want %v", duplicates, want)
	}
}

func TestSubmit_InboxDuplicateDoesNotWaitForTheWalletLock(t *testing.T) {
	h := newHarness(t)
	w := openWallet(t, h.uow, "100.00")
	in, src := input(w, "BET", "25.00"), fromQueue("msg-1", "bet of 25.00")
	first := submitMessage(t, h.svc, in, src).Transaction
	lockWallet(t, w.ID())

	start := time.Now()
	again := submitMessage(t, h.svc, in, src)

	if took, timeout := time.Since(start), dbConfig().LockTimeout; took >= timeout/4 {
		t.Fatalf("redelivery took %s while the wallet was locked, want it answered well within the lock timeout of %s", took, timeout)
	}
	if !again.DuplicateMessage || again.Transaction.ID != first.ID {
		t.Fatalf("result = %+v, want the duplicate of %s", again, first.ID)
	}
}

func TestSubmit_InboxMessageIDReused(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	first := submitMessage(t, h.svc, input(w, "BET", "25.00"), fromQueue("msg-1", "bet of 25.00")).Transaction

	res, err := h.svc.Submit(t.Context(), mustCommand(t, input(w, "BET", "30.00")), fromQueue("msg-1", "bet of 30.00"))

	if !errors.Is(err, wagering.ErrMessageIDReused) {
		t.Fatalf("Submit() = %+v, %v; want ErrMessageIDReused", res, err)
	}
	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)
	if n := countRows(t, db, "SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'"); n != 1 {
		t.Fatalf("%d transactions stored, want 1", n)
	}
	if r := requireCompleted(t, db, "msg-1", first.ID, "PROCESSED"); r.Hash != messageHash("bet of 25.00") {
		t.Fatalf("inbox hash = %s, want the one the message was first received with", r.Hash)
	}
	if txs, duplicates, _ := h.metrics.snapshot(); len(txs) != 1 || len(duplicates) != 0 {
		t.Fatalf("transactions = %v, duplicates = %v; want the first decision only", txs, duplicates)
	}
}

func TestSubmit_InboxReplayRecordsReplay(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	in := input(w, "BET", "25.00")
	first := submitMessage(t, h.svc, in, fromQueue("msg-1", "bet of 25.00")).Transaction

	again := submitMessage(t, h.svc, in, fromQueue("msg-2", "bet of 25.00"))

	if !again.Replay || again.DuplicateMessage || again.Transaction.ID != first.ID {
		t.Fatalf("result = %+v, want the replay of %s", again, first.ID)
	}
	requireCompleted(t, db, "msg-1", first.ID, "PROCESSED")
	requireCompleted(t, db, "msg-2", first.ID, "REPLAY")
	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)

	txs, duplicates, _ := h.metrics.snapshot()
	if len(txs) != 1 || !slices.Equal(duplicates, []string{"sqs/idempotency"}) {
		t.Fatalf("transactions = %v, duplicates = %v; want 1 and one idempotency duplicate", txs, duplicates)
	}
}

func TestSubmit_InboxAndDomainAtomic(t *testing.T) {
	errOutbox := fmt.Errorf("%w: the outbox refused the event", app.ErrPermanent)
	fault := &failingOutbox{}
	h := newHarness(t, withUnitOfWork(func(u app.UnitOfWork) app.UnitOfWork {
		fault.UnitOfWork = u
		return fault
	}))
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	in, src := input(w, "BET", "25.00"), fromQueue("msg-1", "bet of 25.00")
	fault.arm(errOutbox)

	_, err := h.svc.Submit(t.Context(), mustCommand(t, in), src)

	if !errors.Is(err, errOutbox) {
		t.Fatalf("Submit() error = %v, want the outbox failure", err)
	}
	requireNotInInbox(t, db, "msg-1")
	if n := countRows(t, db, "SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'"); n != 0 {
		t.Fatalf("%d transactions stored, want none", n)
	}
	requireWallet(t, h.uow, w.ID(), "100.00", 1)
	requireLedger(t, h.uow, w.ID())
	if n := countRows(t, db, "SELECT count(*) FROM outbox_events"); n != 0 {
		t.Fatalf("%d events stored, want none", n)
	}

	fault.disarm()
	again := submitMessage(t, h.svc, in, src)

	if again.DuplicateMessage || again.Replay {
		t.Fatalf("redelivery = %+v, want it decided as a new message", again)
	}
	tx := again.Transaction
	requireOutcome(t, tx, wager.StatusProcessed, "")
	requireCompleted(t, db, "msg-1", tx.ID, "PROCESSED")
	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)
	requireEvents(t, db, tx, "msg-1", event.TypeWagerTransactionProcessed, event.TypeWalletBalanceChanged)
}

func TestSubmit_InboxPendingCompletes(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")

	tx := submitMessage(t, h.svc, waitingFor(w, "REFUND", "25.00", "bet-not-here-yet"), fromQueue("msg-1", "refund")).Transaction

	requireOutcome(t, tx, wager.StatusPendingReference, "")
	requireCompleted(t, db, "msg-1", tx.ID, "PENDING_REFERENCE")
	if tx.CorrelationID != "corr-msg-1" {
		t.Fatalf("correlation id = %q, want the one of the message", tx.CorrelationID)
	}
	requireEvents(t, db, tx, "msg-1", event.TypeWagerTransactionPendingReference)
}

func TestSubmit_InboxLeavesRefusedMessagesOut(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	in := input(w, "BET", "25.00")
	in.WalletID = newID().String()

	_, err := h.svc.Submit(t.Context(), mustCommand(t, in), fromQueue("msg-1", "bet of 25.00"))

	if !errors.Is(err, wagering.ErrWalletNotFound) {
		t.Fatalf("Submit() error = %v, want ErrWalletNotFound", err)
	}
	requireNotInInbox(t, db, "msg-1")
}
