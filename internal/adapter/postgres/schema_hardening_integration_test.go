//go:build integration

package postgres_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
)

const (
	hardeningEntryTrigger = "ledger_entry_matches_transaction"
	hardeningTxTrigger    = "processed_transaction_has_entry"
	hardeningInboxGuard   = "inbox_guard"
)

func hardeningMove(t *testing.T, tx pgx.Tx, w, movement row, dir string) {
	t.Helper()
	entry := ledgerRow(movement, dir, get[int64](w, "balance_minor"))
	mustInsert(t, tx, "wager_transactions", movement)
	mustInsert(t, tx, "wallet_ledger_entries", entry)
	mustExec(t, tx, "UPDATE wallets SET balance_minor = $2, version = version + 1 WHERE id = $1",
		w["id"], entry["balance_after_minor"])
}

func hardeningCommit(t *testing.T, conn *pgx.Conn, w, movement row, dir string) error {
	t.Helper()
	tx := begin(t, conn)
	hardeningMove(t, tx, w, movement, dir)
	return tx.Commit(t.Context())
}

func hardeningRecord(t *testing.T, conn *pgx.Conn, w, movement row, dir string) row {
	t.Helper()
	if err := hardeningCommit(t, conn, w, movement, dir); err != nil {
		t.Fatalf("record %s: %v", get[string](movement, "kind"), err)
	}
	return hardeningReload(t, conn, w)
}

func hardeningCheckNow(t *testing.T, conn *pgx.Conn, movement, entry row) error {
	t.Helper()
	tx := begin(t, conn)
	mustExec(t, tx, "ALTER TABLE wallet_ledger_entries DISABLE TRIGGER ledger_entry_check")
	mustInsert(t, tx, "wager_transactions", movement)
	mustInsert(t, tx, "wallet_ledger_entries", entry)
	_, err := tx.Exec(t.Context(), "SET CONSTRAINTS "+hardeningEntryTrigger+" IMMEDIATE")
	return err
}

func TestHardening_EntryRequiresProcessedTransaction(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)

	tests := []struct {
		name     string
		movement func(w row) row
		dir      string
	}{
		{"REJECTED", func(w row) row {
			return externalRow(w, "BET", 100).set("status", "REJECTED").set("failure_code", "INSUFFICIENT_FUNDS")
		}, "DEBIT"},
		{"PENDING_REFERENCE", pendingReferenceRow, "CREDIT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := seedWallet(t, owner, 1000)

			err := hardeningCommit(t, owner, w, tt.movement(w), tt.dir)

			requireSQLState(t, err, ledgerMismatch, hardeningEntryTrigger)
		})
	}
}

func TestHardening_LossHasNoEntry(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)
	loss := externalRow(w, "LOSS", 0)

	t.Run("through the immediate check", func(t *testing.T) {
		tx := begin(t, owner)
		mustInsert(t, tx, "wager_transactions", loss)
		err := insert(t.Context(), tx, "wallet_ledger_entries", ledgerRow(loss, "DEBIT", 1000).
			set("amount_minor", int64(1)).set("balance_after_minor", int64(999)))

		requireSQLState(t, err, ledgerMismatch, "ledger_entry_check")
	})

	t.Run("through the deferred check alone", func(t *testing.T) {
		entry := ledgerRow(loss, "DEBIT", 1000).set("amount_minor", int64(1)).set("balance_after_minor", int64(999))

		err := hardeningCheckNow(t, owner, loss, entry)

		requireSQLState(t, err, ledgerMismatch, hardeningEntryTrigger)
	})
}

func TestHardening_DirectionByKind(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)

	t.Run("OPENING credits", func(t *testing.T) {
		w := seedWallet(t, owner, 1000)
		if n := queryInt64(t, owner, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'CREDIT'`, w["id"]); n != 1 {
			t.Errorf("OPENING entries = %d, want 1", n)
		}
	})

	t.Run("BET that credits, through the immediate check", func(t *testing.T) {
		w := seedWallet(t, owner, 1000)
		tx := begin(t, owner)
		bet := externalRow(w, "BET", 100)
		mustInsert(t, tx, "wager_transactions", bet)

		err := insert(t.Context(), tx, "wallet_ledger_entries", ledgerRow(bet, "CREDIT", 1000))

		requireSQLState(t, err, ledgerMismatch, "ledger_entry_check")
	})

	for _, tt := range []struct {
		kind, dir string
	}{
		{"BET", "CREDIT"},
		{"WIN", "DEBIT"},
		{"REFUND", "DEBIT"},
		{"OPENING", "DEBIT"},
	} {
		t.Run(tt.kind+" that "+tt.dir+"s, through the deferred check alone", func(t *testing.T) {
			w := seedWallet(t, owner, 1000)
			movement := externalRow(w, tt.kind, 100)
			switch tt.kind {
			case "REFUND":
				bet := externalRow(w, "BET", 100)
				if err := hardeningCommit(t, owner, w, bet, "DEBIT"); err != nil {
					t.Fatalf("BET: %v", err)
				}
				movement = reversalRow(w, bet, "REFUND")
			case "OPENING":
				empty := seedWallet(t, owner, 0)
				movement = openingRow(empty).set("amount_minor", int64(100))
			}

			err := hardeningCheckNow(t, owner, movement, ledgerRow(movement, tt.dir, 1000))

			requireSQLState(t, err, ledgerMismatch, hardeningEntryTrigger)
		})
	}
}

func TestHardening_RollbackDirection(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)

	tests := []struct {
		name       string
		targetKind string
		targetDir  string
		dir        string
		wantErr    bool
	}{
		{"of a BET credits", "BET", "DEBIT", "CREDIT", false},
		{"of a BET cannot debit", "BET", "DEBIT", "DEBIT", true},
		{"of a WIN debits", "WIN", "CREDIT", "DEBIT", false},
		{"of a WIN cannot credit", "WIN", "CREDIT", "CREDIT", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := seedWallet(t, owner, 1000)
			target := externalRow(w, tt.targetKind, 100)
			tx := begin(t, owner)
			hardeningMove(t, tx, w, target, tt.targetDir)
			mustCommit(t, tx)
			w = hardeningReload(t, owner, w)

			err := hardeningCommit(t, owner, w, reversalRow(w, target, "ROLLBACK"), tt.dir)

			if !tt.wantErr {
				if err != nil {
					t.Fatalf("commit: %v", err)
				}
				return
			}
			requireSQLState(t, err, ledgerMismatch, hardeningEntryTrigger)
		})
	}
}

func TestHardening_RollbackOfRefundDebits(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)
	bet := externalRow(w, "BET", 100)
	if err := hardeningCommit(t, owner, w, bet, "DEBIT"); err != nil {
		t.Fatalf("BET: %v", err)
	}
	w = hardeningReload(t, owner, w)
	refund := reversalRow(w, bet, "REFUND")
	if err := hardeningCommit(t, owner, w, refund, "CREDIT"); err != nil {
		t.Fatalf("REFUND: %v", err)
	}
	w = hardeningReload(t, owner, w)

	if err := hardeningCommit(t, owner, w, reversalRow(w, refund, "ROLLBACK"), "DEBIT"); err != nil {
		t.Errorf("ROLLBACK of a REFUND with DEBIT: %v", err)
	}
}

func hardeningReload(t *testing.T, conn *pgx.Conn, w row) row {
	t.Helper()
	var balance, version int64
	if err := conn.QueryRow(t.Context(), "SELECT balance_minor, version FROM wallets WHERE id = $1", w["id"]).
		Scan(&balance, &version); err != nil {
		t.Fatalf("reload wallet: %v", err)
	}
	return w.set("balance_minor", balance).set("version", version)
}

func TestHardening_ProcessedMovementHasEntry(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)

	t.Run("BET inserted processed without entry", func(t *testing.T) {
		w := seedWallet(t, owner, 1000)
		tx := begin(t, owner)
		mustInsert(t, tx, "wager_transactions", externalRow(w, "BET", 100))

		requireSQLState(t, tx.Commit(t.Context()), ledgerMismatch, hardeningTxTrigger)
	})

	t.Run("WIN updated to processed without entry", func(t *testing.T) {
		w := seedWallet(t, owner, 1000)
		pending := pendingReferenceRow(w)
		tx := begin(t, owner)
		mustInsert(t, tx, "wager_transactions", pending)
		mustCommit(t, tx)

		tx = begin(t, owner)
		mustExec(t, tx, `UPDATE wager_transactions
			SET status = 'PROCESSED', result_balance_minor = 1000, result_wallet_version = 1,
			    processed_at = $2, updated_at = $2
			WHERE id = $1`, pending["id"], t0)

		requireSQLState(t, tx.Commit(t.Context()), ledgerMismatch, hardeningTxTrigger)
	})

	t.Run("LOSS processed without entry", func(t *testing.T) {
		w := seedWallet(t, owner, 1000)
		tx := begin(t, owner)
		mustInsert(t, tx, "wager_transactions", externalRow(w, "LOSS", 0))

		if err := tx.Commit(t.Context()); err != nil {
			t.Errorf("commit: %v", err)
		}
	})

	t.Run("BET with its entry", func(t *testing.T) {
		w := seedWallet(t, owner, 1000)

		if err := hardeningCommit(t, owner, w, externalRow(w, "BET", 100), "DEBIT"); err != nil {
			t.Errorf("commit: %v", err)
		}
	})
}

func TestHardening_NilUUIDRefused(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)
	inbox := row{
		"consumer_name": "wager-transactions",
		"message_id":    "msg-1",
		"payload_hash":  strings.Repeat("0a", 32),
		"received_at":   t0,
	}

	tests := []struct {
		table, column string
		base          row
	}{
		{"wallets", "id", walletRow(0)},
		{"wallets", "player_id", walletRow(0)},
		{"wager_transactions", "id", externalRow(w, "LOSS", 0)},
		{"wager_transactions", "wallet_id", externalRow(w, "LOSS", 0)},
		{"wager_transactions", "player_id", externalRow(w, "LOSS", 0)},
		{"wager_transactions", "reference_transaction_id", externalRow(w, "WIN", 100).set("reference_external_transaction_id", "bet-1")},
		{"wallet_ledger_entries", "id", ledgerRow(externalRow(w, "BET", 100), "DEBIT", 1000)},
		{"wallet_ledger_entries", "wallet_id", ledgerRow(externalRow(w, "BET", 100), "DEBIT", 1000)},
		{"wallet_ledger_entries", "transaction_id", ledgerRow(externalRow(w, "BET", 100), "DEBIT", 1000)},
		{"outbox_events", "id", outboxRow(w)},
		{"inbox_messages", "transaction_id", inbox},
	}
	for _, tt := range tests {
		t.Run(tt.table+"."+tt.column, func(t *testing.T) {
			tx := begin(t, owner)

			err := insert(t.Context(), tx, tt.table, tt.base.set(tt.column, uuid.Nil))

			requireSQLState(t, err, checkViolation, "ck_"+tt.table+"_"+tt.column+"_not_nil")
		})
	}
}

func TestHardening_InboxIsTamperProof(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)
	var openingID uuid.UUID
	if err := owner.QueryRow(t.Context(), "SELECT id FROM wager_transactions WHERE wallet_id = $1", w["id"]).
		Scan(&openingID); err != nil {
		t.Fatalf("read opening: %v", err)
	}
	hash := strings.Repeat("0b", 32)
	mustInsert(t, owner, "inbox_messages", row{
		"consumer_name": "wager-transactions",
		"message_id":    "msg-open",
		"payload_hash":  hash,
		"received_at":   t0,
	})
	mustInsert(t, owner, "inbox_messages", row{
		"consumer_name":  "wager-transactions",
		"message_id":     "msg-done",
		"payload_hash":   hash,
		"received_at":    t0,
		"processed_at":   t0,
		"transaction_id": openingID,
		"outcome":        "PROCESSED",
	})
	mustInsert(t, owner, "inbox_messages", row{
		"consumer_name": "wager-transactions",
		"message_id":    "msg-done-alone",
		"payload_hash":  hash,
		"received_at":   t0,
		"processed_at":  t0,
		"outcome":       "REJECTED",
	})

	t.Run("an open message is completed once", func(t *testing.T) {
		mustExec(t, owner, `UPDATE inbox_messages SET processed_at = $1, transaction_id = $2, outcome = 'PROCESSED'
			WHERE message_id = 'msg-open'`, t0, openingID)
	})

	refused := []struct {
		name, sql, trigger string
	}{
		{"completing a completed message again", `UPDATE inbox_messages SET processed_at = processed_at + interval '1 second' WHERE message_id = 'msg-done'`, hardeningInboxGuard},
		{"changing its outcome", `UPDATE inbox_messages SET outcome = 'REPLAY' WHERE message_id = 'msg-done'`, hardeningInboxGuard},
		{"clearing its transaction", `UPDATE inbox_messages SET transaction_id = NULL WHERE message_id = 'msg-done'`, hardeningInboxGuard},
		{"giving it a transaction later", `UPDATE inbox_messages SET transaction_id = (SELECT id FROM wager_transactions LIMIT 1)
			WHERE message_id = 'msg-done-alone'`, hardeningInboxGuard},
		{"changing the payload hash", `UPDATE inbox_messages SET payload_hash = repeat('0c', 32) WHERE message_id = 'msg-done'`, hardeningInboxGuard},
		{"changing the message id", `UPDATE inbox_messages SET message_id = 'msg-other' WHERE message_id = 'msg-done'`, hardeningInboxGuard},
		{"changing the receive time", `UPDATE inbox_messages SET received_at = received_at - interval '1 day' WHERE message_id = 'msg-done'`, hardeningInboxGuard},
		{"deleting a message", `DELETE FROM inbox_messages WHERE message_id = 'msg-done'`, hardeningInboxGuard},
		{"truncating the inbox", `TRUNCATE inbox_messages`, "inbox_no_truncate"},
	}
	for _, tt := range refused {
		t.Run(tt.name, func(t *testing.T) {
			_, err := owner.Exec(t.Context(), tt.sql)

			requireSQLState(t, err, guardViolation, tt.trigger)
		})
	}
}

func TestHardening_DownRemovesEverything(t *testing.T) {
	ctx := t.Context()
	dsn := scratchDatabase(t)
	if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 7); err != nil {
		t.Fatalf("up to 7: %v", err)
	}
	present := func() (constraints, triggers, functions int64) {
		conn := connect(t, dsn)
		constraints = queryInt64(t, conn, `SELECT count(*) FROM pg_constraint WHERE conname LIKE 'ck\_%\_not\_nil'`)
		triggers = queryInt64(t, conn, `SELECT count(*) FROM pg_trigger
			WHERE tgname IN ('ledger_entry_matches_transaction', 'processed_transaction_has_entry', 'inbox_guard', 'inbox_no_truncate')`)
		functions = queryInt64(t, conn, `SELECT count(*) FROM pg_proc
			WHERE proname IN ('ledger_entry_matches_transaction', 'processed_transaction_has_entry', 'inbox_guard')`)
		return constraints, triggers, functions
	}
	if c, tr, f := present(); c != 11 || tr != 4 || f != 3 {
		t.Fatalf("after up: %d nil checks, %d triggers, %d functions; want 11, 4, 3", c, tr, f)
	}

	if err := postgres.Migrate(ctx, dsn, postgres.DirectionDown, 1); err != nil {
		t.Fatalf("down 1: %v", err)
	}
	requireVersion(t, dsn, 6)
	if c, tr, f := present(); c != 0 || tr != 0 || f != 0 {
		t.Errorf("after down 7: %d nil checks, %d triggers, %d functions left; want none", c, tr, f)
	}

	if err := postgres.Migrate(ctx, dsn, postgres.DirectionUp, 1); err != nil {
		t.Fatalf("up 7 again: %v", err)
	}
	requireVersion(t, dsn, 7)
	if c, tr, f := present(); c != 11 || tr != 4 || f != 3 {
		t.Errorf("after up again: %d nil checks, %d triggers, %d functions; want 11, 4, 3", c, tr, f)
	}
}
