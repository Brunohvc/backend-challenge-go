//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

const (
	insufficientPrivilege = "42501"
	checkViolation        = "23514"
	uniqueViolation       = "23505"
	ledgerMismatch        = "WL001"
	guardViolation        = "WL002"
)

func TestSchema_AppRoleCannotMutateLedger(t *testing.T) {
	resetDB(t)
	app := connect(t, pg.AppDSN)
	w := seedWallet(t, app, 1000)

	for _, sql := range []string{
		"UPDATE wallet_ledger_entries SET created_at = created_at WHERE wallet_id = $1",
		"DELETE FROM wallet_ledger_entries WHERE wallet_id = $1",
	} {
		_, err := app.Exec(t.Context(), sql, w["id"])
		requireSQLState(t, err, insufficientPrivilege, "")
	}
	_, err := app.Exec(t.Context(), "TRUNCATE wallet_ledger_entries")
	requireSQLState(t, err, insufficientPrivilege, "")

	if n := queryInt64(t, app, "SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1", w["id"]); n != 1 {
		t.Fatalf("ledger has %d entries, want the 1 seeded", n)
	}
}

func TestSchema_AppRolePrivileges(t *testing.T) {
	resetDB(t)
	app := connect(t, pg.AppDSN)
	w := seedWallet(t, app, 1000)
	mustInsert(t, app, "outbox_events", outboxRow(w))

	for _, table := range []string{"wallets", "wager_transactions", "outbox_events", "inbox_messages"} {
		_, err := app.Exec(t.Context(), "DELETE FROM "+table)
		requireSQLState(t, err, insufficientPrivilege, "")
		_, err = app.Exec(t.Context(), "TRUNCATE "+table+" CASCADE")
		requireSQLState(t, err, insufficientPrivilege, "")
	}
	for _, sql := range []string{
		"CREATE TABLE app_owned (id int)",
		"SELECT version FROM schema_migrations",
		"ALTER TABLE wallets DISABLE TRIGGER wallets_guard",
	} {
		_, err := app.Exec(t.Context(), sql)
		requireSQLState(t, err, insufficientPrivilege, "")
	}
}

func TestSchema_LedgerAppendOnlyEvenForOwner(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)

	_, err := owner.Exec(t.Context(), "UPDATE wallet_ledger_entries SET amount_minor = amount_minor WHERE wallet_id = $1", w["id"])
	requireSQLState(t, err, guardViolation, "ledger_no_update")
	_, err = owner.Exec(t.Context(), "DELETE FROM wallet_ledger_entries WHERE wallet_id = $1", w["id"])
	requireSQLState(t, err, guardViolation, "ledger_no_update")
	_, err = owner.Exec(t.Context(), "TRUNCATE wallet_ledger_entries")
	requireSQLState(t, err, guardViolation, "ledger_no_truncate")

	if n := queryInt64(t, owner, "SELECT count(*) FROM wallet_ledger_entries"); n != 1 {
		t.Fatalf("ledger has %d entries, want the 1 seeded", n)
	}
}

func TestSchema_BalanceCannotBeNegative(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)

	err := insert(t.Context(), owner, "wallets", walletRow(-1))
	requireSQLState(t, err, checkViolation, "ck_wallets_balance")

	w := seedWallet(t, owner, 100)
	_, err = owner.Exec(t.Context(), "UPDATE wallets SET balance_minor = -1, version = 2 WHERE id = $1", w["id"])
	requireSQLState(t, err, checkViolation, "ck_wallets_balance")

	tx := begin(t, owner)
	bet := externalRow(w, "BET", 250)
	mustInsert(t, tx, "wager_transactions", bet)
	err = insert(t.Context(), tx, "wallet_ledger_entries", ledgerRow(bet, "DEBIT", 100))
	requireSQLState(t, err, checkViolation, "ck_ledger_balance_after")
}

func TestSchema_LedgerArithmetic(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)

	tx := begin(t, owner)
	bet := externalRow(w, "BET", 100)
	mustInsert(t, tx, "wager_transactions", bet)
	err := insert(t.Context(), tx, "wallet_ledger_entries", ledgerRow(bet, "DEBIT", 1000).set("balance_after_minor", int64(950)))
	requireSQLState(t, err, checkViolation, "ck_ledger_math")
}

func TestSchema_WalletMustMatchLedger(t *testing.T) {
	resetDB(t)
	app := connect(t, pg.AppDSN)

	t.Run("zero balance wallet without entries commits", func(t *testing.T) {
		tx := begin(t, app)
		mustInsert(t, tx, "wallets", walletRow(0))
		mustCommit(t, tx)
	})

	t.Run("funded wallet without its opening entry fails at commit", func(t *testing.T) {
		tx := begin(t, app)
		w := walletRow(500)
		if err := insert(t.Context(), tx, "wallets", w); err != nil {
			t.Fatalf("the check is deferred, but the insert failed: %v", err)
		}
		requireSQLState(t, tx.Commit(t.Context()), ledgerMismatch, "wallet_matches_ledger")
		if n := queryInt64(t, app, "SELECT count(*) FROM wallets WHERE id = $1", w["id"]); n != 0 {
			t.Fatal("the wallet was committed")
		}
	})

	t.Run("balance change without an entry fails at commit", func(t *testing.T) {
		w := seedWallet(t, app, 0)
		tx := begin(t, app)
		mustExec(t, tx, "UPDATE wallets SET balance_minor = 500, version = 2 WHERE id = $1", w["id"])
		requireSQLState(t, tx.Commit(t.Context()), ledgerMismatch, "wallet_matches_ledger")
		if b := queryInt64(t, app, "SELECT balance_minor FROM wallets WHERE id = $1", w["id"]); b != 0 {
			t.Fatalf("balance = %d after the failed commit, want 0", b)
		}
	})

	t.Run("entry without the balance change fails at commit", func(t *testing.T) {
		w := seedWallet(t, app, 1000)
		tx := begin(t, app)
		bet := externalRow(w, "BET", 100)
		mustInsert(t, tx, "wager_transactions", bet)
		mustInsert(t, tx, "wallet_ledger_entries", ledgerRow(bet, "DEBIT", 1000))
		requireSQLState(t, tx.Commit(t.Context()), ledgerMismatch, "ledger_matches_wallet")
	})

	t.Run("balance change with its entry commits", func(t *testing.T) {
		w := move(t, app, seedWallet(t, app, 1000), "BET", "DEBIT", 100)
		if b := queryInt64(t, app, "SELECT balance_minor FROM wallets WHERE id = $1", w["id"]); b != 900 {
			t.Fatalf("balance = %d, want 900", b)
		}
	})

	t.Run("order inside the transaction does not matter", func(t *testing.T) {
		w := seedWallet(t, app, 1000)
		tx := begin(t, app)
		mustExec(t, tx, "UPDATE wallets SET balance_minor = 1250, version = 2 WHERE id = $1", w["id"])
		win := externalRow(w, "WIN", 250)
		mustInsert(t, tx, "wager_transactions", win)
		mustInsert(t, tx, "wallet_ledger_entries", ledgerRow(win, "CREDIT", 1000))
		mustCommit(t, tx)
	})
}

func TestSchema_LedgerEntryContinuesTheWallet(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)

	tests := []struct {
		name  string
		entry func(t *testing.T) (tx, entry row)
	}{
		{"does not start at the last balance", func(t *testing.T) (row, row) {
			w := seedWallet(t, owner, 1000)
			bet := externalRow(w, "BET", 100)
			return bet, ledgerRow(bet, "DEBIT", 900)
		}},
		{"first entry does not start at zero", func(t *testing.T) (row, row) {
			w := seedWallet(t, owner, 0)
			win := externalRow(w, "WIN", 100)
			return win, ledgerRow(win, "CREDIT", 50)
		}},
		{"amount differs from its transaction", func(t *testing.T) (row, row) {
			w := seedWallet(t, owner, 1000)
			bet := externalRow(w, "BET", 100)
			entry := ledgerRow(bet, "DEBIT", 1000).set("amount_minor", int64(50)).set("balance_after_minor", int64(950))
			return bet, entry
		}},
		{"currency differs from the wallet", func(t *testing.T) (row, row) {
			w := seedWallet(t, owner, 1000)
			bet := externalRow(w, "BET", 100).set("currency", "USD")
			return bet, ledgerRow(bet, "DEBIT", 1000)
		}},
		{"wallet differs from its transaction", func(t *testing.T) (row, row) {
			w := seedWallet(t, owner, 1000)
			other := seedWallet(t, owner, 1000)
			bet := externalRow(w, "BET", 100)
			return bet, ledgerRow(bet, "DEBIT", 1000).set("wallet_id", other["id"])
		}},
		{"bet that credits", func(t *testing.T) (row, row) {
			w := seedWallet(t, owner, 1000)
			bet := externalRow(w, "BET", 100)
			return bet, ledgerRow(bet, "CREDIT", 1000)
		}},
		{"win that debits", func(t *testing.T) (row, row) {
			w := seedWallet(t, owner, 1000)
			win := externalRow(w, "WIN", 100)
			return win, ledgerRow(win, "DEBIT", 1000)
		}},
		{"loss, which moves no money", func(t *testing.T) (row, row) {
			w := seedWallet(t, owner, 1000)
			loss := externalRow(w, "LOSS", 0)
			return loss, ledgerRow(loss, "DEBIT", 1000).set("amount_minor", int64(100)).set("balance_after_minor", int64(900))
		}},
		{"seq before the last entry of the wallet", func(t *testing.T) (row, row) {
			w := seedWallet(t, owner, 1000)
			bet := externalRow(w, "BET", 100)
			return bet, ledgerRow(bet, "DEBIT", 1000).set("seq", int64(0))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			movement, entry := tc.entry(t)
			tx := begin(t, owner)
			mustInsert(t, tx, "wager_transactions", movement)
			sql := "INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
			args := []any{entry["id"], entry["wallet_id"], entry["transaction_id"], entry["direction"], entry["amount_minor"],
				entry["currency"], entry["balance_before_minor"], entry["balance_after_minor"], entry["created_at"]}
			if seq, ok := entry["seq"]; ok {
				sql = "INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor, created_at, seq) OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
				args = append(args, seq)
			}
			_, err := tx.Exec(t.Context(), sql, args...)
			requireSQLState(t, err, ledgerMismatch, "ledger_entry_check")
		})
	}
}

func TestSchema_OriginShape(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 100)
	w = hardeningRecord(t, owner, w, externalRow(w, "BET", 100), "DEBIT")

	for name, r := range map[string]row{
		"opening with a provider":    openingRow(w.set("balance_minor", int64(100))).set("provider_id", "provider-a"),
		"opening with a round":       openingRow(w.set("balance_minor", int64(100))).set("round_id", "round-1"),
		"opening sent from outside":  openingRow(w.set("balance_minor", int64(100))).set("origin", "EXTERNAL"),
		"bet without a round":        externalRow(w, "BET", 100).set("round_id", nil),
		"bet without a payload hash": externalRow(w, "BET", 100).set("payload_hash", nil),
		"bet made internally":        externalRow(w, "BET", 100).set("origin", "INTERNAL"),
	} {
		t.Run(name, func(t *testing.T) {
			requireSQLState(t, insert(t.Context(), owner, "wager_transactions", r), checkViolation, "ck_origin_shape")
		})
	}
}

func TestSchema_ZeroPolicy(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 0)

	mustInsert(t, owner, "wager_transactions", externalRow(w, "LOSS", 0))

	for name, r := range map[string]row{
		"loss with an amount": externalRow(w, "LOSS", 100),
		"bet of zero":         externalRow(w, "BET", 0),
		"negative win":        externalRow(w, "WIN", -100),
		"opening of zero":     openingRow(w),
	} {
		t.Run(name, func(t *testing.T) {
			requireSQLState(t, insert(t.Context(), owner, "wager_transactions", r), checkViolation, "ck_zero_policy")
		})
	}
}

func TestSchema_SingleOpening(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)

	err := insert(t.Context(), owner, "wager_transactions", openingRow(w))
	requireSQLState(t, err, uniqueViolation, "ux_tx_single_opening")
}

func TestSchema_SingleProcessedReversal(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)
	bet := externalRow(w, "BET", 100)
	w = hardeningRecord(t, owner, w, bet, "DEBIT")
	w = hardeningRecord(t, owner, w, reversalRow(w, bet, "REFUND"), "CREDIT")

	for _, kind := range []string{"REFUND", "ROLLBACK"} {
		err := insert(t.Context(), owner, "wager_transactions", reversalRow(w, bet, kind))
		requireSQLState(t, err, uniqueViolation, "ux_tx_single_reversal")
	}

	mustInsert(t, owner, "wager_transactions", reversalRow(w, bet, "ROLLBACK").
		set("status", "REJECTED").set("failure_code", "REFERENCE_ALREADY_REVERSED"))
}

func TestSchema_IdempotencyKeys(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)
	bet := externalRow(w, "BET", 100)
	w = hardeningRecord(t, owner, w, bet, "DEBIT")

	sameKey := externalRow(w, "BET", 100).set("idempotency_key", bet["idempotency_key"])
	requireSQLState(t, insert(t.Context(), owner, "wager_transactions", sameKey), uniqueViolation, "ux_tx_provider_key")

	sameExternal := externalRow(w, "BET", 100).set("external_transaction_id", bet["external_transaction_id"])
	requireSQLState(t, insert(t.Context(), owner, "wager_transactions", sameExternal), uniqueViolation, "ux_tx_provider_external")

	hardeningRecord(t, owner, w, externalRow(w, "BET", 100).
		set("provider_id", "provider-b").
		set("idempotency_key", bet["idempotency_key"]).
		set("external_transaction_id", bet["external_transaction_id"]), "DEBIT")
}

func TestSchema_TerminalImmutable(t *testing.T) {
	ctx := t.Context()
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)

	processed := externalRow(w, "BET", 100)
	rejected := externalRow(w, "BET", 5000).set("status", "REJECTED").set("failure_code", "INSUFFICIENT_FUNDS")
	failed := externalRow(w, "BET", 100).set("status", "FAILED").set("failure_code", "PROCESSING_FAILED").
		set("result_balance_minor", nil).set("result_wallet_version", nil).set("permanent_failures", 3)
	hardeningRecord(t, owner, w, processed, "DEBIT")
	for _, r := range []row{rejected, failed} {
		mustInsert(t, owner, "wager_transactions", r)
	}

	_, err := owner.Exec(ctx, "UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'INSUFFICIENT_FUNDS' WHERE id = $1", processed["id"])
	requireSQLState(t, err, guardViolation, "wager_transactions_guard")

	for _, r := range []row{processed, rejected, failed} {
		_, err := owner.Exec(ctx, "UPDATE wager_transactions SET updated_at = updated_at + interval '1 second' WHERE id = $1", r["id"])
		requireSQLState(t, err, guardViolation, "wager_transactions_guard")
		_, err = owner.Exec(ctx, "DELETE FROM wager_transactions WHERE id = $1", r["id"])
		requireSQLState(t, err, guardViolation, "wager_transactions_guard")
	}

	_, err = owner.Exec(ctx, "TRUNCATE wager_transactions CASCADE")
	requireSQLState(t, err, guardViolation, "")
}

func TestSchema_TransactionGuard(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)
	waiting := pendingReferenceRow(w)
	mustInsert(t, owner, "wager_transactions", waiting)

	for name, sql := range map[string]string{
		"amount":                  "UPDATE wager_transactions SET amount_minor = 1 WHERE id = $1",
		"wallet":                  "UPDATE wager_transactions SET wallet_id = gen_random_uuid() WHERE id = $1",
		"idempotency key":         "UPDATE wager_transactions SET idempotency_key = 'another-key' WHERE id = $1",
		"payload hash":            "UPDATE wager_transactions SET payload_hash = repeat('b', 64) WHERE id = $1",
		"reference":               "UPDATE wager_transactions SET reference_external_transaction_id = 'other-bet' WHERE id = $1",
		"correlation id":          "UPDATE wager_transactions SET correlation_id = 'another' WHERE id = $1",
		"back to PENDING":         "UPDATE wager_transactions SET status = 'PENDING' WHERE id = $1",
		"attempts going back":     "UPDATE wager_transactions SET reference_attempts = reference_attempts - 1 WHERE id = $1",
		"failures going back":     "UPDATE wager_transactions SET permanent_failures = permanent_failures - 1 WHERE id = $1",
		"expiry moved by a retry": "UPDATE wager_transactions SET expires_at = expires_at + interval '1 hour' WHERE id = $1",
		"delete":                  "DELETE FROM wager_transactions WHERE id = $1",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := owner.Exec(t.Context(), sql, waiting["id"])
			requireSQLState(t, err, guardViolation, "wager_transactions_guard")
		})
	}

	mustExec(t, owner, `UPDATE wager_transactions
		SET reference_attempts = reference_attempts + 1, next_attempt_at = next_attempt_at + interval '1 minute',
		    updated_at = updated_at + interval '1 second'
		WHERE id = $1`, waiting["id"])
	tx := begin(t, owner)
	mustExec(t, tx, `UPDATE wager_transactions
		SET status = 'PROCESSED', result_balance_minor = 1500, result_wallet_version = 2,
		    next_attempt_at = NULL, expires_at = NULL, processed_at = updated_at, updated_at = updated_at
		WHERE id = $1`, waiting["id"])
	mustInsert(t, tx, "wallet_ledger_entries", ledgerRow(waiting, "CREDIT", 1000))
	mustExec(t, tx, "UPDATE wallets SET balance_minor = 1500, version = 2 WHERE id = $1", w["id"])
	mustCommit(t, tx)
}

func TestSchema_WalletGuard(t *testing.T) {
	ctx := t.Context()
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)

	for name, sql := range map[string]string{
		"player":                      "UPDATE wallets SET player_id = gen_random_uuid() WHERE id = $1",
		"currency":                    "UPDATE wallets SET currency = 'USD' WHERE id = $1",
		"creation time":               "UPDATE wallets SET created_at = created_at - interval '1 day' WHERE id = $1",
		"balance without the version": "UPDATE wallets SET balance_minor = 900 WHERE id = $1",
		"balance skipping a version":  "UPDATE wallets SET balance_minor = 900, version = version + 2 WHERE id = $1",
		"version without the balance": "UPDATE wallets SET version = version + 1 WHERE id = $1",
		"version going back":          "UPDATE wallets SET balance_minor = 900, version = version - 1 WHERE id = $1",
		"delete":                      "DELETE FROM wallets WHERE id = $1",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := owner.Exec(t.Context(), sql, w["id"])
			requireSQLState(t, err, guardViolation, "wallets_guard")
		})
	}

	mustExec(t, owner, "UPDATE wallets SET updated_at = updated_at + interval '1 second' WHERE id = $1", w["id"])

	_, err := owner.Exec(ctx, "TRUNCATE wallets CASCADE")
	requireSQLState(t, err, guardViolation, "")
}

func TestSchema_TransactionChecks(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 1000)
	bet := externalRow(w, "BET", 100)
	w = hardeningRecord(t, owner, w, bet, "DEBIT")

	refund := reversalRow(w, bet, "REFUND")
	tests := []struct {
		name       string
		row        row
		constraint string
	}{
		{"unknown origin", externalRow(w, "BET", 100).set("origin", "PARTNER"), "ck_origin_shape"},
		{"unknown kind", externalRow(w, "JACKPOT", 100), "ck_tx_kind"},
		{"unknown status", externalRow(w, "BET", 100).set("status", "DONE").set("processed_at", nil).
			set("result_balance_minor", nil).set("result_wallet_version", nil), "ck_tx_status"},
		{"lowercase currency", externalRow(w, "BET", 100).set("currency", "brl"), "ck_tx_currency"},
		{"processed with a failure code", externalRow(w, "BET", 100).set("failure_code", "INSUFFICIENT_FUNDS"), "ck_failure_code"},
		{"rejected without a failure code", externalRow(w, "BET", 100).set("status", "REJECTED"), "ck_failure_code"},
		{"rejected with an unknown code", externalRow(w, "BET", 100).set("status", "REJECTED").set("failure_code", "NOPE"), "ck_tx_failure_code_value"},
		{"rejected as a processing failure", externalRow(w, "BET", 100).set("status", "REJECTED").set("failure_code", "PROCESSING_FAILED"), "ck_tx_failure_code_value"},
		{"failed with a rejection code", externalRow(w, "BET", 100).set("status", "FAILED").set("failure_code", "INSUFFICIENT_FUNDS").
			set("result_balance_minor", nil).set("result_wallet_version", nil), "ck_tx_failure_code_value"},
		{"pending without a schedule", externalRow(w, "BET", 100).set("status", "PENDING").set("processed_at", nil).
			set("result_balance_minor", nil).set("result_wallet_version", nil), "ck_pending_schedule"},
		{"waiting without an expiry", pendingReferenceRow(w).set("expires_at", nil), "ck_tx_pending_reference"},
		{"waiting without a reference", pendingReferenceRow(w).set("reference_external_transaction_id", nil), "ck_tx_pending_reference"},
		{"refund without a reference", externalRow(w, "REFUND", 100), "ck_reference_required"},
		{"bet with a reference", externalRow(w, "BET", 100).set("reference_external_transaction_id", "bet-0"), "ck_tx_reference_allowed"},
		{"loss with a reference", externalRow(w, "LOSS", 0).set("reference_external_transaction_id", "bet-0"), "ck_tx_reference_allowed"},
		{"reference to itself", externalRow(w, "WIN", 100).set("external_transaction_id", "win-1").
			set("reference_external_transaction_id", "win-1"), "ck_tx_no_self_reference"},
		{"resolved without a reference", externalRow(w, "WIN", 100).set("reference_transaction_id", bet["id"]), "ck_tx_reference_resolution"},
		{"resolved to itself", func() row {
			r := externalRow(w, "WIN", 100).set("reference_external_transaction_id", bet["external_transaction_id"])
			return r.set("reference_transaction_id", r["id"])
		}(), "ck_tx_reference_resolution"},
		{"processed refund not resolved", refund.set("reference_transaction_id", nil), "ck_tx_reversal_resolved"},
		{"final without a processing time", externalRow(w, "BET", 100).set("processed_at", nil), "ck_tx_processed_at"},
		{"waiting with a processing time", pendingReferenceRow(w).set("processed_at", t0), "ck_tx_processed_at"},
		{"processed without a result", externalRow(w, "BET", 100).set("result_balance_minor", nil).set("result_wallet_version", nil), "ck_tx_result"},
		{"result balance without the version", externalRow(w, "BET", 100).set("result_wallet_version", nil), "ck_tx_result"},
		{"waiting with a result", pendingReferenceRow(w).set("result_balance_minor", int64(0)).set("result_wallet_version", int64(1)), "ck_tx_result"},
		{"failed with a result", externalRow(w, "BET", 100).set("status", "FAILED").set("failure_code", "PROCESSING_FAILED"), "ck_tx_result"},
		{"negative result balance", externalRow(w, "BET", 100).set("result_balance_minor", int64(-1)), "ck_tx_result"},
		{"result version zero", externalRow(w, "BET", 100).set("result_wallet_version", int64(0)), "ck_tx_result"},
		{"negative attempts", externalRow(w, "BET", 100).set("reference_attempts", -1), "ck_tx_counters"},
		{"negative failures", externalRow(w, "BET", 100).set("permanent_failures", -1), "ck_tx_counters"},
		{"updated before created", externalRow(w, "BET", 100).set("updated_at", t0.Add(-time.Second)), "ck_tx_updated_at"},
		{"empty correlation id", externalRow(w, "BET", 100).set("correlation_id", ""), "ck_tx_correlation_id"},
		{"uppercase payload hash", externalRow(w, "BET", 100).set("payload_hash", strings.Repeat("0A", 32)), "ck_tx_payload_hash"},
		{"short payload hash", externalRow(w, "BET", 100).set("payload_hash", "abc"), "ck_tx_payload_hash"},
		{"empty provider id", externalRow(w, "BET", 100).set("provider_id", ""), "ck_tx_text_ids"},
		{"provider id with a space", externalRow(w, "BET", 100).set("provider_id", "provider a"), "ck_tx_text_ids"},
		{"round id too long", externalRow(w, "BET", 100).set("round_id", strings.Repeat("r", 129)), "ck_tx_text_ids"},
		{"external id with a newline", externalRow(w, "BET", 100).set("external_transaction_id", "ext-1\n"), "ck_tx_text_ids"},
		{"empty idempotency key", externalRow(w, "BET", 100).set("idempotency_key", ""), "ck_tx_idempotency_key"},
		{"idempotency key with a space", externalRow(w, "BET", 100).set("idempotency_key", "key 1"), "ck_tx_idempotency_key"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requireSQLState(t, insert(t.Context(), owner, "wager_transactions", tc.row), checkViolation, tc.constraint)
		})
	}
}

func TestSchema_WalletAndLedgerChecks(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)

	for name, tc := range map[string]struct {
		row        row
		constraint string
	}{
		"lowercase currency":     {walletRow(0).set("currency", "brl"), "ck_wallets_currency"},
		"version zero":           {walletRow(0).set("version", int64(0)), "ck_wallets_version"},
		"updated before created": {walletRow(0).set("updated_at", t0.Add(-time.Second)), "ck_wallets_updated_at"},
	} {
		t.Run(name, func(t *testing.T) {
			requireSQLState(t, insert(t.Context(), owner, "wallets", tc.row), checkViolation, tc.constraint)
		})
	}

	w := walletRow(0)
	mustInsert(t, owner, "wallets", w)
	err := insert(t.Context(), owner, "wallets", walletRow(0).set("player_id", w["player_id"]))
	requireSQLState(t, err, uniqueViolation, "ux_wallets_player_currency")
	mustInsert(t, owner, "wallets", walletRow(0).set("player_id", w["player_id"]).set("currency", "USD"))

	w = seedWallet(t, owner, 1000)
	opening := queryUUID(t, owner, "SELECT id FROM wager_transactions WHERE wallet_id = $1", w["id"])
	tx := begin(t, owner)
	err = insert(t.Context(), tx, "wallet_ledger_entries", ledgerRow(openingRow(w).set("id", opening), "CREDIT", 1000))
	requireSQLState(t, err, uniqueViolation, "ux_ledger_wallet_tx")
}

func TestSchema_OutboxPayloadImmutable(t *testing.T) {
	ctx := t.Context()
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	app := connect(t, pg.AppDSN)
	w := seedWallet(t, owner, 0)
	event := outboxRow(w)
	mustInsert(t, app, "outbox_events", event)

	for name, sql := range map[string]string{
		"payload":        `UPDATE outbox_events SET payload = '{"eventId":"e-2"}' WHERE id = $1`,
		"event type":     "UPDATE outbox_events SET event_type = 'WalletCredited' WHERE id = $1",
		"event version":  "UPDATE outbox_events SET event_version = 2 WHERE id = $1",
		"aggregate":      "UPDATE outbox_events SET aggregate_id = 'another' WHERE id = $1",
		"aggregate type": "UPDATE outbox_events SET aggregate_type = 'transaction' WHERE id = $1",
		"correlation id": "UPDATE outbox_events SET correlation_id = 'another' WHERE id = $1",
		"causation id":   "UPDATE outbox_events SET causation_id = 'another' WHERE id = $1",
		"occurred at":    "UPDATE outbox_events SET occurred_at = occurred_at + interval '1 second' WHERE id = $1",
		"traceparent": "UPDATE outbox_events " +
			"SET traceparent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01' WHERE id = $1",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := app.Exec(t.Context(), sql, event["id"])
			requireSQLState(t, err, guardViolation, "outbox_guard")
		})
	}

	mustExec(t, app, `UPDATE outbox_events SET locked_by = 'relay-1', locked_until = now() + interval '30 seconds',
		attempts = attempts + 1, last_error = 'timeout', next_attempt_at = now() WHERE id = $1`, event["id"])
	mustExec(t, app, "UPDATE outbox_events SET published_at = now(), locked_by = NULL, locked_until = NULL WHERE id = $1", event["id"])

	for name, sql := range map[string]string{
		"unpublished":      "UPDATE outbox_events SET published_at = NULL WHERE id = $1",
		"published again":  "UPDATE outbox_events SET published_at = published_at + interval '1 second' WHERE id = $1",
		"payload once out": `UPDATE outbox_events SET payload = '{}' WHERE id = $1`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := app.Exec(t.Context(), sql, event["id"])
			requireSQLState(t, err, guardViolation, "outbox_guard")
		})
	}

	_, err := app.Exec(ctx, "DELETE FROM outbox_events WHERE id = $1", event["id"])
	requireSQLState(t, err, insufficientPrivilege, "")
	mustExec(t, owner, "DELETE FROM outbox_events WHERE id = $1", event["id"])
}

func TestSchema_OutboxAndInboxChecks(t *testing.T) {
	resetDB(t)
	owner := connect(t, pg.OwnerDSN)
	w := seedWallet(t, owner, 0)

	for name, tc := range map[string]struct {
		table, constraint string
		row               row
	}{
		"negative attempts":     {"outbox_events", "ck_outbox_attempts", outboxRow(w).set("attempts", -1)},
		"event version zero":    {"outbox_events", "ck_outbox_event_version", outboxRow(w).set("event_version", 0)},
		"payload not an object": {"outbox_events", "ck_outbox_payload", outboxRow(w).set("payload", `[1,2]`)},
		"traceparent not W3C": {"outbox_events", "ck_outbox_traceparent",
			outboxRow(w).set("traceparent", "0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331")},
		"unknown outcome": {"inbox_messages", "ck_inbox_outcome", inboxRow().
			set("outcome", "LOST").set("processed_at", t0)},
		"outcome without processing time": {"inbox_messages", "ck_inbox_completed", inboxRow().set("outcome", "PROCESSED")},
		"processing time without outcome": {"inbox_messages", "ck_inbox_completed", inboxRow().set("processed_at", t0)},
		"malformed hash":                  {"inbox_messages", "ck_inbox_payload_hash", inboxRow().set("payload_hash", strings.Repeat("z", 64))},
	} {
		t.Run(name, func(t *testing.T) {
			requireSQLState(t, insert(t.Context(), owner, tc.table, tc.row), checkViolation, tc.constraint)
		})
	}

	claimed := inboxRow()
	mustInsert(t, owner, "inbox_messages", claimed)
	err := insert(t.Context(), owner, "inbox_messages", inboxRow().set("message_id", claimed["message_id"]))
	requireSQLState(t, err, uniqueViolation, "inbox_messages_pkey")
}

func inboxRow() row {
	return row{
		"consumer_name": "wager-transactions",
		"message_id":    "msg-" + newID().String(),
		"payload_hash":  strings.Repeat("0a", 32),
		"received_at":   t0,
	}
}

func queryUUID(t *testing.T, db execer, sql string, args ...any) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := db.QueryRow(t.Context(), sql, args...).Scan(&id); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return id
}

func TestSchema_AcceptsEveryAggregateState(t *testing.T) {
	resetDB(t)
	app := connect(t, pg.AppDSN)
	w := seedWallet(t, app, 0)
	walletID, playerID := get[uuid.UUID](w, "id"), get[uuid.UUID](w, "player_id")
	now := t0.Add(time.Minute)

	brl := func(minor int64) money.Money {
		m, err := money.FromMinor(minor, money.BRL())
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	external := func(kind, amount, ext, ref string) *wager.Transaction {
		cmd, err := wager.NewCommand(wager.CommandInput{
			ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "key-" + ext,
			PlayerID: playerID.String(), WalletID: walletID.String(), RoundID: "round-1", GameID: "game-1",
			Kind: kind, Amount: amount, Currency: "BRL", ReferenceExternalTransactionID: ref,
		})
		if err != nil {
			t.Fatalf("NewCommand %s: %v", ext, err)
		}
		tx, err := wager.NewExternal(newID(), cmd, "corr-"+ext, t0)
		if err != nil {
			t.Fatalf("NewExternal %s: %v", ext, err)
		}
		return tx
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	db := begin(t, app)
	store := func(name string, tx *wager.Transaction) {
		t.Helper()
		if err := insert(t.Context(), db, "wager_transactions", snapshotRow(tx.Snapshot())); err != nil {
			t.Fatalf("%s (%s): %v", name, tx.Status(), err)
		}
	}

	opening, err := wager.NewOpening(newID(), walletID, playerID, brl(10000), "corr-opening", t0)
	must(err)
	must(opening.MarkProcessed(wager.Result{BalanceAfter: brl(10000), WalletVersion: 1}, now))
	store("opening", opening)

	bet := external("BET", "25.00", "bet-1", "")
	must(bet.MarkProcessed(wager.Result{BalanceAfter: brl(7500), WalletVersion: 2}, now))
	store("bet", bet)

	loss := external("LOSS", "0.00", "loss-1", "")
	must(loss.MarkProcessed(wager.Result{BalanceAfter: brl(7500), WalletVersion: 2}, now))
	store("loss", loss)

	broke := external("BET", "500.00", "bet-2", "")
	must(broke.Reject(wager.FailureInsufficientFunds, brl(7500), 2, now))
	store("rejected with a balance", broke)

	foreign := external("BET", "1.00", "bet-3", "")
	must(foreign.Reject(wager.FailureWalletPlayerMismatch, money.Money{}, 0, now))
	store("rejected without a balance", foreign)

	waiting := external("REFUND", "10.00", "refund-1", "bet-later")
	must(waiting.AwaitReference(now.Add(time.Second), now.Add(time.Hour), now))
	store("waiting", waiting)

	retried := external("ROLLBACK", "10.00", "rollback-1", "bet-later")
	must(retried.AwaitReference(now.Add(time.Second), now.Add(time.Hour), now))
	must(retried.RescheduleReference(now.Add(time.Minute), now.Add(2*time.Second)))
	store("rescheduled", retried)

	refund := external("REFUND", "25.00", "refund-2", "bet-1")
	must(refund.MarkProcessed(wager.Result{BalanceAfter: brl(10000), WalletVersion: 3,
		ReferenceTransactionID: uuid.NullUUID{UUID: bet.ID(), Valid: true}}, now))
	store("processed refund", refund)

	win := external("WIN", "40.00", "win-1", "bet-1")
	must(win.MarkProcessed(wager.Result{BalanceAfter: brl(14000), WalletVersion: 4,
		ReferenceTransactionID: uuid.NullUUID{UUID: bet.ID(), Valid: true}}, now))
	store("win with a reference", win)

	expired := external("ROLLBACK", "10.00", "rollback-2", "bet-never")
	must(expired.AwaitReference(now.Add(time.Second), now.Add(time.Hour), now))
	must(expired.Reject(wager.FailureReferenceNotFound, money.Money{}, 0, now.Add(time.Hour)))
	store("reference expired", expired)

	failed := external("ROLLBACK", "10.00", "rollback-3", "bet-1")
	must(failed.NotePermanentFailure(1, now))
	store("failed", failed)
}

func snapshotRow(s wager.Snapshot) row {
	text := func(v string) any {
		if v == "" {
			return nil
		}
		return v
	}
	r := row{
		"id":                                s.ID,
		"origin":                            string(s.Origin),
		"kind":                              string(s.Kind),
		"status":                            string(s.Status),
		"wallet_id":                         s.WalletID,
		"player_id":                         s.PlayerID,
		"amount_minor":                      s.Money.Minor(),
		"currency":                          s.Money.Currency().Code(),
		"provider_id":                       text(s.ProviderID),
		"external_transaction_id":           text(s.ExternalTransactionID),
		"idempotency_key":                   text(s.IdempotencyKey),
		"payload_hash":                      text(s.PayloadHash),
		"round_id":                          text(s.RoundID),
		"game_id":                           text(s.GameID),
		"reference_external_transaction_id": text(s.ReferenceExternalTransactionID),
		"reference_transaction_id":          s.ReferenceTransactionID,
		"failure_code":                      text(string(s.FailureCode)),
		"result_balance_minor":              nil,
		"result_wallet_version":             nil,
		"correlation_id":                    s.CorrelationID,
		"reference_attempts":                s.ReferenceAttempts,
		"permanent_failures":                s.PermanentFailures,
		"next_attempt_at":                   s.NextAttemptAt,
		"expires_at":                        s.ExpiresAt,
		"created_at":                        s.CreatedAt,
		"updated_at":                        s.UpdatedAt,
		"processed_at":                      s.ProcessedAt,
	}
	if s.ResultBalance != nil {
		r["result_balance_minor"] = s.ResultBalance.Minor()
		r["result_wallet_version"] = s.ResultWalletVersion
	}
	return r
}

func TestSchema_NamedObjects(t *testing.T) {
	owner := connect(t, pg.OwnerDSN)

	for _, index := range []string{
		"ux_wallets_player_currency", "ux_tx_provider_key", "ux_tx_provider_external", "ux_tx_single_opening",
		"ux_tx_single_reversal", "ix_tx_due", "ux_ledger_wallet_tx", "ix_ledger_wallet_seq", "ix_outbox_aggregate",
	} {
		if n := queryInt64(t, owner, "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1", index); n != 1 {
			t.Errorf("index %s not found", index)
		}
	}
	for _, check := range []string{"ck_tx_origin", "ck_origin_shape", "ck_zero_policy", "ck_reference_required", "ck_failure_code", "ck_pending_schedule", "ck_ledger_math"} {
		if n := queryInt64(t, owner, "SELECT count(*) FROM pg_constraint WHERE contype = 'c' AND conname = $1", check); n != 1 {
			t.Errorf("check constraint %s not found", check)
		}
	}

	triggers := map[string]struct {
		table    string
		deferred bool
	}{
		"wallets_guard":            {"wallets", false},
		"wallet_matches_ledger":    {"wallets", true},
		"wager_transactions_guard": {"wager_transactions", false},
		"ledger_entry_check":       {"wallet_ledger_entries", false},
		"ledger_no_update":         {"wallet_ledger_entries", false},
		"ledger_no_truncate":       {"wallet_ledger_entries", false},
		"ledger_matches_wallet":    {"wallet_ledger_entries", true},
		"outbox_guard":             {"outbox_events", false},
	}
	for name, want := range triggers {
		var deferrable, deferred bool
		err := owner.QueryRow(t.Context(), `SELECT tgdeferrable, tginitdeferred FROM pg_trigger
			WHERE tgname = $1 AND tgrelid = $2::regclass`, name, want.table).Scan(&deferrable, &deferred)
		if err != nil {
			t.Errorf("trigger %s on %s: %v", name, want.table, err)
			continue
		}
		if deferrable != want.deferred || deferred != want.deferred {
			t.Errorf("trigger %s: deferrable %v, initially deferred %v; want both %v", name, deferrable, deferred, want.deferred)
		}
	}
}

func TestSchema_StaleWriterCannotForkTheLedger(t *testing.T) {
	resetDB(t)
	app := connect(t, pg.AppDSN)
	w := seedWallet(t, app, 1000)

	first := begin(t, app)
	bet := externalRow(w, "BET", 100)
	mustInsert(t, first, "wager_transactions", bet)
	mustInsert(t, first, "wallet_ledger_entries", ledgerRow(bet, "DEBIT", 1000))
	mustExec(t, first, "UPDATE wallets SET balance_minor = 900, version = 2 WHERE id = $1 AND version = 1", w["id"])

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	stale := connect(t, pg.AppDSN)
	pid := queryInt64(t, stale, "SELECT pg_backend_pid()")
	second := begin(t, stale)
	other := externalRow(w, "BET", 100)
	if err := insert(ctx, second, "wager_transactions", other); err != nil {
		t.Fatalf("store the stale transaction: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- insert(ctx, second, "wallet_ledger_entries", ledgerRow(other, "DEBIT", 1000))
	}()

	waitUntilBlocked(t, pid, done)
	mustCommit(t, first)
	requireSQLState(t, <-done, ledgerMismatch, "ledger_entry_check")
}

func waitUntilBlocked(t *testing.T, pid int64, done <-chan error) {
	t.Helper()
	monitor := connect(t, pg.SuperDSN)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("the stale insert did not wait for the lock: %v", err)
		default:
		}
		if queryInt64(t, monitor, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock'", pid) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("backend %d never waited for a lock", pid)
}
