package wager_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

var (
	txID    = uuid.MustParse("0198b000-0000-7000-8000-000000000001")
	refTxID = uuid.MustParse("0198b000-0000-7000-8000-000000000002")

	t0 = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Minute)
	t2 = t0.Add(2 * time.Minute)
)

const txCorrelationID = "c-0198b000-0001"

func ptrTo[T any](v T) *T { return &v }

func refundInput() wager.CommandInput {
	return edited(func(in *wager.CommandInput) {
		in.ExternalTransactionID = "transaction-124"
		in.Kind = "REFUND"
		in.ReferenceExternalTransactionID = "transaction-123"
	})
}

func newRefund(t *testing.T) *wager.Transaction {
	t.Helper()
	tx, err := wager.NewExternal(txID, newCommand(t, refundInput()), txCorrelationID, t0)
	if err != nil {
		t.Fatalf("NewExternal() error = %v", err)
	}
	return tx
}

func newOpening(t *testing.T) *wager.Transaction {
	t.Helper()
	tx, err := wager.NewOpening(txID, uuid.MustParse(walletUUID), uuid.MustParse(playerUUID),
		parsed(t, "100.00", "BRL"), txCorrelationID, t0)
	if err != nil {
		t.Fatalf("NewOpening() error = %v", err)
	}
	return tx
}

func refundResult(t *testing.T) wager.Result {
	t.Helper()
	return wager.Result{
		BalanceAfter:           parsed(t, "125.00", "BRL"),
		WalletVersion:          4,
		ReferenceTransactionID: uuid.NullUUID{UUID: refTxID, Valid: true},
	}
}

func refundIn(t *testing.T, status wager.Status) *wager.Transaction {
	t.Helper()
	tx := newRefund(t)
	var err error
	switch status {
	case wager.StatusPending:
	case wager.StatusPendingReference:
		err = tx.AwaitReference(t1.Add(time.Second), t1.Add(15*time.Minute), t1)
	case wager.StatusProcessed:
		err = tx.MarkProcessed(refundResult(t), t1)
	case wager.StatusRejected:
		err = tx.Reject(wager.FailureReversalAmountMismatch, parsed(t, "100.00", "BRL"), 3, t1)
	case wager.StatusFailed:
		err = tx.NotePermanentFailure(1, t1)
	default:
		t.Fatalf("refundIn: unknown status %q", status)
	}
	if err != nil {
		t.Fatalf("taking the transaction to %s: %v", status, err)
	}
	return tx
}

type transition struct {
	name     string
	from, to wager.Status
	do       func(tx *wager.Transaction) error
}

func transitionsAt(t *testing.T, now time.Time) []transition {
	t.Helper()
	next, expires := now.Add(time.Second), now.Add(15*time.Minute)
	return []transition{
		{"MarkProcessed", wager.StatusPendingReference, wager.StatusProcessed, func(tx *wager.Transaction) error {
			return tx.MarkProcessed(refundResult(t), now)
		}},
		{"Reject", wager.StatusPendingReference, wager.StatusRejected, func(tx *wager.Transaction) error {
			return tx.Reject(wager.FailureReversalAmountMismatch, parsed(t, "100.00", "BRL"), 3, now)
		}},
		{"AwaitReference", wager.StatusPending, wager.StatusPendingReference, func(tx *wager.Transaction) error {
			return tx.AwaitReference(next, expires, now)
		}},
		{"RescheduleReference", wager.StatusPendingReference, wager.StatusPendingReference, func(tx *wager.Transaction) error {
			return tx.RescheduleReference(next, now)
		}},
		{"NotePermanentFailure", wager.StatusPendingReference, wager.StatusFailed, func(tx *wager.Transaction) error {
			return tx.NotePermanentFailure(1, now)
		}},
	}
}

func assertInvalidTransaction(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, wager.ErrInvalidTransaction) {
		t.Fatalf("error = %v, want ErrInvalidTransaction", err)
	}
}

func assertUnchanged(t *testing.T, tx *wager.Transaction, before wager.Snapshot) {
	t.Helper()
	if after := tx.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Errorf("a failed transition changed the transaction:\n after %+v\nbefore %+v", after, before)
	}
}

func assertSnapshot(t *testing.T, got, want wager.Snapshot) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("snapshot differs:\n got %+v\nwant %+v", got, want)
		for name, pair := range map[string][2]any{
			"ResultBalance": {got.ResultBalance, want.ResultBalance},
			"NextAttemptAt": {got.NextAttemptAt, want.NextAttemptAt},
			"ExpiresAt":     {got.ExpiresAt, want.ExpiresAt},
			"ProcessedAt":   {got.ProcessedAt, want.ProcessedAt},
		} {
			if !reflect.DeepEqual(pair[0], pair[1]) {
				t.Errorf("%s differs: got %v, want %v", name, deref(pair[0]), deref(pair[1]))
			}
		}
	}
}

func deref(v any) any {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil
	}
	return rv.Elem().Interface()
}

func TestErrors_AreDistinct(t *testing.T) {
	errs := []error{wager.ErrInvalidTransition, wager.ErrTerminalState, wager.ErrInvalidTransaction, wager.ErrInvalidDecision}
	for i, a := range errs {
		for j, b := range errs {
			if (i == j) != errors.Is(a, b) {
				t.Errorf("errors.Is(%v, %v) = %v", a, b, errors.Is(a, b))
			}
		}
	}
}

func TestNewOpening_HasNoExternalMetadata(t *testing.T) {
	tx := newOpening(t)

	want := wager.Snapshot{
		ID:            txID,
		Origin:        wager.OriginInternal,
		Kind:          wager.KindOpening,
		Status:        wager.StatusPending,
		WalletID:      uuid.MustParse(walletUUID),
		PlayerID:      uuid.MustParse(playerUUID),
		Money:         parsed(t, "100.00", "BRL"),
		CorrelationID: txCorrelationID,
		CreatedAt:     t0,
		UpdatedAt:     t0,
	}
	assertSnapshot(t, tx.Snapshot(), want)

	for name, got := range map[string]string{
		"ProviderID":                     tx.ProviderID(),
		"ExternalTransactionID":          tx.ExternalTransactionID(),
		"IdempotencyKey":                 tx.IdempotencyKey(),
		"PayloadHash":                    tx.PayloadHash(),
		"RoundID":                        tx.RoundID(),
		"GameID":                         tx.GameID(),
		"ReferenceExternalTransactionID": tx.ReferenceExternalTransactionID(),
	} {
		if got != "" {
			t.Errorf("%s = %q, want empty", name, got)
		}
	}
}

func TestNewOpening_Invalid(t *testing.T) {
	zeroBRL, err := money.Zero(money.BRL())
	if err != nil {
		t.Fatalf("money.Zero() error = %v", err)
	}
	negative, err := money.FromMinor(-100, money.BRL())
	if err != nil {
		t.Fatalf("money.FromMinor() error = %v", err)
	}

	type args struct {
		id, walletID, playerID uuid.UUID
		amount                 money.Money
		correlationID          string
		now                    time.Time
	}
	tests := []struct {
		name string
		edit func(a *args)
	}{
		{"nil id", func(a *args) { a.id = uuid.Nil }},
		{"nil wallet id", func(a *args) { a.walletID = uuid.Nil }},
		{"nil player id", func(a *args) { a.playerID = uuid.Nil }},
		{"zero amount", func(a *args) { a.amount = zeroBRL }},
		{"negative amount", func(a *args) { a.amount = negative }},
		{"uninitialized amount", func(a *args) { a.amount = money.Money{} }},
		{"empty correlation id", func(a *args) { a.correlationID = "" }},
		{"zero time", func(a *args) { a.now = time.Time{} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := args{
				id: txID, walletID: uuid.MustParse(walletUUID), playerID: uuid.MustParse(playerUUID),
				amount: parsed(t, "100.00", "BRL"), correlationID: txCorrelationID, now: t0,
			}
			tc.edit(&a)

			tx, err := wager.NewOpening(a.id, a.walletID, a.playerID, a.amount, a.correlationID, a.now)
			assertInvalidTransaction(t, err)
			if tx != nil {
				t.Errorf("NewOpening() = %+v, want nil", tx.Snapshot())
			}
		})
	}
}

func TestNewExternal_StoresHash(t *testing.T) {
	tests := []struct {
		name string
		in   wager.CommandInput
	}{
		{"bet", betInput()},
		{"win", edited(func(in *wager.CommandInput) { in.Kind = "WIN" })},
		{"win with reference", edited(func(in *wager.CommandInput) {
			in.Kind = "WIN"
			in.ReferenceExternalTransactionID = "transaction-122"
		})},
		{"loss", edited(func(in *wager.CommandInput) {
			in.Kind = "LOSS"
			in.Amount = "0.00"
		})},
		{"refund", refundInput()},
		{"rollback", edited(func(in *wager.CommandInput) {
			in.Kind = "ROLLBACK"
			in.ReferenceExternalTransactionID = "transaction-122"
		})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newCommand(t, tc.in)

			tx, err := wager.NewExternal(txID, cmd, txCorrelationID, t0)
			if err != nil {
				t.Fatalf("NewExternal() error = %v", err)
			}

			want := wager.Snapshot{
				ID:                             txID,
				Origin:                         wager.OriginExternal,
				Kind:                           cmd.Kind,
				Status:                         wager.StatusPending,
				WalletID:                       cmd.WalletID,
				PlayerID:                       cmd.PlayerID,
				Money:                          cmd.Money,
				ProviderID:                     cmd.ProviderID,
				ExternalTransactionID:          cmd.ExternalTransactionID,
				IdempotencyKey:                 cmd.IdempotencyKey,
				PayloadHash:                    cmd.PayloadHash(),
				RoundID:                        cmd.RoundID,
				GameID:                         cmd.GameID,
				ReferenceExternalTransactionID: cmd.ReferenceExternalTransactionID,
				CorrelationID:                  txCorrelationID,
				CreatedAt:                      t0,
				UpdatedAt:                      t0,
			}
			assertSnapshot(t, tx.Snapshot(), want)
		})
	}

	t.Run("hash of the README command", func(t *testing.T) {
		tx, err := wager.NewExternal(txID, newCommand(t, betInput()), txCorrelationID, t0)
		if err != nil {
			t.Fatalf("NewExternal() error = %v", err)
		}
		if got := tx.PayloadHash(); got != readmeVector {
			t.Errorf("PayloadHash() = %s, want %s", got, readmeVector)
		}
	})
}

func TestNewExternal_Invalid(t *testing.T) {
	zeroBRL, err := money.Zero(money.BRL())
	if err != nil {
		t.Fatalf("money.Zero() error = %v", err)
	}
	negative, err := money.FromMinor(-100, money.BRL())
	if err != nil {
		t.Fatalf("money.FromMinor() error = %v", err)
	}

	type args struct {
		id            uuid.UUID
		cmd           wager.Command
		correlationID string
		now           time.Time
	}
	tests := []struct {
		name string
		edit func(a *args)
	}{
		{"nil id", func(a *args) { a.id = uuid.Nil }},
		{"empty correlation id", func(a *args) { a.correlationID = "" }},
		{"zero time", func(a *args) { a.now = time.Time{} }},
		{"zero command", func(a *args) { a.cmd = wager.Command{} }},
		{"nil player id", func(a *args) { a.cmd.PlayerID = uuid.Nil }},
		{"nil wallet id", func(a *args) { a.cmd.WalletID = uuid.Nil }},
		{"uninitialized money", func(a *args) { a.cmd.Money = money.Money{} }},
		{"negative money", func(a *args) { a.cmd.Money = negative }},
		{"zero money on a bet", func(a *args) { a.cmd.Money = zeroBRL }},
		{"positive money on a loss", func(a *args) { a.cmd.Kind = wager.KindLoss }},
		{"opening kind", func(a *args) { a.cmd.Kind = wager.KindOpening }},
		{"unknown kind", func(a *args) { a.cmd.Kind = "bet" }},
		{"empty kind", func(a *args) { a.cmd.Kind = "" }},
		{"empty provider id", func(a *args) { a.cmd.ProviderID = "" }},
		{"empty external transaction id", func(a *args) { a.cmd.ExternalTransactionID = "" }},
		{"empty idempotency key", func(a *args) { a.cmd.IdempotencyKey = "" }},
		{"empty round id", func(a *args) { a.cmd.RoundID = "" }},
		{"empty game id", func(a *args) { a.cmd.GameID = "" }},
		{"refund without reference", func(a *args) { a.cmd.Kind = wager.KindRefund }},
		{"rollback without reference", func(a *args) { a.cmd.Kind = wager.KindRollback }},
		{"bet with reference", func(a *args) { a.cmd.ReferenceExternalTransactionID = "transaction-122" }},
		{"loss with reference", func(a *args) {
			a.cmd.Kind = wager.KindLoss
			a.cmd.Money = zeroBRL
			a.cmd.ReferenceExternalTransactionID = "transaction-122"
		}},

		{"malformed provider id", func(a *args) { a.cmd.ProviderID = "provider a" }},
		{"malformed external transaction id", func(a *args) { a.cmd.ExternalTransactionID = "transaction/123" }},
		{"malformed round id", func(a *args) { a.cmd.RoundID = "round-987\n" }},
		{"malformed game id", func(a *args) { a.cmd.GameID = "fortune-chimp?" }},
		{"text id too long", func(a *args) { a.cmd.GameID = strings.Repeat("g", 129) }},
		{"malformed reference", func(a *args) {
			a.cmd.Kind = wager.KindWin
			a.cmd.ReferenceExternalTransactionID = "transaction 122"
		}},
		{"malformed idempotency key", func(a *args) { a.cmd.IdempotencyKey = "key with spaces" }},
		{"reference to itself", func(a *args) {
			a.cmd.Kind = wager.KindWin
			a.cmd.ReferenceExternalTransactionID = a.cmd.ExternalTransactionID
		}},
		{"idempotency key too long", func(a *args) { a.cmd.IdempotencyKey = strings.Repeat("k", 256) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := args{id: txID, cmd: newCommand(t, betInput()), correlationID: txCorrelationID, now: t0}
			tc.edit(&a)

			tx, err := wager.NewExternal(a.id, a.cmd, a.correlationID, a.now)
			assertInvalidTransaction(t, err)
			if tx != nil {
				t.Errorf("NewExternal() = %+v, want nil", tx.Snapshot())
			}
		})
	}
}

func TestTransition_Table(t *testing.T) {
	statuses := [...]wager.Status{
		wager.StatusPending,
		wager.StatusPendingReference,
		wager.StatusProcessed,
		wager.StatusRejected,
		wager.StatusFailed,
	}
	var (
		ok   error
		bad  = wager.ErrInvalidTransition
		term = wager.ErrTerminalState
	)
	grid := []struct {
		method string
		want   [len(statuses)]error
	}{
		{"MarkProcessed", [...]error{ok, ok, term, term, term}},
		{"Reject", [...]error{ok, ok, term, term, term}},
		{"AwaitReference", [...]error{ok, bad, term, term, term}},
		{"RescheduleReference", [...]error{bad, ok, term, term, term}},
		{"NotePermanentFailure", [...]error{ok, ok, term, term, term}},
	}

	byName := make(map[string]transition)
	for _, tr := range transitionsAt(t, t2) {
		byName[tr.name] = tr
	}
	for _, row := range grid {
		tr, found := byName[row.method]
		if !found {
			t.Fatalf("transition %q is not defined", row.method)
		}
		for i, from := range statuses {
			t.Run(row.method+" from "+string(from), func(t *testing.T) {
				tx := refundIn(t, from)
				before := tx.Snapshot()

				err := tr.do(tx)

				if !errors.Is(err, row.want[i]) {
					t.Fatalf("error = %v, want %v", err, row.want[i])
				}
				if err != nil {
					assertUnchanged(t, tx, before)
					return
				}
				if got := tx.Status(); got != tr.to {
					t.Errorf("Status() = %s, want %s", got, tr.to)
				}
			})
		}
	}
}

func TestTransition_StateErrorWinsOverBadArguments(t *testing.T) {
	tx := refundIn(t, wager.StatusProcessed)

	for name, err := range map[string]error{
		"MarkProcessed":        tx.MarkProcessed(wager.Result{}, time.Time{}),
		"Reject":               tx.Reject("", money.Money{}, -1, time.Time{}),
		"AwaitReference":       tx.AwaitReference(time.Time{}, time.Time{}, time.Time{}),
		"RescheduleReference":  tx.RescheduleReference(time.Time{}, time.Time{}),
		"NotePermanentFailure": tx.NotePermanentFailure(0, time.Time{}),
	} {
		if !errors.Is(err, wager.ErrTerminalState) {
			t.Errorf("%s error = %v, want ErrTerminalState", name, err)
		}
	}
}

func TestTransition_RejectsZeroTime(t *testing.T) {
	for _, tr := range transitionsAt(t, time.Time{}) {
		t.Run(tr.name, func(t *testing.T) {
			tx := refundIn(t, tr.from)
			before := tx.Snapshot()

			assertInvalidTransaction(t, tr.do(tx))
			assertUnchanged(t, tx, before)
		})
	}
}

func TestTransition_UpdatedAtNeverMovesBack(t *testing.T) {
	tx := refundIn(t, wager.StatusPendingReference)
	early := t0.Add(30 * time.Second)

	if err := tx.MarkProcessed(refundResult(t), early); err != nil {
		t.Fatalf("MarkProcessed() error = %v", err)
	}

	if got := tx.UpdatedAt(); !got.Equal(t1) {
		t.Errorf("UpdatedAt() = %v, want %v", got, t1)
	}
	if got := tx.ProcessedAt(); got == nil || !got.Equal(early) {
		t.Errorf("ProcessedAt() = %v, want %v: the transition keeps its own time", got, early)
	}
}

func TestMarkProcessed_RecordsResult(t *testing.T) {
	tx := refundIn(t, wager.StatusPendingReference)
	before := tx.Snapshot()

	if err := tx.MarkProcessed(refundResult(t), t2); err != nil {
		t.Fatalf("MarkProcessed() error = %v", err)
	}

	want := before
	want.Status = wager.StatusProcessed
	want.ResultBalance = ptrTo(parsed(t, "125.00", "BRL"))
	want.ResultWalletVersion = 4
	want.ReferenceTransactionID = uuid.NullUUID{UUID: refTxID, Valid: true}
	want.ProcessedAt = ptrTo(t2)
	want.NextAttemptAt, want.ExpiresAt = nil, nil
	want.UpdatedAt = t2
	assertSnapshot(t, tx.Snapshot(), want)
}

func TestMarkProcessed_WithoutReference(t *testing.T) {
	tx, err := wager.NewExternal(txID, newCommand(t, betInput()), txCorrelationID, t0)
	if err != nil {
		t.Fatalf("NewExternal() error = %v", err)
	}
	res := wager.Result{BalanceAfter: parsed(t, "75.00", "BRL"), WalletVersion: 2}

	if err := tx.MarkProcessed(res, t1); err != nil {
		t.Fatalf("MarkProcessed() error = %v", err)
	}

	if got := tx.ReferenceTransactionID(); got.Valid {
		t.Errorf("ReferenceTransactionID() = %v, want none", got)
	}
	if got := tx.Status(); got != wager.StatusProcessed {
		t.Errorf("Status() = %s, want PROCESSED", got)
	}
}

func TestMarkProcessed_OpeningTransaction(t *testing.T) {
	tx := newOpening(t)
	res := wager.Result{BalanceAfter: parsed(t, "100.00", "BRL"), WalletVersion: 1}

	if err := tx.MarkProcessed(res, t1); err != nil {
		t.Fatalf("MarkProcessed() error = %v", err)
	}

	if _, err := wager.Rehydrate(tx.Snapshot()); err != nil {
		t.Fatalf("Rehydrate() error = %v", err)
	}
}

func TestMarkProcessed_InvalidResult(t *testing.T) {
	negative, err := money.FromMinor(-1, money.BRL())
	if err != nil {
		t.Fatalf("money.FromMinor() error = %v", err)
	}

	tests := []struct {
		name string
		edit func(r *wager.Result)
	}{
		{"uninitialized balance", func(r *wager.Result) { r.BalanceAfter = money.Money{} }},
		{"negative balance", func(r *wager.Result) { r.BalanceAfter = negative }},
		{"balance in another currency", func(r *wager.Result) { r.BalanceAfter = parsed(t, "125.00", "USD") }},
		{"zero wallet version", func(r *wager.Result) { r.WalletVersion = 0 }},
		{"negative wallet version", func(r *wager.Result) { r.WalletVersion = -1 }},
		{"nil reference id", func(r *wager.Result) { r.ReferenceTransactionID = uuid.NullUUID{Valid: true} }},
		{"its own id as reference", func(r *wager.Result) {
			r.ReferenceTransactionID = uuid.NullUUID{UUID: txID, Valid: true}
		}},
		{"refund without the reference it resolved", func(r *wager.Result) { r.ReferenceTransactionID = uuid.NullUUID{} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := refundIn(t, wager.StatusPendingReference)
			before := tx.Snapshot()
			res := refundResult(t)
			tc.edit(&res)

			assertInvalidTransaction(t, tx.MarkProcessed(res, t2))
			assertUnchanged(t, tx, before)
		})
	}

	t.Run("reference id on a transaction with no reference", func(t *testing.T) {
		tx, err := wager.NewExternal(txID, newCommand(t, betInput()), txCorrelationID, t0)
		if err != nil {
			t.Fatalf("NewExternal() error = %v", err)
		}
		before := tx.Snapshot()

		assertInvalidTransaction(t, tx.MarkProcessed(refundResult(t), t1))
		assertUnchanged(t, tx, before)
	})
}

func TestReject_RequiresCode(t *testing.T) {
	tests := []struct {
		name string
		code wager.FailureCode
	}{
		{"empty", ""},
		{"unknown", "SOMETHING_ELSE"},
		{"wrong case", "insufficient_funds"},
		{"processing failed", wager.FailureProcessingFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := refundIn(t, wager.StatusPending)
			before := tx.Snapshot()

			err := tx.Reject(tc.code, parsed(t, "100.00", "BRL"), 3, t1)

			assertInvalidTransaction(t, err)
			assertUnchanged(t, tx, before)
		})
	}
}

func TestReject_AcceptsEveryRejectionCode(t *testing.T) {
	codes := []wager.FailureCode{
		wager.FailureInsufficientFunds,
		wager.FailureReversalInsufficientFunds,
		wager.FailureCurrencyMismatch,
		wager.FailureWalletPlayerMismatch,
		wager.FailureReferenceNotFound,
		wager.FailureReferenceNotProcessed,
		wager.FailureReferenceMismatch,
		wager.FailureInvalidReferenceKind,
		wager.FailureReversalAmountMismatch,
		wager.FailureReferenceAlreadyReversed,
		wager.FailureBalanceLimitExceeded,
	}
	for _, code := range codes {
		t.Run(string(code), func(t *testing.T) {
			tx := refundIn(t, wager.StatusPending)

			if err := tx.Reject(code, parsed(t, "100.00", "BRL"), 3, t1); err != nil {
				t.Fatalf("Reject() error = %v", err)
			}

			if got := tx.FailureCode(); got != code {
				t.Errorf("FailureCode() = %q, want %q", got, code)
			}
		})
	}
}

func TestReject_RecordsOutcome(t *testing.T) {
	tx := refundIn(t, wager.StatusPendingReference)
	before := tx.Snapshot()

	err := tx.Reject(wager.FailureReferenceNotFound, parsed(t, "100.00", "BRL"), 3, t2)
	if err != nil {
		t.Fatalf("Reject() error = %v", err)
	}

	want := before
	want.Status = wager.StatusRejected
	want.FailureCode = wager.FailureReferenceNotFound
	want.ResultBalance = ptrTo(parsed(t, "100.00", "BRL"))
	want.ResultWalletVersion = 3
	want.ProcessedAt = ptrTo(t2)
	want.NextAttemptAt, want.ExpiresAt = nil, nil
	want.UpdatedAt = t2
	assertSnapshot(t, tx.Snapshot(), want)
}

func TestReject_ObservedBalance(t *testing.T) {
	negative, err := money.FromMinor(-1, money.BRL())
	if err != nil {
		t.Fatalf("money.FromMinor() error = %v", err)
	}
	zeroBRL, err := money.Zero(money.BRL())
	if err != nil {
		t.Fatalf("money.Zero() error = %v", err)
	}

	tests := []struct {
		name        string
		observed    money.Money
		version     int64
		wantBalance *money.Money
		wantInvalid bool
	}{
		{name: "balance of the wallet", observed: parsed(t, "100.00", "BRL"), version: 3,
			wantBalance: ptrTo(parsed(t, "100.00", "BRL"))},
		{name: "empty wallet", observed: zeroBRL, version: 1, wantBalance: &zeroBRL},
		{name: "no balance observed", observed: money.Money{}, version: 0},
		{name: "balance without version", observed: parsed(t, "100.00", "BRL"), version: 0, wantInvalid: true},
		{name: "negative version", observed: parsed(t, "100.00", "BRL"), version: -1, wantInvalid: true},
		{name: "version without balance", observed: money.Money{}, version: 3, wantInvalid: true},
		{name: "negative balance", observed: negative, version: 3, wantInvalid: true},
		{name: "balance in another currency", observed: parsed(t, "100.00", "USD"), version: 3, wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := refundIn(t, wager.StatusPending)
			before := tx.Snapshot()

			err := tx.Reject(wager.FailureInsufficientFunds, tc.observed, tc.version, t1)

			if tc.wantInvalid {
				assertInvalidTransaction(t, err)
				assertUnchanged(t, tx, before)
				return
			}
			if err != nil {
				t.Fatalf("Reject() error = %v", err)
			}
			if got := tx.ResultBalance(); !reflect.DeepEqual(got, tc.wantBalance) {
				t.Errorf("ResultBalance() = %v, want %v", deref(got), deref(tc.wantBalance))
			}
			if got := tx.ResultWalletVersion(); got != tc.version {
				t.Errorf("ResultWalletVersion() = %d, want %d", got, tc.version)
			}
			if _, err := wager.Rehydrate(tx.Snapshot()); err != nil {
				t.Errorf("Rehydrate() error = %v", err)
			}
		})
	}
}

func TestAwaitReference_RequiresFutureTimes(t *testing.T) {
	next, expires := t1.Add(time.Second), t1.Add(15*time.Minute)

	tests := []struct {
		name          string
		next, expires time.Time
		wantInvalid   bool
	}{
		{name: "future times", next: next, expires: expires},
		{name: "expiry as soon as the first attempt", next: next, expires: next},
		{name: "next attempt now", next: t1, expires: expires, wantInvalid: true},
		{name: "next attempt in the past", next: t0, expires: expires, wantInvalid: true},
		{name: "no next attempt", next: time.Time{}, expires: expires, wantInvalid: true},
		{name: "expiry now", next: next, expires: t1, wantInvalid: true},
		{name: "expiry in the past", next: next, expires: t0, wantInvalid: true},
		{name: "no expiry", next: next, expires: time.Time{}, wantInvalid: true},
		{name: "expiry before the first attempt", next: expires, expires: next, wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := refundIn(t, wager.StatusPending)
			before := tx.Snapshot()

			err := tx.AwaitReference(tc.next, tc.expires, t1)

			if tc.wantInvalid {
				assertInvalidTransaction(t, err)
				assertUnchanged(t, tx, before)
				return
			}
			if err != nil {
				t.Fatalf("AwaitReference() error = %v", err)
			}
			want := before
			want.Status = wager.StatusPendingReference
			want.NextAttemptAt, want.ExpiresAt = ptrTo(tc.next), ptrTo(tc.expires)
			want.UpdatedAt = t1
			assertSnapshot(t, tx.Snapshot(), want)
		})
	}
}

func TestAwaitReference_RequiresReference(t *testing.T) {
	tests := []struct {
		name string
		in   wager.CommandInput
	}{
		{"bet", betInput()},
		{"win without reference", edited(func(in *wager.CommandInput) { in.Kind = "WIN" })},
		{"loss", edited(func(in *wager.CommandInput) {
			in.Kind = "LOSS"
			in.Amount = "0.00"
		})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := wager.NewExternal(txID, newCommand(t, tc.in), txCorrelationID, t0)
			if err != nil {
				t.Fatalf("NewExternal() error = %v", err)
			}
			before := tx.Snapshot()

			assertInvalidTransaction(t, tx.AwaitReference(t1.Add(time.Second), t1.Add(15*time.Minute), t1))
			assertUnchanged(t, tx, before)
		})
	}
}

func TestRescheduleReference(t *testing.T) {
	tx := refundIn(t, wager.StatusPendingReference)
	expires := tx.ExpiresAt()

	next1, next2 := t2.Add(2*time.Second), t2.Add(4*time.Second)
	for i, next := range []time.Time{next1, next2} {
		if err := tx.RescheduleReference(next, t2); err != nil {
			t.Fatalf("RescheduleReference() #%d error = %v", i+1, err)
		}

		if got := tx.ReferenceAttempts(); got != i+1 {
			t.Errorf("ReferenceAttempts() = %d, want %d", got, i+1)
		}
		if got := tx.NextAttemptAt(); got == nil || !got.Equal(next) {
			t.Errorf("NextAttemptAt() = %v, want %v", got, next)
		}
		if got := tx.Status(); got != wager.StatusPendingReference {
			t.Errorf("Status() = %s, want PENDING_REFERENCE", got)
		}
	}
	if got := tx.ExpiresAt(); !reflect.DeepEqual(got, expires) {
		t.Errorf("ExpiresAt() = %v, want %v", deref(got), deref(expires))
	}
	if got := tx.UpdatedAt(); !got.Equal(t2) {
		t.Errorf("UpdatedAt() = %v, want %v", got, t2)
	}
}

func TestRescheduleReference_RequiresFutureTime(t *testing.T) {
	tests := []struct {
		name string
		next time.Time
	}{
		{"now", t2},
		{"past", t1},
		{"zero", time.Time{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := refundIn(t, wager.StatusPendingReference)
			before := tx.Snapshot()

			assertInvalidTransaction(t, tx.RescheduleReference(tc.next, t2))
			assertUnchanged(t, tx, before)
		})
	}
}

func TestNotePermanentFailure_FailsAtLimit(t *testing.T) {
	const limit = 3

	for _, from := range []wager.Status{wager.StatusPending, wager.StatusPendingReference} {
		t.Run(string(from), func(t *testing.T) {
			tx := refundIn(t, from)

			for i := 1; i < limit; i++ {
				if err := tx.NotePermanentFailure(limit, t2); err != nil {
					t.Fatalf("NotePermanentFailure() #%d error = %v", i, err)
				}
				if got := tx.Status(); got != from {
					t.Fatalf("after %d failures Status() = %s, want %s", i, got, from)
				}
				if got := tx.PermanentFailures(); got != i {
					t.Fatalf("PermanentFailures() = %d, want %d", got, i)
				}
				if got := tx.FailureCode(); got != "" {
					t.Fatalf("after %d failures FailureCode() = %q, want none", i, got)
				}
			}

			before := tx.Snapshot()
			if err := tx.NotePermanentFailure(limit, t2); err != nil {
				t.Fatalf("NotePermanentFailure() #%d error = %v", limit, err)
			}

			want := before
			want.Status = wager.StatusFailed
			want.FailureCode = wager.FailureProcessingFailed
			want.PermanentFailures = limit
			want.ProcessedAt = ptrTo(t2)
			want.NextAttemptAt, want.ExpiresAt = nil, nil
			want.UpdatedAt = t2
			assertSnapshot(t, tx.Snapshot(), want)

			if err := tx.NotePermanentFailure(limit, t2); !errors.Is(err, wager.ErrTerminalState) {
				t.Errorf("NotePermanentFailure() on a FAILED transaction error = %v, want ErrTerminalState", err)
			}
		})
	}
}

func TestNotePermanentFailure_LimitOfOne(t *testing.T) {
	tx := refundIn(t, wager.StatusPending)

	if err := tx.NotePermanentFailure(1, t1); err != nil {
		t.Fatalf("NotePermanentFailure() error = %v", err)
	}

	if got := tx.Status(); got != wager.StatusFailed {
		t.Errorf("Status() = %s, want FAILED", got)
	}
	if got := tx.ReferenceAttempts(); got != 0 {
		t.Errorf("ReferenceAttempts() = %d, want 0", got)
	}
}

func TestNotePermanentFailure_InvalidLimit(t *testing.T) {
	for _, limit := range []int{0, -1} {
		tx := refundIn(t, wager.StatusPending)
		before := tx.Snapshot()

		assertInvalidTransaction(t, tx.NotePermanentFailure(limit, t1))
		assertUnchanged(t, tx, before)
	}
}

func TestZeroTransaction_FailsInsteadOfPanicking(t *testing.T) {
	var tx wager.Transaction

	for _, tr := range transitionsAt(t, t1) {
		if err := tr.do(&tx); !errors.Is(err, wager.ErrInvalidTransition) {
			t.Errorf("%s on the zero Transaction error = %v, want ErrInvalidTransition", tr.name, err)
		}
	}
}

func TestRehydrate_DoesNotReapply(t *testing.T) {
	inFlight := refundIn(t, wager.StatusPendingReference)
	for i, next := range []time.Time{t2.Add(time.Second), t2.Add(2 * time.Second)} {
		if err := inFlight.RescheduleReference(next, t2); err != nil {
			t.Fatalf("RescheduleReference() #%d error = %v", i+1, err)
		}
	}
	if err := inFlight.NotePermanentFailure(3, t2); err != nil {
		t.Fatalf("NotePermanentFailure() error = %v", err)
	}

	opening := newOpening(t)
	if err := opening.MarkProcessed(wager.Result{BalanceAfter: parsed(t, "100.00", "BRL"), WalletVersion: 1}, t1); err != nil {
		t.Fatalf("MarkProcessed() error = %v", err)
	}

	tests := []struct {
		name string
		tx   *wager.Transaction
	}{
		{"pending", refundIn(t, wager.StatusPending)},
		{"pending reference", refundIn(t, wager.StatusPendingReference)},
		{"pending reference with counters", inFlight},
		{"processed", refundIn(t, wager.StatusProcessed)},
		{"rejected", refundIn(t, wager.StatusRejected)},
		{"failed", refundIn(t, wager.StatusFailed)},
		{"opening", opening},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.tx.Snapshot()

			got, err := wager.Rehydrate(before)
			if err != nil {
				t.Fatalf("Rehydrate() error = %v", err)
			}

			assertSnapshot(t, got.Snapshot(), before)
			if got.Status() != before.Status {
				t.Errorf("Status() = %s, want %s", got.Status(), before.Status)
			}
			if got.ReferenceAttempts() != before.ReferenceAttempts || got.PermanentFailures() != before.PermanentFailures {
				t.Errorf("counters = %d and %d, want %d and %d", got.ReferenceAttempts(), got.PermanentFailures(),
					before.ReferenceAttempts, before.PermanentFailures)
			}
		})
	}
}

func TestRehydrate_AcceptsStoredStates(t *testing.T) {
	tests := []struct {
		name string
		from wager.Status
		edit func(s *wager.Snapshot)
	}{
		{"pending that was claimed", wager.StatusPending, func(s *wager.Snapshot) { s.NextAttemptAt = ptrTo(t2) }},
		{"next attempt after the expiry", wager.StatusPendingReference, func(s *wager.Snapshot) {
			s.NextAttemptAt = ptrTo(s.ExpiresAt.Add(time.Minute))
		}},
		{"rejection that observed no balance", wager.StatusRejected, func(s *wager.Snapshot) {
			s.ResultBalance, s.ResultWalletVersion = nil, 0
		}},
		{"processed before it was created", wager.StatusProcessed, func(s *wager.Snapshot) {
			s.ProcessedAt = ptrTo(s.CreatedAt.Add(-time.Second))
		}},
		{"identifier outside the input format", wager.StatusProcessed, func(s *wager.Snapshot) {
			s.RoundID = "round 987"
		}},
		{"reference to itself", wager.StatusPendingReference, func(s *wager.Snapshot) {
			s.ReferenceExternalTransactionID = s.ExternalTransactionID
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := refundIn(t, tc.from).Snapshot()
			tc.edit(&s)

			if _, err := wager.Rehydrate(s); err != nil {
				t.Fatalf("Rehydrate() error = %v", err)
			}
		})
	}
}

func TestRehydrate_Invalid(t *testing.T) {
	zeroBRL, err := money.Zero(money.BRL())
	if err != nil {
		t.Fatalf("money.Zero() error = %v", err)
	}
	negative, err := money.FromMinor(-100, money.BRL())
	if err != nil {
		t.Fatalf("money.FromMinor() error = %v", err)
	}

	toOpening := func(s *wager.Snapshot) {
		s.Origin, s.Kind = wager.OriginInternal, wager.KindOpening
		s.ProviderID, s.ExternalTransactionID, s.IdempotencyKey, s.PayloadHash = "", "", "", ""
		s.RoundID, s.GameID, s.ReferenceExternalTransactionID = "", "", ""
	}
	asBet := func(s *wager.Snapshot) {
		s.Kind = wager.KindBet
		s.ReferenceExternalTransactionID = ""
	}

	tests := []struct {
		name string
		from wager.Status
		edit func(s *wager.Snapshot)
	}{
		{"nil id", wager.StatusPending, func(s *wager.Snapshot) { s.ID = uuid.Nil }},
		{"nil wallet id", wager.StatusPending, func(s *wager.Snapshot) { s.WalletID = uuid.Nil }},
		{"nil player id", wager.StatusPending, func(s *wager.Snapshot) { s.PlayerID = uuid.Nil }},
		{"empty correlation id", wager.StatusPending, func(s *wager.Snapshot) { s.CorrelationID = "" }},
		{"zero created at", wager.StatusPending, func(s *wager.Snapshot) { s.CreatedAt = time.Time{} }},
		{"zero updated at", wager.StatusPending, func(s *wager.Snapshot) { s.UpdatedAt = time.Time{} }},
		{"updated before created", wager.StatusPending, func(s *wager.Snapshot) {
			s.UpdatedAt = s.CreatedAt.Add(-time.Second)
		}},
		{"negative reference attempts", wager.StatusPending, func(s *wager.Snapshot) { s.ReferenceAttempts = -1 }},
		{"negative permanent failures", wager.StatusPending, func(s *wager.Snapshot) { s.PermanentFailures = -1 }},
		{"unknown status", wager.StatusPending, func(s *wager.Snapshot) { s.Status = "CANCELED" }},
		{"empty status", wager.StatusPending, func(s *wager.Snapshot) { s.Status = "" }},

		{"unknown origin", wager.StatusPending, func(s *wager.Snapshot) { s.Origin = "SYSTEM" }},
		{"internal origin that is not an opening", wager.StatusPending, func(s *wager.Snapshot) {
			toOpening(s)
			s.Kind = wager.KindBet
		}},
		{"opening with a provider id", wager.StatusPending, func(s *wager.Snapshot) {
			toOpening(s)
			s.ProviderID = "provider-a"
		}},
		{"opening with an external transaction id", wager.StatusPending, func(s *wager.Snapshot) {
			toOpening(s)
			s.ExternalTransactionID = "transaction-123"
		}},
		{"opening with an idempotency key", wager.StatusPending, func(s *wager.Snapshot) {
			toOpening(s)
			s.IdempotencyKey = "key"
		}},
		{"opening with a payload hash", wager.StatusPending, func(s *wager.Snapshot) {
			toOpening(s)
			s.PayloadHash = readmeVector
		}},
		{"opening with a round id", wager.StatusPending, func(s *wager.Snapshot) {
			toOpening(s)
			s.RoundID = "round-987"
		}},
		{"opening with a game id", wager.StatusPending, func(s *wager.Snapshot) {
			toOpening(s)
			s.GameID = "fortune-chimp"
		}},
		{"opening with a reference", wager.StatusPending, func(s *wager.Snapshot) {
			toOpening(s)
			s.ReferenceExternalTransactionID = "transaction-122"
		}},
		{"external opening", wager.StatusPending, func(s *wager.Snapshot) { s.Kind = wager.KindOpening }},
		{"external with an unknown kind", wager.StatusPending, func(s *wager.Snapshot) { s.Kind = "CASHOUT" }},
		{"external without provider id", wager.StatusPending, func(s *wager.Snapshot) { s.ProviderID = "" }},
		{"external without external transaction id", wager.StatusPending, func(s *wager.Snapshot) {
			s.ExternalTransactionID = ""
		}},
		{"external without idempotency key", wager.StatusPending, func(s *wager.Snapshot) { s.IdempotencyKey = "" }},
		{"external without payload hash", wager.StatusPending, func(s *wager.Snapshot) { s.PayloadHash = "" }},
		{"external without round id", wager.StatusPending, func(s *wager.Snapshot) { s.RoundID = "" }},
		{"external without game id", wager.StatusPending, func(s *wager.Snapshot) { s.GameID = "" }},

		{"uninitialized money", wager.StatusPending, func(s *wager.Snapshot) { s.Money = money.Money{} }},
		{"negative money", wager.StatusPending, func(s *wager.Snapshot) { s.Money = negative }},
		{"zero money on a bet", wager.StatusPending, func(s *wager.Snapshot) {
			asBet(s)
			s.Money = zeroBRL
		}},
		{"positive money on a loss", wager.StatusPending, func(s *wager.Snapshot) {
			s.Kind = wager.KindLoss
			s.ReferenceExternalTransactionID = ""
		}},
		{"refund without reference", wager.StatusPending, func(s *wager.Snapshot) { s.ReferenceExternalTransactionID = "" }},
		{"rollback without reference", wager.StatusPending, func(s *wager.Snapshot) {
			s.Kind = wager.KindRollback
			s.ReferenceExternalTransactionID = ""
		}},
		{"bet with reference", wager.StatusPending, func(s *wager.Snapshot) {
			s.Kind = wager.KindBet
		}},
		{"loss with reference", wager.StatusPending, func(s *wager.Snapshot) {
			s.Kind = wager.KindLoss
			s.Money = zeroBRL
		}},

		{"failure code on a pending transaction", wager.StatusPending, func(s *wager.Snapshot) {
			s.FailureCode = wager.FailureInsufficientFunds
		}},
		{"failure code on a processed transaction", wager.StatusProcessed, func(s *wager.Snapshot) {
			s.FailureCode = wager.FailureInsufficientFunds
		}},
		{"rejected without code", wager.StatusRejected, func(s *wager.Snapshot) { s.FailureCode = "" }},
		{"rejected with an unknown code", wager.StatusRejected, func(s *wager.Snapshot) { s.FailureCode = "NOPE" }},
		{"rejected with the code of a failure", wager.StatusRejected, func(s *wager.Snapshot) {
			s.FailureCode = wager.FailureProcessingFailed
		}},
		{"failed without code", wager.StatusFailed, func(s *wager.Snapshot) { s.FailureCode = "" }},
		{"failed with a rejection code", wager.StatusFailed, func(s *wager.Snapshot) {
			s.FailureCode = wager.FailureInsufficientFunds
		}},

		{"pending reference without next attempt", wager.StatusPendingReference, func(s *wager.Snapshot) {
			s.NextAttemptAt = nil
		}},
		{"pending reference without expiry", wager.StatusPendingReference, func(s *wager.Snapshot) { s.ExpiresAt = nil }},
		{"zero next attempt", wager.StatusPendingReference, func(s *wager.Snapshot) { s.NextAttemptAt = ptrTo(time.Time{}) }},
		{"zero expiry", wager.StatusPendingReference, func(s *wager.Snapshot) { s.ExpiresAt = ptrTo(time.Time{}) }},
		{"waiting with no reference to wait for", wager.StatusPendingReference, asBet},

		{"processed without processed at", wager.StatusProcessed, func(s *wager.Snapshot) { s.ProcessedAt = nil }},
		{"rejected without processed at", wager.StatusRejected, func(s *wager.Snapshot) { s.ProcessedAt = nil }},
		{"failed without processed at", wager.StatusFailed, func(s *wager.Snapshot) { s.ProcessedAt = nil }},
		{"pending with processed at", wager.StatusPending, func(s *wager.Snapshot) { s.ProcessedAt = ptrTo(t1) }},
		{"zero processed at", wager.StatusProcessed, func(s *wager.Snapshot) { s.ProcessedAt = ptrTo(time.Time{}) }},

		{"processed without result", wager.StatusProcessed, func(s *wager.Snapshot) {
			s.ResultBalance, s.ResultWalletVersion = nil, 0
		}},
		{"result balance without version", wager.StatusProcessed, func(s *wager.Snapshot) { s.ResultWalletVersion = 0 }},
		{"result version without balance", wager.StatusRejected, func(s *wager.Snapshot) { s.ResultBalance = nil }},
		{"negative result version", wager.StatusProcessed, func(s *wager.Snapshot) { s.ResultWalletVersion = -1 }},
		{"result on a pending transaction", wager.StatusPending, func(s *wager.Snapshot) {
			s.ResultBalance, s.ResultWalletVersion = ptrTo(parsed(t, "100.00", "BRL")), 1
		}},
		{"result on a failed transaction", wager.StatusFailed, func(s *wager.Snapshot) {
			s.ResultBalance, s.ResultWalletVersion = ptrTo(parsed(t, "100.00", "BRL")), 1
		}},
		{"result in another currency", wager.StatusProcessed, func(s *wager.Snapshot) {
			s.ResultBalance = ptrTo(parsed(t, "125.00", "USD"))
		}},
		{"negative result balance", wager.StatusProcessed, func(s *wager.Snapshot) { s.ResultBalance = ptrTo(negative) }},
		{"uninitialized result balance", wager.StatusProcessed, func(s *wager.Snapshot) {
			s.ResultBalance = ptrTo(money.Money{})
		}},

		{"nil reference transaction id", wager.StatusProcessed, func(s *wager.Snapshot) {
			s.ReferenceTransactionID = uuid.NullUUID{Valid: true}
		}},
		{"its own id as reference", wager.StatusProcessed, func(s *wager.Snapshot) {
			s.ReferenceTransactionID = uuid.NullUUID{UUID: s.ID, Valid: true}
		}},
		{"reference id without an external reference", wager.StatusProcessed, func(s *wager.Snapshot) {
			asBet(s)
		}},
		{"processed refund without the reference it resolved", wager.StatusProcessed, func(s *wager.Snapshot) {
			s.ReferenceTransactionID = uuid.NullUUID{}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := refundIn(t, tc.from).Snapshot()
			tc.edit(&s)

			tx, err := wager.Rehydrate(s)

			assertInvalidTransaction(t, err)
			if tx != nil {
				t.Errorf("Rehydrate() = %+v, want nil", tx.Snapshot())
			}
		})
	}
}

func TestRehydrate_KeepsItsOwnCopy(t *testing.T) {
	s := refundIn(t, wager.StatusPendingReference).Snapshot()
	tx, err := wager.Rehydrate(s)
	if err != nil {
		t.Fatalf("Rehydrate() error = %v", err)
	}
	want := tx.Snapshot()

	*s.NextAttemptAt = t0
	*s.ExpiresAt = t0
	assertSnapshot(t, tx.Snapshot(), want)
}

func TestTransaction_HandsOutCopies(t *testing.T) {
	processed := refundIn(t, wager.StatusProcessed)
	pending := refundIn(t, wager.StatusPendingReference)
	wantProcessed, wantPending := processed.Snapshot(), pending.Snapshot()

	snapshot := processed.Snapshot()
	*snapshot.ResultBalance = money.Money{}
	*snapshot.ProcessedAt = t0
	*processed.ResultBalance() = money.Money{}
	*processed.ProcessedAt() = t0
	*pending.NextAttemptAt() = t0
	*pending.ExpiresAt() = t0

	assertSnapshot(t, processed.Snapshot(), wantProcessed)
	assertSnapshot(t, pending.Snapshot(), wantPending)
}

func TestTransaction_Getters(t *testing.T) {
	tests := []struct {
		name string
		tx   *wager.Transaction
	}{
		{"pending", refundIn(t, wager.StatusPending)},
		{"pending reference", refundIn(t, wager.StatusPendingReference)},
		{"processed", refundIn(t, wager.StatusProcessed)},
		{"rejected", refundIn(t, wager.StatusRejected)},
		{"failed", refundIn(t, wager.StatusFailed)},
		{"opening", newOpening(t)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx, s := tc.tx, tc.tx.Snapshot()

			got := wager.Snapshot{
				ID:                             tx.ID(),
				Origin:                         tx.Origin(),
				Kind:                           tx.Kind(),
				Status:                         tx.Status(),
				WalletID:                       tx.WalletID(),
				PlayerID:                       tx.PlayerID(),
				Money:                          tx.Money(),
				ProviderID:                     tx.ProviderID(),
				ExternalTransactionID:          tx.ExternalTransactionID(),
				IdempotencyKey:                 tx.IdempotencyKey(),
				PayloadHash:                    tx.PayloadHash(),
				RoundID:                        tx.RoundID(),
				GameID:                         tx.GameID(),
				ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID(),
				ReferenceTransactionID:         tx.ReferenceTransactionID(),
				FailureCode:                    tx.FailureCode(),
				ResultBalance:                  tx.ResultBalance(),
				ResultWalletVersion:            tx.ResultWalletVersion(),
				CorrelationID:                  tx.CorrelationID(),
				ReferenceAttempts:              tx.ReferenceAttempts(),
				PermanentFailures:              tx.PermanentFailures(),
				NextAttemptAt:                  tx.NextAttemptAt(),
				ExpiresAt:                      tx.ExpiresAt(),
				CreatedAt:                      tx.CreatedAt(),
				UpdatedAt:                      tx.UpdatedAt(),
				ProcessedAt:                    tx.ProcessedAt(),
			}
			assertSnapshot(t, got, s)
		})
	}
}
