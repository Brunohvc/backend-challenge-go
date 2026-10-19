package wager_test

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

var (
	entryID    = uuid.MustParse("0198b000-0000-7000-8000-0000000000e1")
	priorTxID  = uuid.MustParse("0198b000-0000-7000-8000-000000000003")
	strangerID = uuid.MustParse("0198b000-0000-7000-8000-0000000000ff")

	decidedAt = t2
)

var defaultPolicy = wager.ReferencePolicy{
	MaxAttempts: 10,
	TTL:         15 * time.Minute,
	BackoffBase: time.Second,
	BackoffMax:  time.Minute,
}

func opInput(kind wager.Kind, amount, ref string) wager.CommandInput {
	return edited(func(in *wager.CommandInput) {
		in.ExternalTransactionID = "transaction-200"
		in.Kind = string(kind)
		in.Amount = amount
		in.ReferenceExternalTransactionID = ref
	})
}

func reversalOf(kind wager.Kind) wager.CommandInput {
	return opInput(kind, "25.00", "transaction-123")
}

func pendingOp(t *testing.T, in wager.CommandInput) *wager.Transaction {
	t.Helper()
	tx, err := wager.NewExternal(txID, newCommand(t, in), txCorrelationID, t0)
	if err != nil {
		t.Fatalf("NewExternal() error = %v", err)
	}
	return tx
}

func waitingOp(t *testing.T, in wager.CommandInput, attempts int) *wager.Transaction {
	t.Helper()
	tx := pendingOp(t, in)
	if err := tx.AwaitReference(t0.Add(time.Second), t0.Add(defaultPolicy.TTL), t0); err != nil {
		t.Fatalf("AwaitReference() error = %v", err)
	}
	s := tx.Snapshot()
	s.ReferenceAttempts = attempts
	tx, err := wager.Rehydrate(s)
	if err != nil {
		t.Fatalf("Rehydrate() error = %v", err)
	}
	return tx
}

func referenceInput(kind wager.Kind) wager.CommandInput {
	return edited(func(in *wager.CommandInput) {
		in.Kind = string(kind)
		switch kind {
		case wager.KindLoss:
			in.Amount = "0.00"
		case wager.KindRefund, wager.KindRollback:
			in.ReferenceExternalTransactionID = "transaction-100"
		}
	})
}

func reference(t *testing.T, in wager.CommandInput, status wager.Status) *wager.Transaction {
	t.Helper()
	tx, err := wager.NewExternal(refTxID, newCommand(t, in), txCorrelationID, t0)
	if err != nil {
		t.Fatalf("NewExternal() error = %v", err)
	}
	switch status {
	case wager.StatusPending:
	case wager.StatusPendingReference:
		err = tx.AwaitReference(t1.Add(time.Second), t1.Add(15*time.Minute), t1)
	case wager.StatusProcessed:
		r := wager.Result{BalanceAfter: parsed(t, "100.00", in.Currency), WalletVersion: 1}
		if in.ReferenceExternalTransactionID != "" {
			r.ReferenceTransactionID = uuid.NullUUID{UUID: priorTxID, Valid: true}
		}
		err = tx.MarkProcessed(r, t1)
	case wager.StatusRejected:
		err = tx.Reject(wager.FailureInsufficientFunds, parsed(t, "100.00", in.Currency), 1, t1)
	case wager.StatusFailed:
		err = tx.NotePermanentFailure(1, t1)
	default:
		t.Fatalf("reference: unknown status %q", status)
	}
	if err != nil {
		t.Fatalf("taking the reference to %s: %v", status, err)
	}
	return tx
}

func processedRef(t *testing.T, kind wager.Kind) *wager.Transaction {
	t.Helper()
	return reference(t, referenceInput(kind), wager.StatusProcessed)
}

func walletWith(t *testing.T, edit func(s *wallet.Snapshot)) *wallet.Wallet {
	t.Helper()
	s := wallet.Snapshot{
		ID:        uuid.MustParse(walletUUID),
		PlayerID:  uuid.MustParse(playerUUID),
		Balance:   parsed(t, "100.00", "BRL"),
		Version:   1,
		CreatedAt: t0,
		UpdatedAt: t0,
	}
	if edit != nil {
		edit(&s)
	}
	w, err := wallet.Rehydrate(s)
	if err != nil {
		t.Fatalf("wallet.Rehydrate() error = %v", err)
	}
	return w
}

func holding(t *testing.T, amount string) *wallet.Wallet {
	t.Helper()
	balance := parsed(t, amount, "BRL")
	return walletWith(t, func(s *wallet.Snapshot) { s.Balance = balance })
}

func decisionFor(tx *wager.Transaction, w *wallet.Wallet, ref *wager.Transaction) wager.DecisionInput {
	return wager.DecisionInput{
		Tx:        tx,
		Wallet:    w,
		Reference: ref,
		Policy:    defaultPolicy,
		EntryID:   entryID,
		Now:       decidedAt,
	}
}

func decide(t *testing.T, in wager.DecisionInput) wager.Decision {
	t.Helper()
	d, err := wager.Decide(in)
	if err != nil {
		t.Fatalf("Decide() error = %v", err)
	}
	return d
}

type outcome struct {
	status     wager.Status
	code       wager.FailureCode
	balance    string
	version    int64
	entry      wallet.Direction
	entered    bool
	unobserved bool
}

var (
	credited = outcome{status: wager.StatusProcessed, balance: "125.00", version: 2, entry: wallet.DirectionCredit}
	debited  = outcome{status: wager.StatusProcessed, balance: "75.00", version: 2, entry: wallet.DirectionDebit}
	waits    = outcome{status: wager.StatusPendingReference, balance: "100.00", version: 1, entered: true}
)

func rejected(code wager.FailureCode) outcome {
	return outcome{status: wager.StatusRejected, code: code, balance: "100.00", version: 1}
}

func assertOutcome(t *testing.T, in wager.DecisionInput, d wager.Decision, want outcome) {
	t.Helper()
	tx, w := in.Tx, in.Wallet

	if got := tx.Status(); got != want.status {
		t.Errorf("Status() = %s, want %s", got, want.status)
	}
	if got := tx.FailureCode(); got != want.code {
		t.Errorf("FailureCode() = %q, want %q", got, want.code)
	}
	wantBalance := parsed(t, want.balance, w.Currency().Code())
	if got := w.Balance(); !got.Equal(wantBalance) {
		t.Errorf("wallet Balance() = %s, want %s", got, wantBalance)
	}
	if got := w.Version(); got != want.version {
		t.Errorf("wallet Version() = %d, want %d", got, want.version)
	}
	if d.EnteredPendingReference != want.entered {
		t.Errorf("EnteredPendingReference = %v, want %v", d.EnteredPendingReference, want.entered)
	}

	switch {
	case want.entry == "" && d.Entry != nil:
		t.Errorf("Entry = %+v, want none", *d.Entry)
	case want.entry != "" && d.Entry == nil:
		t.Errorf("Entry = nil, want a %s", want.entry)
	case want.entry != "":
		assertEntry(t, *d.Entry, in, want.entry)
	}

	var wantResult *money.Money
	var wantVersion int64
	if (want.status == wager.StatusProcessed || want.status == wager.StatusRejected) && !want.unobserved {
		wantResult, wantVersion = &wantBalance, want.version
	}
	if got := tx.ResultBalance(); !reflect.DeepEqual(got, wantResult) {
		t.Errorf("ResultBalance() = %v, want %v", deref(got), deref(wantResult))
	}
	if got := tx.ResultWalletVersion(); got != wantVersion {
		t.Errorf("ResultWalletVersion() = %d, want %d", got, wantVersion)
	}

	var wantRef uuid.NullUUID
	if want.status == wager.StatusProcessed && in.Reference != nil {
		wantRef = uuid.NullUUID{UUID: in.Reference.ID(), Valid: true}
	}
	if got := tx.ReferenceTransactionID(); got != wantRef {
		t.Errorf("ReferenceTransactionID() = %v, want %v", got, wantRef)
	}
	if tx.Status().IsTerminal() {
		if got := tx.ProcessedAt(); got == nil || !got.Equal(in.Now) {
			t.Errorf("ProcessedAt() = %v, want %v", deref(got), in.Now)
		}
	}

	if _, err := wager.Rehydrate(tx.Snapshot()); err != nil {
		t.Errorf("wager.Rehydrate() error = %v", err)
	}
	if _, err := wallet.Rehydrate(w.Snapshot()); err != nil {
		t.Errorf("wallet.Rehydrate() error = %v", err)
	}
}

func assertEntry(t *testing.T, e wallet.LedgerEntry, in wager.DecisionInput, dir wallet.Direction) {
	t.Helper()
	tx, w := in.Tx, in.Wallet
	if e.ID() != entryID || e.WalletID() != w.ID() || e.TransactionID() != tx.ID() {
		t.Errorf("entry ids = (%s, %s, %s), want (%s, %s, %s)",
			e.ID(), e.WalletID(), e.TransactionID(), entryID, w.ID(), tx.ID())
	}
	if got := e.Direction(); got != dir {
		t.Errorf("entry Direction() = %s, want %s", got, dir)
	}
	if got := e.Amount(); !got.Equal(tx.Money()) {
		t.Errorf("entry Amount() = %s, want %s", got, tx.Money())
	}
	if got := e.BalanceAfter(); !got.Equal(w.Balance()) {
		t.Errorf("entry BalanceAfter() = %s, want the wallet's %s", got, w.Balance())
	}
	if got := e.CreatedAt(); !got.Equal(in.Now) {
		t.Errorf("entry CreatedAt() = %v, want %v", got, in.Now)
	}
}

func TestDecide_Bet(t *testing.T) {
	tests := []struct {
		name, amount, balance string
	}{
		{"part of the balance", "80.00", "20.00"},
		{"the whole balance", "100.00", "0.00"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := decisionFor(pendingOp(t, opInput(wager.KindBet, tc.amount, "")), holding(t, "100.00"), nil)

			d := decide(t, in)

			assertOutcome(t, in, d, outcome{
				status: wager.StatusProcessed, balance: tc.balance, version: 2, entry: wallet.DirectionDebit,
			})
		})
	}
}

func TestDecide_BetInsufficient(t *testing.T) {
	for _, amount := range []string{"120.00", "100.01"} {
		t.Run(amount, func(t *testing.T) {
			in := decisionFor(pendingOp(t, opInput(wager.KindBet, amount, "")), holding(t, "100.00"), nil)

			d := decide(t, in)

			assertOutcome(t, in, d, rejected(wager.FailureInsufficientFunds))
		})
	}
}

func TestDecide_Win(t *testing.T) {
	bet := func(t *testing.T) *wager.Transaction { return processedRef(t, wager.KindBet) }
	tests := []struct {
		name     string
		in       wager.CommandInput
		ref      func(t *testing.T) *wager.Transaction
		reversed bool
		want     outcome
	}{
		{name: "standing on its own", in: opInput(wager.KindWin, "25.00", ""), want: credited},
		{name: "paying the bet it names", in: reversalOf(wager.KindWin), ref: bet, want: credited},
		{name: "for more than the bet", in: opInput(wager.KindWin, "50.00", "transaction-123"), ref: bet, want: outcome{
			status: wager.StatusProcessed, balance: "150.00", version: 2, entry: wallet.DirectionCredit,
		}},
		{name: "of a bet already reversed", in: reversalOf(wager.KindWin), ref: bet, reversed: true,
			want: rejected(wager.FailureReferenceAlreadyReversed)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ref *wager.Transaction
			if tc.ref != nil {
				ref = tc.ref(t)
			}
			in := decisionFor(pendingOp(t, tc.in), holding(t, "100.00"), ref)
			in.ReferenceReversed = tc.reversed

			d := decide(t, in)

			assertOutcome(t, in, d, tc.want)
		})
	}
}

func TestDecide_LossNoMovement(t *testing.T) {
	for _, balance := range []string{"100.00", "0.00"} {
		t.Run(balance, func(t *testing.T) {
			in := decisionFor(pendingOp(t, opInput(wager.KindLoss, "0.00", "")), holding(t, balance), nil)
			in.EntryID = uuid.Nil

			d := decide(t, in)

			assertOutcome(t, in, d, outcome{status: wager.StatusProcessed, balance: balance, version: 1})
		})
	}
}

func TestDecide_WalletPlayerMismatch(t *testing.T) {
	strangers := func(s *wallet.Snapshot) { s.PlayerID = strangerID }
	tests := []struct {
		name string
		in   wager.CommandInput
	}{
		{"bet", opInput(wager.KindBet, "25.00", "")},
		{"loss", opInput(wager.KindLoss, "0.00", "")},
		{"bet in another currency", edited(func(in *wager.CommandInput) { in.Currency = "USD" })},
		{"refund still missing its bet", reversalOf(wager.KindRefund)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := decisionFor(pendingOp(t, tc.in), walletWith(t, strangers), nil)

			d := decide(t, in)

			want := rejected(wager.FailureWalletPlayerMismatch)
			want.unobserved = true
			assertOutcome(t, in, d, want)
		})
	}
}

func TestDecide_CurrencyMismatch(t *testing.T) {
	inUSD := func(in wager.CommandInput) wager.CommandInput {
		in.Currency = "USD"
		return in
	}
	tests := []struct {
		name string
		in   wager.CommandInput
	}{
		{"bet", inUSD(opInput(wager.KindBet, "25.00", ""))},
		{"loss", inUSD(opInput(wager.KindLoss, "0.00", ""))},
		{"refund still missing its bet", inUSD(reversalOf(wager.KindRefund))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := decisionFor(pendingOp(t, tc.in), holding(t, "100.00"), nil)

			d := decide(t, in)

			want := rejected(wager.FailureCurrencyMismatch)
			want.unobserved = true
			assertOutcome(t, in, d, want)
		})
	}
}

func TestDecide_ReferenceMatrix(t *testing.T) {
	invalidKind := rejected(wager.FailureInvalidReferenceKind)
	notProcessed := rejected(wager.FailureReferenceNotProcessed)

	columns := []struct {
		name string
		ref  func(t *testing.T) *wager.Transaction
	}{
		{"BET", func(t *testing.T) *wager.Transaction { return processedRef(t, wager.KindBet) }},
		{"WIN", func(t *testing.T) *wager.Transaction { return processedRef(t, wager.KindWin) }},
		{"REFUND", func(t *testing.T) *wager.Transaction { return processedRef(t, wager.KindRefund) }},
		{"ROLLBACK", func(t *testing.T) *wager.Transaction { return processedRef(t, wager.KindRollback) }},
		{"LOSS", func(t *testing.T) *wager.Transaction { return processedRef(t, wager.KindLoss) }},
		{"REJECTED BET", func(t *testing.T) *wager.Transaction {
			return reference(t, referenceInput(wager.KindBet), wager.StatusRejected)
		}},
		{"FAILED BET", func(t *testing.T) *wager.Transaction {
			return reference(t, referenceInput(wager.KindBet), wager.StatusFailed)
		}},
		{"PENDING BET", func(t *testing.T) *wager.Transaction {
			return reference(t, referenceInput(wager.KindBet), wager.StatusPending)
		}},
		{"PENDING_REFERENCE REFUND", func(t *testing.T) *wager.Transaction {
			return reference(t, referenceInput(wager.KindRefund), wager.StatusPendingReference)
		}},
		{"REJECTED WIN", func(t *testing.T) *wager.Transaction {
			return reference(t, referenceInput(wager.KindWin), wager.StatusRejected)
		}},
		{"absent", func(*testing.T) *wager.Transaction { return nil }},
	}
	rows := []struct {
		kind wager.Kind
		want [11]outcome
	}{
		{wager.KindRefund, [...]outcome{
			credited, invalidKind, invalidKind, invalidKind, invalidKind,
			notProcessed, notProcessed, waits, invalidKind, invalidKind, waits,
		}},
		{wager.KindRollback, [...]outcome{
			credited, debited, debited, invalidKind, invalidKind,
			notProcessed, notProcessed, waits, waits, notProcessed, waits,
		}},
		{wager.KindWin, [...]outcome{
			credited, invalidKind, invalidKind, invalidKind, invalidKind,
			notProcessed, notProcessed, waits, invalidKind, invalidKind, waits,
		}},
	}
	for _, row := range rows {
		for i, col := range columns {
			t.Run(string(row.kind)+" of "+col.name, func(t *testing.T) {
				in := decisionFor(pendingOp(t, reversalOf(row.kind)), holding(t, "100.00"), col.ref(t))

				d := decide(t, in)

				assertOutcome(t, in, d, row.want[i])
			})
		}
	}
}

func TestDecide_RefundOfBet(t *testing.T) {
	in := decisionFor(pendingOp(t, reversalOf(wager.KindRefund)), holding(t, "100.00"), processedRef(t, wager.KindBet))

	d := decide(t, in)

	assertOutcome(t, in, d, credited)
	if got := in.Tx.ReferenceTransactionID(); got != (uuid.NullUUID{UUID: refTxID, Valid: true}) {
		t.Errorf("ReferenceTransactionID() = %v, want %s", got, refTxID)
	}
}

func TestDecide_RollbackOfWinInsufficient(t *testing.T) {
	tests := []struct {
		name string
		in   wager.CommandInput
		ref  wager.Kind
		want wager.FailureCode
	}{
		{"rollback of a win", reversalOf(wager.KindRollback), wager.KindWin, wager.FailureReversalInsufficientFunds},
		{"rollback of a refund", reversalOf(wager.KindRollback), wager.KindRefund, wager.FailureReversalInsufficientFunds},
		{"bet, for contrast", opInput(wager.KindBet, "25.00", ""), "", wager.FailureInsufficientFunds},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ref *wager.Transaction
			if tc.ref != "" {
				ref = processedRef(t, tc.ref)
			}
			in := decisionFor(pendingOp(t, tc.in), holding(t, "10.00"), ref)

			d := decide(t, in)

			assertOutcome(t, in, d, outcome{
				status: wager.StatusRejected, code: tc.want, balance: "10.00", version: 1,
			})
		})
	}
}

func TestDecide_AlreadyReversed(t *testing.T) {
	tests := []struct {
		name     string
		kind     wager.Kind
		ref      wager.Kind
		reversed bool
		want     outcome
	}{
		{"refund of a bet already refunded or rolled back", wager.KindRefund, wager.KindBet, true,
			rejected(wager.FailureReferenceAlreadyReversed)},
		{"rollback of a bet already refunded or rolled back", wager.KindRollback, wager.KindBet, true,
			rejected(wager.FailureReferenceAlreadyReversed)},
		{"rollback of a win already rolled back", wager.KindRollback, wager.KindWin, true,
			rejected(wager.FailureReferenceAlreadyReversed)},
		{"rollback of a refund already rolled back", wager.KindRollback, wager.KindRefund, true,
			rejected(wager.FailureReferenceAlreadyReversed)},
		{"win paying a bet already refunded or rolled back", wager.KindWin, wager.KindBet, true,
			rejected(wager.FailureReferenceAlreadyReversed)},
		{"rollback of the refund of a bet", wager.KindRollback, wager.KindRefund, false, debited},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := decisionFor(pendingOp(t, reversalOf(tc.kind)), holding(t, "100.00"), processedRef(t, tc.ref))
			in.ReferenceReversed = tc.reversed

			d := decide(t, in)

			assertOutcome(t, in, d, tc.want)
		})
	}
}

func TestDecide_AmountMismatch(t *testing.T) {
	tests := []struct {
		name   string
		kind   wager.Kind
		amount string
		ref    wager.Kind
	}{
		{"partial refund", wager.KindRefund, "20.00", wager.KindBet},
		{"refund of more than the bet", wager.KindRefund, "25.01", wager.KindBet},
		{"partial rollback of a bet", wager.KindRollback, "24.99", wager.KindBet},
		{"rollback of more than the win", wager.KindRollback, "30.00", wager.KindWin},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := pendingOp(t, opInput(tc.kind, tc.amount, "transaction-123"))
			in := decisionFor(tx, holding(t, "100.00"), processedRef(t, tc.ref))

			d := decide(t, in)

			assertOutcome(t, in, d, rejected(wager.FailureReversalAmountMismatch))
		})
	}
}

func TestDecide_ReferenceMismatchRound(t *testing.T) {
	otherRound := edited(func(in *wager.CommandInput) { in.RoundID = "round-988" })
	for _, kind := range []wager.Kind{wager.KindRefund, wager.KindRollback, wager.KindWin} {
		t.Run(string(kind), func(t *testing.T) {
			ref := reference(t, otherRound, wager.StatusProcessed)
			in := decisionFor(pendingOp(t, reversalOf(kind)), holding(t, "100.00"), ref)

			d := decide(t, in)

			assertOutcome(t, in, d, rejected(wager.FailureReferenceMismatch))
		})
	}
}

func TestDecide_ReferenceMismatch(t *testing.T) {
	tests := []struct {
		name string
		edit func(in *wager.CommandInput)
	}{
		{"another provider", func(in *wager.CommandInput) { in.ProviderID = "provider-b" }},
		{"another player", func(in *wager.CommandInput) { in.PlayerID = strangerID.String() }},
		{"another wallet", func(in *wager.CommandInput) { in.WalletID = strangerID.String() }},
		{"another currency", func(in *wager.CommandInput) { in.Currency = "USD" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ref := reference(t, edited(tc.edit), wager.StatusProcessed)
			in := decisionFor(pendingOp(t, reversalOf(wager.KindRefund)), holding(t, "100.00"), ref)

			d := decide(t, in)

			assertOutcome(t, in, d, rejected(wager.FailureReferenceMismatch))
		})
	}
}

func TestDecide_RuleOrder(t *testing.T) {
	otherRound := edited(func(in *wager.CommandInput) { in.RoundID = "round-988" })
	inOtherRound := func(status wager.Status) func(t *testing.T) *wager.Transaction {
		return func(t *testing.T) *wager.Transaction { return reference(t, otherRound, status) }
	}
	processed := func(kind wager.Kind) func(t *testing.T) *wager.Transaction {
		return func(t *testing.T) *wager.Transaction { return processedRef(t, kind) }
	}
	tests := []struct {
		name     string
		in       wager.CommandInput
		wallet   string
		ref      func(t *testing.T) *wager.Transaction
		reversed bool
		want     outcome
	}{
		{
			name:   "status of the reference before what it is about",
			in:     reversalOf(wager.KindRefund),
			wallet: "100.00",
			ref:    inOtherRound(wager.StatusRejected),
			want:   rejected(wager.FailureReferenceNotProcessed),
		},
		{
			name:   "a reference not yet final is waited for before it is compared",
			in:     reversalOf(wager.KindRefund),
			wallet: "100.00",
			ref:    inOtherRound(wager.StatusPending),
			want:   waits,
		},
		{
			name:   "what the reference is about before its amount",
			in:     opInput(wager.KindRefund, "20.00", "transaction-123"),
			wallet: "100.00",
			ref:    inOtherRound(wager.StatusProcessed),
			want:   rejected(wager.FailureReferenceMismatch),
		},
		{
			name:     "amount before an earlier reversal",
			in:       opInput(wager.KindRefund, "20.00", "transaction-123"),
			wallet:   "100.00",
			ref:      processed(wager.KindBet),
			reversed: true,
			want:     rejected(wager.FailureReversalAmountMismatch),
		},
		{
			name:     "what a win refers to before an earlier reversal",
			in:       reversalOf(wager.KindWin),
			wallet:   "100.00",
			ref:      inOtherRound(wager.StatusProcessed),
			reversed: true,
			want:     rejected(wager.FailureReferenceMismatch),
		},
		{
			name:     "a win skips the amount but not an earlier reversal",
			in:       opInput(wager.KindWin, "50.00", "transaction-123"),
			wallet:   "100.00",
			ref:      processed(wager.KindBet),
			reversed: true,
			want:     rejected(wager.FailureReferenceAlreadyReversed),
		},
		{
			name:     "an earlier reversal before the balance",
			in:       reversalOf(wager.KindRollback),
			wallet:   "10.00",
			ref:      processed(wager.KindWin),
			reversed: true,
			want: outcome{
				status: wager.StatusRejected, code: wager.FailureReferenceAlreadyReversed, balance: "10.00", version: 1,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := decisionFor(pendingOp(t, tc.in), holding(t, tc.wallet), tc.ref(t))
			in.ReferenceReversed = tc.reversed

			d := decide(t, in)

			assertOutcome(t, in, d, tc.want)
		})
	}

	t.Run("whose wallet it is before the reference", func(t *testing.T) {
		w := walletWith(t, func(s *wallet.Snapshot) { s.PlayerID = strangerID })
		in := decisionFor(pendingOp(t, reversalOf(wager.KindRefund)), w, processedRef(t, wager.KindLoss))

		d := decide(t, in)

		want := rejected(wager.FailureWalletPlayerMismatch)
		want.unobserved = true
		assertOutcome(t, in, d, want)
	})
}

func TestDecide_SelfReference(t *testing.T) {
	for _, kind := range []wager.Kind{wager.KindRefund, wager.KindRollback, wager.KindWin} {
		t.Run(string(kind), func(t *testing.T) {
			s := waitingOp(t, reversalOf(kind), 0).Snapshot()
			s.ReferenceExternalTransactionID = s.ExternalTransactionID
			tx, err := wager.Rehydrate(s)
			if err != nil {
				t.Fatalf("Rehydrate() error = %v", err)
			}
			in := decisionFor(tx, holding(t, "100.00"), tx)

			d := decide(t, in)

			assertOutcome(t, in, d, rejected(wager.FailureInvalidReferenceKind))
		})
	}
}

func TestDecide_MissingReferenceEntersPending(t *testing.T) {
	for _, kind := range []wager.Kind{wager.KindRefund, wager.KindRollback, wager.KindWin} {
		t.Run(string(kind), func(t *testing.T) {
			in := decisionFor(pendingOp(t, reversalOf(kind)), holding(t, "100.00"), nil)

			d := decide(t, in)

			assertOutcome(t, in, d, waits)
			assertSchedule(t, in.Tx, 0, decidedAt.Add(time.Second), decidedAt.Add(15*time.Minute))
		})
	}

	t.Run("TTL shorter than the first backoff", func(t *testing.T) {
		in := decisionFor(pendingOp(t, reversalOf(wager.KindRefund)), holding(t, "100.00"), nil)
		in.Policy.TTL = 500 * time.Millisecond

		d := decide(t, in)

		assertOutcome(t, in, d, waits)
		deadline := decidedAt.Add(500 * time.Millisecond)
		assertSchedule(t, in.Tx, 0, deadline, deadline)
	})
}

func assertSchedule(t *testing.T, tx *wager.Transaction, attempts int, next, expires time.Time) {
	t.Helper()
	if got := tx.ReferenceAttempts(); got != attempts {
		t.Errorf("ReferenceAttempts() = %d, want %d", got, attempts)
	}
	if got := tx.NextAttemptAt(); got == nil || !got.Equal(next) {
		t.Errorf("NextAttemptAt() = %v, want %v", deref(got), next)
	}
	if got := tx.ExpiresAt(); got == nil || !got.Equal(expires) {
		t.Errorf("ExpiresAt() = %v, want %v", deref(got), expires)
	}
}

var stillWaiting = outcome{status: wager.StatusPendingReference, balance: "100.00", version: 1}

func TestDecide_PendingRescheduleBackoff(t *testing.T) {
	expires := t0.Add(defaultPolicy.TTL)
	tests := []struct {
		attempt int
		delay   time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, time.Minute},
		{9, time.Minute},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("attempt %d", tc.attempt), func(t *testing.T) {
			in := decisionFor(waitingOp(t, reversalOf(wager.KindRefund), tc.attempt-1), holding(t, "100.00"), nil)
			in.Policy.MaxAttempts = 100

			d := decide(t, in)

			assertOutcome(t, in, d, stillWaiting)
			assertSchedule(t, in.Tx, tc.attempt, decidedAt.Add(tc.delay), expires)
		})
	}

	t.Run("never past the expiry", func(t *testing.T) {
		tx := waitingOp(t, reversalOf(wager.KindRefund), 7)
		in := decisionFor(tx, holding(t, "100.00"), nil)
		in.Now = expires.Add(-10 * time.Second)

		d := decide(t, in)

		assertOutcome(t, in, d, stillWaiting)
		assertSchedule(t, tx, 8, expires, expires)
	})
}

func TestDecide_ExpiresByAttempts(t *testing.T) {
	tests := []struct {
		name        string
		maxAttempts int
		missed      int
		want        outcome
	}{
		{"one look left", 10, 8, stillWaiting},
		{"last look", 10, 9, rejected(wager.FailureReferenceNotFound)},
		{"a single look allowed", 1, 0, rejected(wager.FailureReferenceNotFound)},
		{"past the limit", 10, 12, rejected(wager.FailureReferenceNotFound)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := decisionFor(waitingOp(t, reversalOf(wager.KindRefund), tc.missed), holding(t, "100.00"), nil)
			in.Policy.MaxAttempts = tc.maxAttempts

			d := decide(t, in)

			assertOutcome(t, in, d, tc.want)
			assertGaveUp(t, in.Tx, tc.want.status == wager.StatusRejected)
		})
	}
}

func TestDecide_ExpiresByTTL(t *testing.T) {
	expires := t0.Add(defaultPolicy.TTL)
	tests := []struct {
		name string
		now  time.Time
		want outcome
	}{
		{"just before the expiry", expires.Add(-time.Nanosecond), stillWaiting},
		{"at the expiry", expires, rejected(wager.FailureReferenceNotFound)},
		{"after the expiry", expires.Add(time.Hour), rejected(wager.FailureReferenceNotFound)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := decisionFor(waitingOp(t, reversalOf(wager.KindRefund), 2), holding(t, "100.00"), nil)
			in.Now = tc.now

			d := decide(t, in)

			assertOutcome(t, in, d, tc.want)
			assertGaveUp(t, in.Tx, tc.want.status == wager.StatusRejected)
		})
	}
}

func TestDecide_ExistingPendingReferenceExpiresAsNotProcessed(t *testing.T) {
	expires := t0.Add(defaultPolicy.TTL)
	tests := []struct {
		name   string
		missed int
		now    time.Time
		want   outcome
	}{
		{"still in time", 3, decidedAt, stillWaiting},
		{"by attempts", 9, decidedAt, rejected(wager.FailureReferenceNotProcessed)},
		{"by TTL", 3, expires, rejected(wager.FailureReferenceNotProcessed)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ref := reference(t, referenceInput(wager.KindBet), wager.StatusPending)
			in := decisionFor(waitingOp(t, reversalOf(wager.KindRefund), tc.missed), holding(t, "100.00"), ref)
			in.Now = tc.now

			d := decide(t, in)

			assertOutcome(t, in, d, tc.want)
			assertGaveUp(t, in.Tx, tc.want.status == wager.StatusRejected)
		})
	}
}

func assertGaveUp(t *testing.T, tx *wager.Transaction, gaveUp bool) {
	t.Helper()
	if scheduled := tx.NextAttemptAt() != nil && tx.ExpiresAt() != nil; scheduled == gaveUp {
		t.Errorf("NextAttemptAt() = %v, ExpiresAt() = %v, want a schedule: %v",
			deref(tx.NextAttemptAt()), deref(tx.ExpiresAt()), !gaveUp)
	}
}

func TestDecide_ResolvesWaitingTransaction(t *testing.T) {
	expires := t0.Add(defaultPolicy.TTL)
	tests := []struct {
		name   string
		missed int
		now    time.Time
	}{
		{"in time", 3, decidedAt},
		{"on the last look", 9, decidedAt},
		{"after the expiry", 3, expires.Add(time.Minute)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := waitingOp(t, reversalOf(wager.KindRefund), tc.missed)
			in := decisionFor(tx, holding(t, "100.00"), processedRef(t, wager.KindBet))
			in.Now = tc.now

			d := decide(t, in)

			assertOutcome(t, in, d, credited)
			assertGaveUp(t, tx, true)
		})
	}
}

func TestDecide_CreditOverflow(t *testing.T) {
	nearLimit, err := money.FromMinor(math.MaxInt64-2499, money.BRL())
	if err != nil {
		t.Fatalf("money.FromMinor() error = %v", err)
	}
	atLimit, err := money.FromMinor(math.MaxInt64-2500, money.BRL())
	if err != nil {
		t.Fatalf("money.FromMinor() error = %v", err)
	}
	maxBalance, err := money.FromMinor(math.MaxInt64, money.BRL())
	if err != nil {
		t.Fatalf("money.FromMinor() error = %v", err)
	}

	tests := []struct {
		name    string
		in      wager.CommandInput
		ref     wager.Kind
		balance money.Money
		want    outcome
	}{
		{"win", opInput(wager.KindWin, "25.00", ""), "", nearLimit, outcome{
			status: wager.StatusRejected, code: wager.FailureBalanceLimitExceeded, balance: nearLimit.String(), version: 1,
		}},
		{"refund", reversalOf(wager.KindRefund), wager.KindBet, nearLimit, outcome{
			status: wager.StatusRejected, code: wager.FailureBalanceLimitExceeded, balance: nearLimit.String(), version: 1,
		}},
		{"rollback of a bet", reversalOf(wager.KindRollback), wager.KindBet, nearLimit, outcome{
			status: wager.StatusRejected, code: wager.FailureBalanceLimitExceeded, balance: nearLimit.String(), version: 1,
		}},
		{"win up to the limit", opInput(wager.KindWin, "25.00", ""), "", atLimit, outcome{
			status: wager.StatusProcessed, balance: maxBalance.String(), version: 2, entry: wallet.DirectionCredit,
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ref *wager.Transaction
			if tc.ref != "" {
				ref = processedRef(t, tc.ref)
			}
			w := walletWith(t, func(s *wallet.Snapshot) { s.Balance = tc.balance })
			in := decisionFor(pendingOp(t, tc.in), w, ref)

			d := decide(t, in)

			assertOutcome(t, in, d, tc.want)
		})
	}
}

func TestDecide_InvalidInput(t *testing.T) {
	refund := func(t *testing.T) wager.DecisionInput {
		return decisionFor(pendingOp(t, reversalOf(wager.KindRefund)), holding(t, "100.00"), processedRef(t, wager.KindBet))
	}
	tests := []struct {
		name string
		in   func(t *testing.T) wager.DecisionInput
		want error
	}{
		{"no transaction", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Tx = nil
			return in
		}, wager.ErrInvalidDecision},
		{"no wallet", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Wallet = nil
			return in
		}, wager.ErrInvalidDecision},
		{"zero transaction", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Tx = &wager.Transaction{}
			return in
		}, wager.ErrInvalidDecision},
		{"zero wallet", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Wallet = &wallet.Wallet{}
			return in
		}, wager.ErrInvalidDecision},
		{"transaction already decided", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			if err := in.Tx.Reject(wager.FailureReferenceMismatch, parsed(t, "100.00", "BRL"), 1, t1); err != nil {
				t.Fatalf("Reject() error = %v", err)
			}
			return in
		}, wager.ErrTerminalState},
		{"opening", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Tx, in.Reference = newOpening(t), nil
			return in
		}, wager.ErrInvalidDecision},
		{"zero time", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Now = time.Time{}
			return in
		}, wager.ErrInvalidDecision},
		{"wallet the transaction does not name", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Wallet = walletWith(t, func(s *wallet.Snapshot) { s.ID = strangerID })
			return in
		}, wager.ErrInvalidDecision},
		{"reference for a bet", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Tx = pendingOp(t, opInput(wager.KindBet, "25.00", ""))
			return in
		}, wager.ErrInvalidDecision},
		{"reference for a loss", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Tx = pendingOp(t, opInput(wager.KindLoss, "0.00", ""))
			return in
		}, wager.ErrInvalidDecision},
		{"reference for a win that names none", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Tx = pendingOp(t, opInput(wager.KindWin, "25.00", ""))
			return in
		}, wager.ErrInvalidDecision},
		{"reversal reported without a reference", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Reference, in.ReferenceReversed = nil, true
			return in
		}, wager.ErrInvalidDecision},
		{"reference under another external id", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Reference = reference(t, edited(func(in *wager.CommandInput) {
				in.ExternalTransactionID = "transaction-999"
			}), wager.StatusProcessed)
			return in
		}, wager.ErrInvalidDecision},
		{"opening as reference", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Reference = newOpening(t)
			return in
		}, wager.ErrInvalidDecision},
		{"opening as reference for a bet", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Tx, in.Reference = pendingOp(t, opInput(wager.KindBet, "25.00", "")), newOpening(t)
			return in
		}, wager.ErrInvalidDecision},
		{"policy without attempts", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Policy.MaxAttempts = 0
			return in
		}, wager.ErrInvalidDecision},
		{"policy without TTL", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Policy.TTL = 0
			return in
		}, wager.ErrInvalidDecision},
		{"policy with a negative TTL", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Policy.TTL = -time.Minute
			return in
		}, wager.ErrInvalidDecision},
		{"policy without backoff base", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Policy.BackoffBase = 0
			return in
		}, wager.ErrInvalidDecision},
		{"policy without backoff max", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Policy.BackoffMax = 0
			return in
		}, wager.ErrInvalidDecision},
		{"policy with a backoff base above the max", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Policy.BackoffBase, in.Policy.BackoffMax = 2*time.Minute, time.Minute
			return in
		}, wager.ErrInvalidDecision},
		{"movement without entry id", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.EntryID = uuid.Nil
			return in
		}, wager.ErrInvalidDecision},
		{"bet beyond the balance without entry id", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Tx, in.Reference = pendingOp(t, opInput(wager.KindBet, "120.00", "")), nil
			in.EntryID = uuid.Nil
			return in
		}, wager.ErrInvalidDecision},
		{"jitter that removes the delay", func(t *testing.T) wager.DecisionInput {
			in := refund(t)
			in.Reference = nil
			in.Policy.Jitter = func(time.Duration) time.Duration { return 0 }
			return in
		}, wager.ErrInvalidTransaction},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in(t)
			var txBefore wager.Snapshot
			if in.Tx != nil {
				txBefore = in.Tx.Snapshot()
			}
			var walletBefore wallet.Snapshot
			if in.Wallet != nil {
				walletBefore = in.Wallet.Snapshot()
			}

			d, err := wager.Decide(in)

			if !errors.Is(err, tc.want) {
				t.Fatalf("Decide() error = %v, want %v", err, tc.want)
			}
			if d != (wager.Decision{}) {
				t.Errorf("Decide() = %+v, want the zero Decision", d)
			}
			if in.Tx != nil {
				assertUnchanged(t, in.Tx, txBefore)
			}
			if in.Wallet != nil && in.Wallet.Snapshot() != walletBefore {
				t.Errorf("wallet changed:\n got %+v\nwant %+v", in.Wallet.Snapshot(), walletBefore)
			}
		})
	}
}

func TestDecide_FailureAfterMovementChangesNothing(t *testing.T) {
	impostor, err := wager.NewExternal(txID, newCommand(t, referenceInput(wager.KindBet)), txCorrelationID, t0)
	if err != nil {
		t.Fatalf("NewExternal() error = %v", err)
	}
	if err := impostor.MarkProcessed(wager.Result{BalanceAfter: parsed(t, "100.00", "BRL"), WalletVersion: 1}, t1); err != nil {
		t.Fatalf("MarkProcessed() error = %v", err)
	}
	in := decisionFor(pendingOp(t, reversalOf(wager.KindRefund)), holding(t, "100.00"), impostor)
	txBefore, walletBefore := in.Tx.Snapshot(), in.Wallet.Snapshot()

	d, err := wager.Decide(in)

	assertInvalidTransaction(t, err)
	if d != (wager.Decision{}) {
		t.Errorf("Decide() = %+v, want the zero Decision", d)
	}
	assertUnchanged(t, in.Tx, txBefore)
	if got := in.Wallet.Snapshot(); got != walletBefore {
		t.Errorf("wallet changed:\n got %+v\nwant %+v", got, walletBefore)
	}
}

func TestDecide_OnlyWhatIsUsedIsRequired(t *testing.T) {
	t.Run("bet without policy", func(t *testing.T) {
		in := decisionFor(pendingOp(t, opInput(wager.KindBet, "25.00", "")), holding(t, "100.00"), nil)
		in.Policy = wager.ReferencePolicy{}

		d := decide(t, in)

		assertOutcome(t, in, d, debited)
	})
	t.Run("rejected refund without entry id", func(t *testing.T) {
		tx := pendingOp(t, opInput(wager.KindRefund, "20.00", "transaction-123"))
		in := decisionFor(tx, holding(t, "100.00"), processedRef(t, wager.KindBet))
		in.EntryID = uuid.Nil

		d := decide(t, in)

		assertOutcome(t, in, d, rejected(wager.FailureReversalAmountMismatch))
	})
	t.Run("wait without entry id", func(t *testing.T) {
		in := decisionFor(pendingOp(t, reversalOf(wager.KindRefund)), holding(t, "100.00"), nil)
		in.EntryID = uuid.Nil

		d := decide(t, in)

		assertOutcome(t, in, d, waits)
	})
}

func TestReferencePolicy_NextAttemptAt(t *testing.T) {
	const maxDuration = time.Duration(math.MaxInt64)
	now := decidedAt
	tests := []struct {
		name    string
		policy  wager.ReferencePolicy
		attempt int
		want    time.Duration
	}{
		{"first look", defaultPolicy, 0, time.Second},
		{"doubles", defaultPolicy, 1, 2 * time.Second},
		{"keeps doubling", defaultPolicy, 5, 32 * time.Second},
		{"capped", defaultPolicy, 6, time.Minute},
		{"negative attempt counts as the first", defaultPolicy, -3, time.Second},
		{"attempt of 62", defaultPolicy, 62, time.Minute},
		{"attempt of 63", defaultPolicy, 63, time.Minute},
		{"attempt of 64", defaultPolicy, 64, time.Minute},
		{"largest attempt", defaultPolicy, math.MaxInt, time.Minute},
		{"base above the cap", wager.ReferencePolicy{BackoffBase: time.Hour, BackoffMax: time.Minute}, 0, time.Minute},
		{"largest delay that fits", wager.ReferencePolicy{BackoffBase: time.Second, BackoffMax: maxDuration}, 33,
			(1 << 33) * time.Second},
		{"first delay that would not fit", wager.ReferencePolicy{BackoffBase: time.Second, BackoffMax: maxDuration}, 34,
			maxDuration},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := tc.policy.NextAttemptAt(tc.attempt, now), now.Add(tc.want); !got.Equal(want) {
				t.Errorf("NextAttemptAt(%d) = %v, want %v", tc.attempt, got, want)
			}
		})
	}

	t.Run("jitter receives the capped backoff", func(t *testing.T) {
		var received time.Duration
		p := defaultPolicy
		p.Jitter = func(d time.Duration) time.Duration {
			received = d
			return d / 2
		}

		got := p.NextAttemptAt(7, now)

		if received != time.Minute {
			t.Errorf("Jitter received %v, want %v", received, time.Minute)
		}
		if want := now.Add(30 * time.Second); !got.Equal(want) {
			t.Errorf("NextAttemptAt(7) = %v, want %v", got, want)
		}
	})
}
