//go:build integration

package wagering_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	return mustMoney(t, amount, money.BRL())
}

func TestSubmit_BetProcessed(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")

	res := submit(t, h.svc, input(w, "BET", "25.00"))

	if res.Replay || res.DuplicateMessage {
		t.Fatalf("result = %+v, want a new decision", res)
	}
	tx := res.Transaction
	requireOutcome(t, tx, wager.StatusProcessed, "")
	requireResultBalance(t, tx, "75.00")
	if tx.ResultWalletVersion != 2 || tx.CorrelationID != fromHTTP.CorrelationID {
		t.Fatalf("result version %d, correlation %q; want 2, %q", tx.ResultWalletVersion, tx.CorrelationID, fromHTTP.CorrelationID)
	}
	stored, err := h.svc.Get(t.Context(), tx.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	requireOutcome(t, stored, wager.StatusProcessed, "")
	requireResultBalance(t, stored, "75.00")

	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	debit := requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)[1]
	if debit.TransactionID() != tx.ID || debit.Amount().String() != "25.00" ||
		debit.BalanceBefore().String() != "100.00" || debit.BalanceAfter().String() != "75.00" {
		t.Fatalf("debit = %s of %s from %s to %s, want 25.00 of %s from 100.00 to 75.00",
			debit.Direction(), debit.TransactionID(), debit.BalanceBefore(), debit.BalanceAfter(), tx.ID)
	}

	evs := requireEvents(t, db, tx, "", event.TypeWagerTransactionProcessed, event.TypeWalletBalanceChanged)
	requireData(t, evs[0], map[string]any{
		"playerId":              w.PlayerID().String(),
		"kind":                  string(wager.KindBet),
		"money":                 brl(t, "25.00"),
		"balanceAfter":          brl(t, "75.00"),
		"walletVersion":         json.Number("2"),
		"providerId":            providerA,
		"externalTransactionId": tx.ExternalTransactionID,
		"roundId":               "round-1",
		"gameId":                "game-1",
	})
	requireData(t, evs[1], map[string]any{
		"direction":     string(wallet.DirectionDebit),
		"money":         brl(t, "25.00"),
		"balanceBefore": brl(t, "100.00"),
		"balanceAfter":  brl(t, "75.00"),
		"walletVersion": json.Number("2"),
	})

	txs, duplicates, retries := h.metrics.snapshot()
	if want := []recordedTransaction{{"http", "BET", "PROCESSED", ""}}; !slices.Equal(txs, want) {
		t.Fatalf("transaction metrics = %v, want %v", txs, want)
	}
	if len(duplicates) != 0 || len(retries) != 0 {
		t.Fatalf("duplicates = %v, retries = %v, want none", duplicates, retries)
	}
}

func TestSubmit_ReplaySameKey(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	in := input(w, "BET", "25.00")

	first := submit(t, h.svc, in).Transaction
	submit(t, h.svc, input(w, "BET", "10.00"))
	again := submit(t, h.svc, in)

	if !again.Replay {
		t.Fatal("second submission of the same key is not a replay")
	}
	tx := again.Transaction
	if tx.ID != first.ID {
		t.Fatalf("replay answered with transaction %s, want %s", tx.ID, first.ID)
	}
	requireOutcome(t, tx, wager.StatusProcessed, "")
	requireResultBalance(t, tx, "75.00")
	if tx.ResultWalletVersion != 2 {
		t.Fatalf("replay result version = %d, want 2", tx.ResultWalletVersion)
	}

	requireWallet(t, h.uow, w.ID(), "65.00", 3)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit, wallet.DirectionDebit)
	if n := len(events(t, db, w.ID())); n != 4 {
		t.Fatalf("outbox holds %d events, want the 4 of the two bets", n)
	}
	txs, duplicates, _ := h.metrics.snapshot()
	if len(txs) != 2 || !slices.Equal(duplicates, []string{"http/idempotency"}) {
		t.Fatalf("transactions = %v, duplicates = %v; want 2 and one idempotency duplicate", txs, duplicates)
	}
}

func TestSubmit_KeyReusedDifferentPayload(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	in := input(w, "BET", "25.00")
	submit(t, h.svc, in)

	other := in
	other.Amount = "30.00"
	_, err := h.svc.Submit(t.Context(), mustCommand(t, other), fromHTTP)

	if !errors.Is(err, wagering.ErrIdempotencyKeyReused) {
		t.Fatalf("Submit() error = %v, want ErrIdempotencyKeyReused", err)
	}
	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)
	if n := countRows(t, db, "SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'"); n != 1 {
		t.Fatalf("%d transactions stored, want 1", n)
	}
}

func TestSubmit_SameExternalOtherKey(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	in := input(w, "BET", "25.00")
	submit(t, h.svc, in)

	other := in
	other.IdempotencyKey = "key-other"
	_, err := h.svc.Submit(t.Context(), mustCommand(t, other), fromHTTP)

	if !errors.Is(err, wagering.ErrDuplicateExternalTransaction) {
		t.Fatalf("Submit() error = %v, want ErrDuplicateExternalTransaction", err)
	}
	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)
	if n := countRows(t, db, "SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'"); n != 1 {
		t.Fatalf("%d transactions stored, want 1", n)
	}
}

func TestSubmit_WalletNotFound(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	in := input(w, "BET", "25.00")
	in.WalletID = newID().String()

	_, err := h.svc.Submit(t.Context(), mustCommand(t, in), fromHTTP)

	if !errors.Is(err, wagering.ErrWalletNotFound) {
		t.Fatalf("Submit() error = %v, want ErrWalletNotFound", err)
	}
	if n := countRows(t, db, "SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'"); n != 0 {
		t.Fatalf("%d transactions stored, want none", n)
	}
	if n := countRows(t, db, "SELECT count(*) FROM outbox_events"); n != 0 {
		t.Fatalf("%d events stored, want none", n)
	}
}

func TestSubmit_TwoBetsOf80On100(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	cmds := []wager.Command{
		mustCommand(t, input(w, "BET", "80.00")),
		mustCommand(t, input(w, "BET", "80.00")),
	}

	results := make([]wagering.Result, len(cmds))
	errs := make([]error, len(cmds))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, cmd := range cmds {
		wg.Go(func() {
			<-start
			results[i], errs[i] = h.svc.Submit(context.WithoutCancel(t.Context()), cmd, fromHTTP)
		})
	}
	close(start)
	wg.Wait()

	var processed, rejected []wager.Snapshot
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Submit() #%d error = %v", i, err)
		}
		switch tx := results[i].Transaction; tx.Status {
		case wager.StatusProcessed:
			processed = append(processed, tx)
		case wager.StatusRejected:
			rejected = append(rejected, tx)
		}
	}
	if len(processed) != 1 || len(rejected) != 1 {
		t.Fatalf("%d processed and %d rejected, want one of each", len(processed), len(rejected))
	}
	requireResultBalance(t, processed[0], "20.00")
	requireOutcome(t, rejected[0], wager.StatusRejected, wager.FailureInsufficientFunds)
	requireResultBalance(t, rejected[0], "20.00")
	if rejected[0].ResultWalletVersion != 2 {
		t.Fatalf("rejection recorded version %d, want 2, the one the debit left", rejected[0].ResultWalletVersion)
	}

	requireWallet(t, h.uow, w.ID(), "20.00", 2)
	debit := requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)[1]
	if debit.TransactionID() != processed[0].ID {
		t.Fatalf("debit of transaction %s, want %s", debit.TransactionID(), processed[0].ID)
	}
	evs := events(t, db, w.ID())
	want := []struct {
		typ string
		tx  wager.Snapshot
	}{
		{event.TypeWagerTransactionProcessed, processed[0]},
		{event.TypeWalletBalanceChanged, processed[0]},
		{event.TypeWagerTransactionRejected, rejected[0]},
	}
	if len(evs) != len(want) {
		t.Fatalf("outbox holds %d events, want %d", len(evs), len(want))
	}
	for i, exp := range want {
		requireEnvelope(t, evs[i], exp.tx, "", exp.typ)
	}
}

func TestSubmit_SameBet50Concurrent(t *testing.T) {
	const submissions = 50
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	cmd := mustCommand(t, input(w, "BET", "25.00"))

	results := make([]wagering.Result, submissions)
	errs := make([]error, submissions)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range submissions {
		wg.Go(func() {
			<-start
			results[i], errs[i] = h.svc.Submit(context.WithoutCancel(t.Context()), cmd, fromHTTP)
		})
	}
	close(start)
	wg.Wait()

	var decided, replays int
	ids := map[uuid.UUID]bool{}
	for i, res := range results {
		if errs[i] != nil {
			t.Fatalf("Submit() #%d error = %v", i, errs[i])
		}
		if res.Replay {
			replays++
		} else {
			decided++
		}
		ids[res.Transaction.ID] = true
		requireOutcome(t, res.Transaction, wager.StatusProcessed, "")
		requireResultBalance(t, res.Transaction, "75.00")
	}
	if decided != 1 || replays != submissions-1 || len(ids) != 1 {
		t.Fatalf("%d decided, %d replays, %d transaction ids; want 1, %d, 1", decided, replays, len(ids), submissions-1)
	}

	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)
	if n := len(events(t, db, w.ID())); n != 2 {
		t.Fatalf("outbox holds %d events, want the 2 of one bet", n)
	}
	txs, duplicates, _ := h.metrics.snapshot()
	if len(txs) != 1 || len(duplicates) != submissions-1 {
		t.Fatalf("%d transactions and %d duplicates counted, want 1 and %d", len(txs), len(duplicates), submissions-1)
	}
}

func TestSubmit_LossKeepsVersion(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")

	tx := submit(t, h.svc, input(w, "LOSS", "0.00")).Transaction

	requireOutcome(t, tx, wager.StatusProcessed, "")
	requireResultBalance(t, tx, "100.00")
	if tx.ResultWalletVersion != 1 {
		t.Fatalf("result version = %d, want 1", tx.ResultWalletVersion)
	}
	requireWallet(t, h.uow, w.ID(), "100.00", 1)
	requireLedger(t, h.uow, w.ID())
	evs := requireEvents(t, db, tx, "", event.TypeWagerTransactionProcessed)
	requireData(t, evs[0], map[string]any{
		"money":         brl(t, "0.00"),
		"balanceAfter":  brl(t, "100.00"),
		"walletVersion": json.Number("1"),
	})
}

func TestSubmit_RefundAfterBet(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	bet := submit(t, h.svc, input(w, "BET", "25.00")).Transaction

	refund := submit(t, h.svc, referring(w, "REFUND", "25.00", bet)).Transaction

	requireOutcome(t, refund, wager.StatusProcessed, "")
	requireResultBalance(t, refund, "100.00")
	if refund.ReferenceTransactionID != (uuid.NullUUID{UUID: bet.ID, Valid: true}) {
		t.Fatalf("refund resolved to %v, want the bet %s", refund.ReferenceTransactionID, bet.ID)
	}
	requireWallet(t, h.uow, w.ID(), "100.00", 3)
	credit := requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit, wallet.DirectionCredit)[2]
	if credit.TransactionID() != refund.ID || credit.Amount().String() != "25.00" {
		t.Fatalf("credit = %s of %s, want 25.00 of %s", credit.Amount(), credit.TransactionID(), refund.ID)
	}

	evs := events(t, db, w.ID())
	if len(evs) != 4 {
		t.Fatalf("outbox holds %d events, want 4", len(evs))
	}
	requireEnvelope(t, evs[2], refund, "", event.TypeWagerTransactionProcessed)
	requireEnvelope(t, evs[3], refund, "", event.TypeWalletBalanceChanged)
	requireData(t, evs[2], map[string]any{"referenceTransactionId": bet.ID.String(), "balanceAfter": brl(t, "100.00")})
	requireData(t, evs[3], map[string]any{
		"direction":     string(wallet.DirectionCredit),
		"balanceBefore": brl(t, "75.00"),
		"balanceAfter":  brl(t, "100.00"),
		"walletVersion": json.Number("3"),
	})
}

func TestSubmit_RollbackOfWinInsufficient(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	win := submit(t, h.svc, input(w, "WIN", "50.00")).Transaction
	submit(t, h.svc, input(w, "BET", "140.00"))

	rollback := submit(t, h.svc, referring(w, "ROLLBACK", "50.00", win)).Transaction

	requireOutcome(t, rollback, wager.StatusRejected, wager.FailureReversalInsufficientFunds)
	requireResultBalance(t, rollback, "10.00")
	requireWallet(t, h.uow, w.ID(), "10.00", 3)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionCredit, wallet.DirectionDebit)
	evs := events(t, db, w.ID())
	last := evs[len(evs)-1]
	requireEnvelope(t, last, rollback, "", event.TypeWagerTransactionRejected)
	requireData(t, last, map[string]any{
		"failureCode": string(wager.FailureReversalInsufficientFunds),
		"money":       brl(t, "50.00"),
		"kind":        string(wager.KindRollback),
	})
}

func TestSubmit_RefundBeforeBetIsPending(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	in := input(w, "REFUND", "25.00")
	in.ReferenceExternalTransactionID = "bet-not-here-yet"

	tx := submit(t, h.svc, in).Transaction

	requireOutcome(t, tx, wager.StatusPendingReference, "")
	if tx.ResultBalance != nil || tx.ProcessedAt != nil {
		t.Fatalf("a waiting transaction recorded a result: balance %v, processed at %v", tx.ResultBalance, tx.ProcessedAt)
	}
	if next, expires := t0.Add(time.Second), t0.Add(15*time.Minute); tx.NextAttemptAt == nil || !tx.NextAttemptAt.Equal(next) ||
		tx.ExpiresAt == nil || !tx.ExpiresAt.Equal(expires) {
		t.Fatalf("next attempt %v, expiry %v; want %v and %v", tx.NextAttemptAt, tx.ExpiresAt, next, expires)
	}
	requireWallet(t, h.uow, w.ID(), "100.00", 1)
	requireLedger(t, h.uow, w.ID())
	evs := requireEvents(t, db, tx, "", event.TypeWagerTransactionPendingReference)
	requireData(t, evs[0], map[string]any{
		"referenceExternalTransactionId": "bet-not-here-yet",
		"nextAttemptAt":                  "2026-10-16T12:00:01.000Z",
		"expiresAt":                      "2026-10-16T12:15:00.000Z",
		"providerId":                     providerA,
		"externalTransactionId":          tx.ExternalTransactionID,
	})
	txs, _, _ := h.metrics.snapshot()
	if want := []recordedTransaction{{"http", "REFUND", "PENDING_REFERENCE", ""}}; !slices.Equal(txs, want) {
		t.Fatalf("transaction metrics = %v, want %v", txs, want)
	}

	again := submit(t, h.svc, in)
	if !again.Replay || again.Transaction.ID != tx.ID || again.Transaction.Status != wager.StatusPendingReference {
		t.Fatalf("replay = %+v, want the waiting transaction %s", again, tx.ID)
	}
	if n := len(events(t, db, w.ID())); n != 1 {
		t.Fatalf("outbox holds %d events, want the wait announced once", n)
	}
}

func TestSubmit_RejectionPersistsAndReplays(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	in := input(w, "BET", "150.00")

	tx := submit(t, h.svc, in).Transaction

	requireOutcome(t, tx, wager.StatusRejected, wager.FailureInsufficientFunds)
	requireResultBalance(t, tx, "100.00")
	requireWallet(t, h.uow, w.ID(), "100.00", 1)
	requireLedger(t, h.uow, w.ID())
	evs := requireEvents(t, db, tx, "", event.TypeWagerTransactionRejected)
	requireData(t, evs[0], map[string]any{
		"failureCode":           string(wager.FailureInsufficientFunds),
		"money":                 brl(t, "150.00"),
		"providerId":            providerA,
		"externalTransactionId": tx.ExternalTransactionID,
	})

	again := submit(t, h.svc, in)
	if !again.Replay || again.Transaction.ID != tx.ID {
		t.Fatalf("replay = %+v, want transaction %s again", again, tx.ID)
	}
	requireOutcome(t, again.Transaction, wager.StatusRejected, wager.FailureInsufficientFunds)
	requireResultBalance(t, again.Transaction, "100.00")
	txs, duplicates, _ := h.metrics.snapshot()
	if want := []recordedTransaction{{"http", "BET", "REJECTED", "INSUFFICIENT_FUNDS"}}; !slices.Equal(txs, want) ||
		!slices.Equal(duplicates, []string{"http/idempotency"}) {
		t.Fatalf("transactions = %v, duplicates = %v; want %v and one idempotency duplicate", txs, duplicates, want)
	}
}

func TestSubmit_OwnershipRejectsWithoutBalance(t *testing.T) {
	tests := []struct {
		name   string
		change func(*wager.CommandInput)
		code   wager.FailureCode
	}{
		{"other currency", func(in *wager.CommandInput) { in.Currency = "USD" }, wager.FailureCurrencyMismatch},
		{"other player", func(in *wager.CommandInput) { in.PlayerID = newID().String() }, wager.FailureWalletPlayerMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			db := connect(t)
			w := openWallet(t, h.uow, "100.00")
			in := input(w, "BET", "25.00")
			tt.change(&in)

			tx := submit(t, h.svc, in).Transaction

			requireOutcome(t, tx, wager.StatusRejected, tt.code)
			if tx.ResultBalance != nil || tx.ResultWalletVersion != 0 {
				t.Fatalf("rejection recorded the balance %v at version %d, want none", tx.ResultBalance, tx.ResultWalletVersion)
			}
			requireWallet(t, h.uow, w.ID(), "100.00", 1)
			requireLedger(t, h.uow, w.ID())
			evs := requireEvents(t, db, tx, "", event.TypeWagerTransactionRejected)
			requireData(t, evs[0], map[string]any{"failureCode": string(tt.code)})
		})
	}
}

func TestSubmit_WinOnReversedBetIsRejected(t *testing.T) {
	h := newHarness(t)
	w := openWallet(t, h.uow, "100.00")
	bet := submit(t, h.svc, input(w, "BET", "25.00")).Transaction
	submit(t, h.svc, referring(w, "REFUND", "25.00", bet))

	win := submit(t, h.svc, referring(w, "WIN", "40.00", bet)).Transaction

	requireOutcome(t, win, wager.StatusRejected, wager.FailureReferenceAlreadyReversed)
	requireResultBalance(t, win, "100.00")
	requireWallet(t, h.uow, w.ID(), "100.00", 3)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit, wallet.DirectionCredit)
}

func TestSubmit_GeneratesCorrelationIDWhenMissing(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	src := wagering.Source{Channel: wagering.ChannelSQS, CausationID: "msg-1"}

	res, err := h.svc.Submit(t.Context(), mustCommand(t, input(w, "BET", "25.00")), src)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	tx := res.Transaction
	if _, err := uuid.Parse(tx.CorrelationID); err != nil {
		t.Fatalf("correlation id = %q, want a generated uuid", tx.CorrelationID)
	}
	requireEvents(t, db, tx, "msg-1", event.TypeWagerTransactionProcessed, event.TypeWalletBalanceChanged)
	txs, _, _ := h.metrics.snapshot()
	if want := []recordedTransaction{{"sqs", "BET", "PROCESSED", ""}}; !slices.Equal(txs, want) {
		t.Fatalf("transaction metrics = %v, want %v", txs, want)
	}
}

func TestSubmit_ReplayDoesNotWaitForTheWalletLock(t *testing.T) {
	h := newHarness(t)
	w := openWallet(t, h.uow, "100.00")
	in := input(w, "BET", "25.00")
	first := submit(t, h.svc, in).Transaction
	lockWallet(t, w.ID())

	start := time.Now()
	again := submit(t, h.svc, in)
	if took := time.Since(start); took >= time.Second {
		t.Fatalf("replay took %s while the wallet was locked, want it not to wait", took)
	}
	if !again.Replay || again.Transaction.ID != first.ID {
		t.Fatalf("result = %+v, want the replay of %s", again, first.ID)
	}
}

func TestSubmit_ParallelWallets(t *testing.T) {
	cfg := dbConfig()
	cfg.LockTimeout = 500 * time.Millisecond
	h := newHarness(t, withDB(cfg))
	db := connect(t)
	a, b := openWallet(t, h.uow, "100.00"), openWallet(t, h.uow, "100.00")
	lockWallet(t, a.ID())

	betA := mustCommand(t, input(a, "BET", "10.00"))
	doneA := make(chan error, 1)
	startA := time.Now()
	go func() {
		_, err := h.svc.Submit(context.WithoutCancel(t.Context()), betA, fromHTTP)
		doneA <- err
	}()
	awaitLockWait(t, db)

	startB := time.Now()
	resB := submit(t, h.svc, input(b, "BET", "10.00"))
	if took := time.Since(startB); took >= time.Second {
		t.Fatalf("wallet B took %s while wallet A was locked, want under 1s", took)
	}
	requireOutcome(t, resB.Transaction, wager.StatusProcessed, "")
	select {
	case err := <-doneA:
		t.Fatalf("wallet A finished (%v) before wallet B, want it still waiting", err)
	default:
	}

	errA := <-doneA
	waitedA := time.Since(startA)
	var pgErr *pgconn.PgError
	if !errors.Is(errA, app.ErrUnavailable) || !errors.As(errA, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("Submit() on wallet A error = %v, want app.ErrUnavailable caused by a lock timeout (55P03)", errA)
	}
	if attempts := time.Duration(cfg.TxMaxAttempts); waitedA < attempts*cfg.LockTimeout {
		t.Fatalf("wallet A gave up after %s, before %d lock timeouts of %s", waitedA, cfg.TxMaxAttempts, cfg.LockTimeout)
	}
	if _, _, retries := h.metrics.snapshot(); !slices.Equal(retries, slices.Repeat([]string{app.RetryLockTimeout}, cfg.TxMaxAttempts-1)) {
		t.Fatalf("retries = %v, want %d of %s", retries, cfg.TxMaxAttempts-1, app.RetryLockTimeout)
	}

	requireWallet(t, h.uow, a.ID(), "100.00", 1)
	requireLedger(t, h.uow, a.ID())
	if n := countRows(t, db, "SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND origin = 'EXTERNAL'", a.ID()); n != 0 {
		t.Fatalf("%d transactions stored for wallet A, want none", n)
	}
	if n := len(events(t, db, a.ID())); n != 0 {
		t.Fatalf("%d events stored for wallet A, want none", n)
	}
	requireWallet(t, h.uow, b.ID(), "90.00", 2)
}

func TestSubmit_RollbackOnFailure_NoOutbox(t *testing.T) {
	errOutbox := fmt.Errorf("%w: the outbox refused the event", app.ErrPermanent)
	fault := &failingOutbox{}
	h := newHarness(t, withUnitOfWork(func(u app.UnitOfWork) app.UnitOfWork {
		fault.UnitOfWork = u
		return fault
	}))
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	fault.arm(errOutbox)

	res, err := h.svc.Submit(t.Context(), mustCommand(t, input(w, "BET", "25.00")), fromHTTP)

	if !errors.Is(err, errOutbox) {
		t.Fatalf("Submit() error = %v, want the outbox failure", err)
	}
	if res.Transaction.ID != uuid.Nil {
		t.Fatalf("Submit() result = %+v, want none", res)
	}
	requireWallet(t, h.uow, w.ID(), "100.00", 1)
	requireLedger(t, h.uow, w.ID())
	if n := countRows(t, db, "SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'"); n != 0 {
		t.Fatalf("%d transactions stored, want none", n)
	}
	if n := countRows(t, db, "SELECT count(*) FROM outbox_events"); n != 0 {
		t.Fatalf("%d events stored, want none", n)
	}
	if txs, _, _ := h.metrics.snapshot(); len(txs) != 0 {
		t.Fatalf("transaction metrics = %v, want none for a rollback", txs)
	}
}

func TestSubmit_ConcurrentFirstUseOfKey(t *testing.T) {
	h := newHarness(t, withUnitOfWork(func(u app.UnitOfWork) app.UnitOfWork {
		return insertRendezvous{UnitOfWork: u, meet: newRendezvous(2)}
	}))
	db := connect(t)
	w1, w2 := openWallet(t, h.uow, "100.00"), openWallet(t, h.uow, "100.00")
	in1, in2 := input(w1, "BET", "25.00"), input(w2, "BET", "30.00")
	in2.IdempotencyKey = in1.IdempotencyKey
	cmds := []wager.Command{mustCommand(t, in1), mustCommand(t, in2)}

	results := make([]wagering.Result, len(cmds))
	errs := make([]error, len(cmds))
	var wg sync.WaitGroup
	for i, cmd := range cmds {
		wg.Go(func() { results[i], errs[i] = h.svc.Submit(context.WithoutCancel(t.Context()), cmd, fromHTTP) })
	}
	wg.Wait()

	var processed, reused int
	for i, err := range errs {
		switch {
		case err == nil && results[i].Transaction.Status == wager.StatusProcessed && !results[i].Replay:
			processed++
		case errors.Is(err, wagering.ErrIdempotencyKeyReused):
			reused++
		default:
			t.Fatalf("Submit() #%d = %+v, %v; want a processed bet or ErrIdempotencyKeyReused", i, results[i], err)
		}
	}
	if processed != 1 || reused != 1 {
		t.Fatalf("%d processed and %d refused, want one of each", processed, reused)
	}
	if _, _, retries := h.metrics.snapshot(); !slices.Equal(retries, []string{app.RetryUniqueRace}) {
		t.Fatalf("retries = %v, want the one of the insert that lost the race", retries)
	}
	if n := countRows(t, db, "SELECT count(*) FROM wager_transactions WHERE idempotency_key = $1", in1.IdempotencyKey); n != 1 {
		t.Fatalf("%d transactions stored under the key, want 1", n)
	}
}

func TestGetForProvider_Isolation(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	tx := submit(t, h.svc, input(w, "BET", "25.00")).Transaction
	var openingID uuid.UUID
	if err := db.QueryRow(t.Context(), "SELECT id FROM wager_transactions WHERE kind = 'OPENING'").Scan(&openingID); err != nil {
		t.Fatalf("read the opening: %v", err)
	}

	got, err := h.svc.GetForProvider(t.Context(), tx.ID, providerA)
	if err != nil || got.ID != tx.ID {
		t.Fatalf("GetForProvider(own) = %s, %v; want %s", got.ID, err, tx.ID)
	}
	got, err = h.svc.GetByExternal(t.Context(), providerA, tx.ExternalTransactionID)
	if err != nil || got.ID != tx.ID {
		t.Fatalf("GetByExternal(own) = %s, %v; want %s", got.ID, err, tx.ID)
	}
	if got, err := h.svc.Get(t.Context(), openingID); err != nil || got.Kind != wager.KindOpening {
		t.Fatalf("Get(opening) = %+v, %v; want the opening", got, err)
	}

	hidden := []struct {
		name string
		get  func() (wager.Snapshot, error)
	}{
		{"another provider's transaction", func() (wager.Snapshot, error) {
			return h.svc.GetForProvider(t.Context(), tx.ID, providerB)
		}},
		{"an opening", func() (wager.Snapshot, error) { return h.svc.GetForProvider(t.Context(), openingID, providerA) }},
		{"an opening asked without provider", func() (wager.Snapshot, error) { return h.svc.GetForProvider(t.Context(), openingID, "") }},
		{"an unknown id", func() (wager.Snapshot, error) { return h.svc.GetForProvider(t.Context(), newID(), providerA) }},
		{"another provider's external id", func() (wager.Snapshot, error) {
			return h.svc.GetByExternal(t.Context(), providerB, tx.ExternalTransactionID)
		}},
		{"an unknown external id", func() (wager.Snapshot, error) { return h.svc.GetByExternal(t.Context(), providerA, "ext-unknown") }},
		{"an unknown id, internally", func() (wager.Snapshot, error) { return h.svc.Get(t.Context(), newID()) }},
	}
	for _, tt := range hidden {
		got, err := tt.get()
		if !errors.Is(err, wagering.ErrTransactionNotFound) {
			t.Errorf("%s: error = %v, want ErrTransactionNotFound", tt.name, err)
		}
		if got.ID != uuid.Nil {
			t.Errorf("%s: returned transaction %s, want none", tt.name, got.ID)
		}
	}
}

func requireEnvelope(t *testing.T, e outboxEvent, tx wager.Snapshot, causation, typ string) {
	t.Helper()
	requireEnvelopeAt(t, e, tx, causation, typ, t0)
}

func requireEnvelopeAt(t *testing.T, e outboxEvent, tx wager.Snapshot, causation, typ string, at time.Time) {
	t.Helper()
	if e.Type != typ {
		t.Fatalf("event %s is a %s, want a %s", e.ID, e.Type, typ)
	}
	requireAboutAt(t, e, tx, causation, at)
}

type failingOutbox struct {
	app.UnitOfWork
	faultSwitch
}

func (u *failingOutbox) Do(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	return u.UnitOfWork.Do(ctx, func(ctx context.Context, r app.Repos) error {
		return fn(ctx, failingOutboxRepos{Repos: r, fault: &u.faultSwitch})
	})
}

type failingOutboxRepos struct {
	app.Repos
	fault *faultSwitch
}

func (r failingOutboxRepos) Outbox() app.OutboxRepository {
	return failingAppend{OutboxRepository: r.Repos.Outbox(), fault: r.fault}
}

type failingAppend struct {
	app.OutboxRepository
	fault *faultSwitch
}

func (a failingAppend) Append(ctx context.Context, recs ...app.OutboxRecord) error {
	if err := a.fault.armed(); err != nil {
		return err
	}
	return a.OutboxRepository.Append(ctx, recs...)
}

type insertRendezvous struct {
	app.UnitOfWork
	meet *rendezvous
}

func (u insertRendezvous) Do(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	return u.UnitOfWork.Do(ctx, func(ctx context.Context, r app.Repos) error {
		return fn(ctx, rendezvousRepos{Repos: r, meet: u.meet})
	})
}

type rendezvousRepos struct {
	app.Repos
	meet *rendezvous
}

func (r rendezvousRepos) Transactions() app.TransactionRepository {
	return rendezvousInserts{TransactionRepository: r.Repos.Transactions(), meet: r.meet}
}

type rendezvousInserts struct {
	app.TransactionRepository
	meet *rendezvous
}

func (x rendezvousInserts) Insert(ctx context.Context, t *wager.Transaction) error {
	x.meet.arrive()
	return x.TransactionRepository.Insert(ctx, t)
}

type rendezvous struct {
	mu      sync.Mutex
	n       int
	arrived int
	all     chan struct{}
}

func newRendezvous(n int) *rendezvous { return &rendezvous{n: n, all: make(chan struct{})} }

func (r *rendezvous) arrive() {
	r.mu.Lock()
	r.arrived++
	nth := r.arrived
	if nth == r.n {
		close(r.all)
	}
	r.mu.Unlock()
	if nth > r.n {
		return
	}
	select {
	case <-r.all:
	case <-time.After(5 * time.Second):
	}
}
