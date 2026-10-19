//go:build integration

package wagering_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

func claimFor(t *testing.T, h *harness, lease time.Duration) []app.DueTransaction {
	t.Helper()
	due, err := h.svc.ClaimDue(t.Context(), lease, 10)
	if err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	return due
}

func claimOne(t *testing.T, h *harness, tx wager.Snapshot) app.DueTransaction {
	t.Helper()
	return requireClaimed(t, claimFor(t, h, time.Minute), tx)
}

func claimAgain(t *testing.T, h *harness, tx wager.Snapshot, lease time.Duration) app.DueTransaction {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if due := claimFor(t, h, lease); len(due) > 0 {
			return requireClaimed(t, due, tx)
		}
		if time.Now().After(deadline) {
			t.Fatalf("transaction %s was not claimed again", tx.ID)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func requireClaimed(t *testing.T, due []app.DueTransaction, tx wager.Snapshot) app.DueTransaction {
	t.Helper()
	if len(due) != 1 || due[0].ID != tx.ID || due[0].WalletID != tx.WalletID {
		t.Fatalf("claimed %v, want transaction %s of wallet %s alone", due, tx.ID, tx.WalletID)
	}
	return due[0]
}

func resume(t *testing.T, h *harness, due app.DueTransaction, want wagering.ResumeOutcome) {
	t.Helper()
	got, err := h.svc.Resume(t.Context(), due)
	if err != nil {
		t.Fatalf("Resume(%s) error = %v", due.ID, err)
	}
	if got != want {
		t.Fatalf("Resume(%s) = %q, want %q", due.ID, got, want)
	}
}

func stored(t *testing.T, h *harness, id uuid.UUID) wager.Snapshot {
	t.Helper()
	tx, err := h.svc.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("Get(%s) error = %v", id, err)
	}
	return tx
}

func requireTransactionMetrics(t *testing.T, h *harness, want ...recordedTransaction) {
	t.Helper()
	if txs, _, _ := h.metrics.snapshot(); !slices.Equal(txs, want) {
		t.Fatalf("transaction metrics = %v, want %v", txs, want)
	}
}

func TestResume_ResolvesAfterReferenceArrives(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "25.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", betIn.ExternalTransactionID)).Transaction
	requireOutcome(t, refund, wager.StatusPendingReference, "")
	bet := submit(t, h.svc, betIn).Transaction
	due := claimOne(t, h, refund)
	resolvedAt := t0.Add(5 * time.Second)
	h.clock.Set(resolvedAt)

	resume(t, h, due, wagering.ResumeProcessed)

	got := stored(t, h, refund.ID)
	requireOutcome(t, got, wager.StatusProcessed, "")
	requireResultBalance(t, got, "100.00")
	if got.ResultWalletVersion != 3 || got.ReferenceTransactionID != (uuid.NullUUID{UUID: bet.ID, Valid: true}) {
		t.Fatalf("refund resolved to %v at version %d, want the bet %s at version 3",
			got.ReferenceTransactionID, got.ResultWalletVersion, bet.ID)
	}
	if got.ProcessedAt == nil || !got.ProcessedAt.Equal(resolvedAt) || got.NextAttemptAt != nil || got.ExpiresAt != nil {
		t.Fatalf("processed at %v, next attempt %v, expiry %v; want processed at %v and no schedule",
			deref(got.ProcessedAt), deref(got.NextAttemptAt), deref(got.ExpiresAt), resolvedAt)
	}
	if got.CorrelationID != fromHTTP.CorrelationID {
		t.Fatalf("correlation id = %q, want %q, the one the refund was sent with", got.CorrelationID, fromHTTP.CorrelationID)
	}

	requireWallet(t, h.uow, w.ID(), "100.00", 3)
	credit := requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit, wallet.DirectionCredit)[2]
	if credit.TransactionID() != refund.ID || credit.Amount().String() != "25.00" {
		t.Fatalf("credit = %s of %s, want 25.00 of %s", credit.Amount(), credit.TransactionID(), refund.ID)
	}

	evs := events(t, db, w.ID())
	if len(evs) != 5 {
		t.Fatalf("outbox holds %d events, want the wait, the 2 of the bet and the 2 of the refund", len(evs))
	}
	causation := refund.ID.String()
	requireEnvelopeAt(t, evs[3], got, causation, event.TypeWagerTransactionProcessed, resolvedAt)
	requireEnvelopeAt(t, evs[4], got, causation, event.TypeWalletBalanceChanged, resolvedAt)
	requireData(t, evs[3], map[string]any{
		"referenceTransactionId": bet.ID.String(),
		"balanceAfter":           brl(t, "100.00"),
		"walletVersion":          json.Number("3"),
	})
	requireData(t, evs[4], map[string]any{
		"direction":     string(wallet.DirectionCredit),
		"balanceBefore": brl(t, "75.00"),
		"balanceAfter":  brl(t, "100.00"),
	})

	requireTransactionMetrics(t, h,
		recordedTransaction{"http", "REFUND", "PENDING_REFERENCE", ""},
		recordedTransaction{"http", "BET", "PROCESSED", ""},
		recordedTransaction{"resolver", "REFUND", "PROCESSED", ""})
	if again := claimFor(t, h, time.Minute); len(again) != 0 {
		t.Fatalf("claimed %v after the resolution, want nothing due", again)
	}
}

func TestResume_RejectsAfterTTL(t *testing.T) {
	policy := referencePolicy
	policy.TTL = 3 * time.Second
	h := newHarness(t, withReferencePolicy(policy))
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", "bet-never-sent")).Transaction
	expiry := t0.Add(policy.TTL)
	if refund.ExpiresAt == nil || !refund.ExpiresAt.Equal(expiry) {
		t.Fatalf("expiry = %v, want %v", deref(refund.ExpiresAt), expiry)
	}
	due := claimOne(t, h, refund)
	h.clock.Set(expiry)

	resume(t, h, due, wagering.ResumeRejected)

	got := stored(t, h, refund.ID)
	requireOutcome(t, got, wager.StatusRejected, wager.FailureReferenceNotFound)
	requireResultBalance(t, got, "100.00")
	if got.ResultWalletVersion != 1 || got.ProcessedAt == nil || !got.ProcessedAt.Equal(expiry) {
		t.Fatalf("rejected at %v with version %d, want at %v with version 1", deref(got.ProcessedAt), got.ResultWalletVersion, expiry)
	}
	requireWallet(t, h.uow, w.ID(), "100.00", 1)
	requireLedger(t, h.uow, w.ID())

	evs := events(t, db, w.ID())
	if len(evs) != 2 {
		t.Fatalf("outbox holds %d events, want the wait and the rejection", len(evs))
	}
	requireEnvelopeAt(t, evs[1], got, refund.ID.String(), event.TypeWagerTransactionRejected, expiry)
	requireData(t, evs[1], map[string]any{
		"failureCode": string(wager.FailureReferenceNotFound),
		"money":       brl(t, "25.00"),
	})
	requireTransactionMetrics(t, h,
		recordedTransaction{"http", "REFUND", "PENDING_REFERENCE", ""},
		recordedTransaction{"resolver", "REFUND", "REJECTED", string(wager.FailureReferenceNotFound)})
}

func TestResume_ReschedulesWithBackoff(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", "bet-not-here-yet")).Transaction

	for _, look := range []struct {
		at       time.Time
		attempts int
		delay    time.Duration
	}{
		{t0.Add(time.Second), 1, 2 * time.Second},
		{t0.Add(3 * time.Second), 2, 4 * time.Second},
	} {
		h.clock.Set(look.at)
		resume(t, h, claimOne(t, h, refund), wagering.ResumeRescheduled)

		got := stored(t, h, refund.ID)
		requireOutcome(t, got, wager.StatusPendingReference, "")
		if next := look.at.Add(look.delay); got.ReferenceAttempts != look.attempts || got.NextAttemptAt == nil ||
			!got.NextAttemptAt.Equal(next) {
			t.Fatalf("look at %v: %d attempts, next at %v; want %d, next at %v",
				look.at, got.ReferenceAttempts, deref(got.NextAttemptAt), look.attempts, next)
		}
		if got.ExpiresAt == nil || !got.ExpiresAt.Equal(*refund.ExpiresAt) || !got.UpdatedAt.Equal(look.at) {
			t.Fatalf("look at %v: expiry %v, updated at %v; want the expiry %v kept, updated at the look",
				look.at, deref(got.ExpiresAt), got.UpdatedAt, *refund.ExpiresAt)
		}
		if got.ResultBalance != nil || got.ProcessedAt != nil {
			t.Fatalf("look at %v recorded a result: balance %v, processed at %v", look.at, got.ResultBalance, got.ProcessedAt)
		}
	}

	requireWallet(t, h.uow, w.ID(), "100.00", 1)
	requireLedger(t, h.uow, w.ID())
	requireEvents(t, db, refund, "", event.TypeWagerTransactionPendingReference)
	requireTransactionMetrics(t, h, recordedTransaction{"http", "REFUND", "PENDING_REFERENCE", ""})
}

func TestResume_TwoPendingReversalsSameBet(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "25.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", betIn.ExternalTransactionID)).Transaction
	rollback := submit(t, h.svc, waitingFor(w, "ROLLBACK", "25.00", betIn.ExternalTransactionID)).Transaction
	submit(t, h.svc, betIn)
	due := claimFor(t, h, time.Minute)
	if len(due) != 2 {
		t.Fatalf("claimed %v, want the two reversals", due)
	}

	outcomes := make([]wagering.ResumeOutcome, len(due))
	errs := make([]error, len(due))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, d := range due {
		wg.Go(func() {
			<-start
			outcomes[i], errs[i] = h.svc.Resume(t.Context(), d)
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Resume() #%d error = %v", i, err)
		}
	}

	var processed, refused []wager.Snapshot
	for _, id := range []uuid.UUID{refund.ID, rollback.ID} {
		switch tx := stored(t, h, id); tx.Status {
		case wager.StatusProcessed:
			processed = append(processed, tx)
		case wager.StatusRejected:
			refused = append(refused, tx)
		}
	}
	if len(processed) != 1 || len(refused) != 1 {
		t.Fatalf("%d processed and %d rejected, want one of each", len(processed), len(refused))
	}
	requireOutcome(t, refused[0], wager.StatusRejected, wager.FailureReferenceAlreadyReversed)
	for i, d := range due {
		want := wagering.ResumeRejected
		if d.ID == processed[0].ID {
			want = wagering.ResumeProcessed
		}
		if outcomes[i] != want {
			t.Fatalf("Resume(%s) = %q, want %q", d.ID, outcomes[i], want)
		}
	}
	requireResultBalance(t, refused[0], "100.00")
	if refused[0].ResultWalletVersion != 3 {
		t.Fatalf("rejection recorded version %d, want 3", refused[0].ResultWalletVersion)
	}
	requireWallet(t, h.uow, w.ID(), "100.00", 3)
	if credit := requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit, wallet.DirectionCredit)[2]; credit.TransactionID() != processed[0].ID {
		t.Fatalf("credit of transaction %s, want %s", credit.TransactionID(), processed[0].ID)
	}

	evs := events(t, db, w.ID())
	if len(evs) != 7 {
		t.Fatalf("outbox holds %d events, want 7", len(evs))
	}
	requireEnvelope(t, evs[4], processed[0], processed[0].ID.String(), event.TypeWagerTransactionProcessed)
	requireEnvelope(t, evs[5], processed[0], processed[0].ID.String(), event.TypeWalletBalanceChanged)
	requireEnvelope(t, evs[6], refused[0], refused[0].ID.String(), event.TypeWagerTransactionRejected)
}

func TestResume_ReferenceRejected(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "150.00")
	rollback := submit(t, h.svc, waitingFor(w, "ROLLBACK", "150.00", betIn.ExternalTransactionID)).Transaction
	bet := submit(t, h.svc, betIn).Transaction
	requireOutcome(t, bet, wager.StatusRejected, wager.FailureInsufficientFunds)

	resume(t, h, claimOne(t, h, rollback), wagering.ResumeRejected)

	got := stored(t, h, rollback.ID)
	requireOutcome(t, got, wager.StatusRejected, wager.FailureReferenceNotProcessed)
	requireResultBalance(t, got, "100.00")
	requireWallet(t, h.uow, w.ID(), "100.00", 1)
	requireLedger(t, h.uow, w.ID())
	evs := events(t, db, w.ID())
	if len(evs) != 3 {
		t.Fatalf("outbox holds %d events, want 3", len(evs))
	}
	requireEnvelope(t, evs[2], got, rollback.ID.String(), event.TypeWagerTransactionRejected)
	requireData(t, evs[2], map[string]any{"failureCode": string(wager.FailureReferenceNotProcessed)})
}

func TestResume_SkipsTerminal(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "25.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", betIn.ExternalTransactionID)).Transaction
	submit(t, h.svc, betIn)
	due := claimOne(t, h, refund)
	resume(t, h, due, wagering.ResumeProcessed)
	settled := stored(t, h, refund.ID)
	requireOutcome(t, settled, wager.StatusProcessed, "")
	published := len(events(t, db, w.ID()))
	counted, _, _ := h.metrics.snapshot()
	h.clock.Advance(time.Minute)

	resume(t, h, due, wagering.ResumeSkipped)

	if got := stored(t, h, refund.ID); !reflect.DeepEqual(got, settled) {
		t.Fatalf("transaction changed to %+v, want it left as %+v", got, settled)
	}
	requireWallet(t, h.uow, w.ID(), "100.00", 3)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit, wallet.DirectionCredit)
	if n := len(events(t, db, w.ID())); n != published {
		t.Fatalf("outbox holds %d events, want the %d it held before", n, published)
	}
	requireTransactionMetrics(t, h, counted...)
}

func TestResume_StaleLeaseIsSkipped(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "25.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", betIn.ExternalTransactionID)).Transaction
	submit(t, h.svc, betIn)
	stale := requireClaimed(t, claimFor(t, h, time.Millisecond), refund)
	current := claimAgain(t, h, refund, time.Minute)
	if !current.LeaseUntil.After(stale.LeaseUntil) {
		t.Fatalf("the second lease ends at %v, want after the first one, %v", current.LeaseUntil, stale.LeaseUntil)
	}

	resume(t, h, stale, wagering.ResumeSkipped)

	got := stored(t, h, refund.ID)
	requireOutcome(t, got, wager.StatusPendingReference, "")
	if got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(current.LeaseUntil) {
		t.Fatalf("next attempt at %v, want the lease of the second claim, %v", deref(got.NextAttemptAt), current.LeaseUntil)
	}
	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	if n := len(events(t, db, w.ID())); n != 3 {
		t.Fatalf("outbox holds %d events, want the 3 written before", n)
	}
	requireTransactionMetrics(t, h,
		recordedTransaction{"http", "REFUND", "PENDING_REFERENCE", ""},
		recordedTransaction{"http", "BET", "PROCESSED", ""})

	resume(t, h, current, wagering.ResumeProcessed)

	requireOutcome(t, stored(t, h, refund.ID), wager.StatusProcessed, "")
	requireWallet(t, h.uow, w.ID(), "100.00", 3)
}

func TestResume_GivesUpWhileTheWalletIsLocked(t *testing.T) {
	cfg := dbConfig()
	cfg.LockTimeout = 200 * time.Millisecond
	h := newHarness(t, withDB(cfg))
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "25.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", betIn.ExternalTransactionID)).Transaction
	submit(t, h.svc, betIn)
	due := claimOne(t, h, refund)
	lockWallet(t, w.ID())

	outcome, err := h.svc.Resume(t.Context(), due)

	if !errors.Is(err, app.ErrUnavailable) || errors.Is(err, app.ErrPermanent) || outcome != "" {
		t.Fatalf("Resume() = %q, %v; want no outcome and app.ErrUnavailable alone", outcome, err)
	}
	got := stored(t, h, refund.ID)
	requireOutcome(t, got, wager.StatusPendingReference, "")
	if got.PermanentFailures != 0 || got.ReferenceAttempts != 0 || got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(due.LeaseUntil) {
		t.Fatalf("%d permanent failures, %d reference attempts, next at %v; want none, none and the end of the lease, %v",
			got.PermanentFailures, got.ReferenceAttempts, deref(got.NextAttemptAt), due.LeaseUntil)
	}
	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)
	if n := len(events(t, db, w.ID())); n != 3 {
		t.Fatalf("outbox holds %d events, want the 3 written before", n)
	}
	if _, _, retries := h.metrics.snapshot(); !slices.Equal(retries, slices.Repeat([]string{app.RetryLockTimeout}, cfg.TxMaxAttempts-1)) {
		t.Fatalf("retries = %v, want %d of %s", retries, cfg.TxMaxAttempts-1, app.RetryLockTimeout)
	}
	requireTransactionMetrics(t, h,
		recordedTransaction{"http", "REFUND", "PENDING_REFERENCE", ""},
		recordedTransaction{"http", "BET", "PROCESSED", ""})
}

func TestResume_SkipsAnotherWallet(t *testing.T) {
	h := newHarness(t)
	db := connect(t)
	w, other := openWallet(t, h.uow, "100.00"), openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "25.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", betIn.ExternalTransactionID)).Transaction
	submit(t, h.svc, betIn)
	due := claimOne(t, h, refund)
	misnamed := due
	misnamed.WalletID = other.ID()

	resume(t, h, misnamed, wagering.ResumeSkipped)

	got := stored(t, h, refund.ID)
	requireOutcome(t, got, wager.StatusPendingReference, "")
	if got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(due.LeaseUntil) {
		t.Fatalf("next attempt at %v, want the lease of the claim, %v", deref(got.NextAttemptAt), due.LeaseUntil)
	}
	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireWallet(t, h.uow, other.ID(), "100.00", 1)
	if n, m := len(events(t, db, w.ID())), len(events(t, db, other.ID())); n != 3 || m != 0 {
		t.Fatalf("outbox holds %d events of the wallet and %d of the other one, want the 3 written before and none", n, m)
	}
	var said bool
	for _, rec := range h.logs.records(t, slog.LevelDebug) {
		if rec["msg"] == "leaving a transaction that is not of the wallet the claim names" &&
			rec["transactionWalletId"] == w.ID().String() && rec["walletId"] == other.ID().String() {
			said = true
		}
	}
	if !said {
		t.Fatal("no debug log names the wallet of the transaction and the one of the claim")
	}

	resume(t, h, due, wagering.ResumeProcessed)
}

func TestResume_PermanentFailureMarksFailedAtLimit(t *testing.T) {
	errLedger := fmt.Errorf("%w: the ledger refused the entry", app.ErrPermanent)
	fault := &ledgerFault{}
	h := newHarness(t, withUnitOfWork(func(u app.UnitOfWork) app.UnitOfWork {
		fault.UnitOfWork = u
		return fault
	}))
	db := connect(t)
	w := openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "25.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", betIn.ExternalTransactionID)).Transaction
	submit(t, h.svc, betIn)
	fault.arm(errLedger)

	due := requireClaimed(t, claimFor(t, h, time.Millisecond), refund)
	for attempt := 1; attempt <= permanentFailureLimit; attempt++ {
		if attempt > 1 {
			due = claimAgain(t, h, refund, time.Millisecond)
		}

		outcome, err := h.svc.Resume(t.Context(), due)

		if !errors.Is(err, errLedger) || !errors.Is(err, app.ErrPermanent) {
			t.Fatalf("Resume() attempt %d error = %v, want the permanent failure of the ledger", attempt, err)
		}
		var want wagering.ResumeOutcome
		if attempt == permanentFailureLimit {
			want = wagering.ResumeFailed
		}
		if outcome != want {
			t.Fatalf("Resume() attempt %d = %q, want %q", attempt, outcome, want)
		}
		got := stored(t, h, refund.ID)
		if got.PermanentFailures != attempt {
			t.Fatalf("attempt %d: %d permanent failures counted, want %d", attempt, got.PermanentFailures, attempt)
		}
		if attempt < permanentFailureLimit {
			requireOutcome(t, got, wager.StatusPendingReference, "")
			if got.ReferenceAttempts != 0 || got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(due.LeaseUntil) {
				t.Fatalf("attempt %d: %d reference attempts, next at %v; want none and the end of the lease, %v",
					attempt, got.ReferenceAttempts, deref(got.NextAttemptAt), due.LeaseUntil)
			}
		}
	}

	got := stored(t, h, refund.ID)
	requireOutcome(t, got, wager.StatusFailed, wager.FailureProcessingFailed)
	if got.ProcessedAt == nil || !got.ProcessedAt.Equal(t0) || got.NextAttemptAt != nil || got.ResultBalance != nil {
		t.Fatalf("failed at %v, next attempt %v, balance %v; want failed at %v, unscheduled and without a result",
			deref(got.ProcessedAt), deref(got.NextAttemptAt), got.ResultBalance, t0)
	}
	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireLedger(t, h.uow, w.ID(), wallet.DirectionDebit)
	if n := len(events(t, db, w.ID())); n != 3 {
		t.Fatalf("outbox holds %d events, want the 3 written before the failures", n)
	}
	requireTransactionMetrics(t, h,
		recordedTransaction{"http", "REFUND", "PENDING_REFERENCE", ""},
		recordedTransaction{"http", "BET", "PROCESSED", ""},
		recordedTransaction{"resolver", "REFUND", "FAILED", string(wager.FailureProcessingFailed)})

	logged := h.logs.records(t, slog.LevelError)
	if len(logged) != 1 {
		t.Fatalf("logged %d errors, want the one of the failure: %v", len(logged), logged)
	}
	rec := logged[0]
	cause, _ := rec["error"].(string)
	if rec["transactionId"] != refund.ID.String() || rec["walletId"] != w.ID().String() || !strings.Contains(cause, "the ledger refused the entry") {
		t.Fatalf("error log = %v, want the transaction %s, the wallet %s and the cause", rec, refund.ID, w.ID())
	}
	if rec["correlationId"] != refund.CorrelationID || rec["providerId"] != providerA {
		t.Fatalf("error log = %v, want the correlation id %s and the provider %s", rec, refund.CorrelationID, providerA)
	}
	if again := claimFor(t, h, time.Minute); len(again) != 0 {
		t.Fatalf("claimed %v after the failure, want nothing due", again)
	}
}

func TestResume_StaleLeaseDoesNotCountAPermanentFailure(t *testing.T) {
	errLedger := fmt.Errorf("%w: the ledger refused the entry", app.ErrPermanent)
	fault := &ledgerFault{}
	h := newHarness(t, withUnitOfWork(func(u app.UnitOfWork) app.UnitOfWork {
		fault.UnitOfWork = u
		return fault
	}))
	w := openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "25.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", betIn.ExternalTransactionID)).Transaction
	submit(t, h.svc, betIn)
	due := requireClaimed(t, claimFor(t, h, time.Millisecond), refund)
	fault.arm(errLedger)
	var current app.DueTransaction
	fault.afterFailure(func() {
		current = claimAgain(t, h, refund, time.Minute)
	})

	outcome, err := h.svc.Resume(t.Context(), due)

	if !errors.Is(err, errLedger) || !errors.Is(err, app.ErrPermanent) || outcome != "" {
		t.Fatalf("Resume() = %q, %v; want no outcome and the permanent failure of the ledger", outcome, err)
	}
	if current.ID != refund.ID {
		t.Fatal("the lease was not taken over between the failure and its count")
	}
	got := stored(t, h, refund.ID)
	requireOutcome(t, got, wager.StatusPendingReference, "")
	if got.PermanentFailures != 0 || got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(current.LeaseUntil) {
		t.Fatalf("%d permanent failures counted, next attempt at %v; want none and the lease of the second claim, %v",
			got.PermanentFailures, deref(got.NextAttemptAt), current.LeaseUntil)
	}
	requireWallet(t, h.uow, w.ID(), "75.00", 2)
	requireTransactionMetrics(t, h,
		recordedTransaction{"http", "REFUND", "PENDING_REFERENCE", ""},
		recordedTransaction{"http", "BET", "PROCESSED", ""})
}

func TestResume_PermanentFailureThatCannotBeCounted(t *testing.T) {
	errLedger := fmt.Errorf("%w: the ledger refused the entry", app.ErrPermanent)
	cfg := dbConfig()
	cfg.LockTimeout = 200 * time.Millisecond
	fault := &ledgerFault{}
	h := newHarness(t, withDB(cfg), withUnitOfWork(func(u app.UnitOfWork) app.UnitOfWork {
		fault.UnitOfWork = u
		return fault
	}))
	w := openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "25.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", betIn.ExternalTransactionID)).Transaction
	submit(t, h.svc, betIn)
	due := claimOne(t, h, refund)
	fault.arm(errLedger)
	fault.afterFailure(func() { lockWallet(t, w.ID()) })

	outcome, err := h.svc.Resume(t.Context(), due)

	if !errors.Is(err, errLedger) || !errors.Is(err, app.ErrPermanent) || !errors.Is(err, app.ErrUnavailable) || outcome != "" {
		t.Fatalf("Resume() = %q, %v; want no outcome, the permanent failure and app.ErrUnavailable", outcome, err)
	}
	if msg := err.Error(); !strings.Contains(msg, "the ledger refused the entry (the failure could not be counted: ") {
		t.Fatalf("Resume() error = %q, want the cause followed by why it could not be counted", msg)
	}
	if got := stored(t, h, refund.ID); got.PermanentFailures != 0 || got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(due.LeaseUntil) {
		t.Fatalf("%d permanent failures counted, next attempt at %v; want none and the end of the lease, %v",
			got.PermanentFailures, deref(got.NextAttemptAt), due.LeaseUntil)
	}
	if _, _, retries := h.metrics.snapshot(); !slices.Equal(retries, slices.Repeat([]string{app.RetryLockTimeout}, cfg.TxMaxAttempts-1)) {
		t.Fatalf("retries = %v, want %d of %s", retries, cfg.TxMaxAttempts-1, app.RetryLockTimeout)
	}
}

type ledgerFault struct {
	app.UnitOfWork
	faultSwitch
	onFailure atomic.Pointer[func()]
}

func (f *ledgerFault) afterFailure(fn func()) { f.onFailure.Store(&fn) }

func (f *ledgerFault) Do(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	err := f.UnitOfWork.Do(ctx, func(ctx context.Context, r app.Repos) error {
		return fn(ctx, ledgerFaultRepos{Repos: r, fault: &f.faultSwitch})
	})
	if armed := f.armed(); armed != nil && errors.Is(err, armed) {
		if hook := f.onFailure.Swap(nil); hook != nil {
			(*hook)()
		}
	}
	return err
}

type ledgerFaultRepos struct {
	app.Repos
	fault *faultSwitch
}

func (r ledgerFaultRepos) Ledger() app.LedgerRepository {
	return faultyLedger{LedgerRepository: r.Repos.Ledger(), fault: r.fault}
}

type faultyLedger struct {
	app.LedgerRepository
	fault *faultSwitch
}

func (l faultyLedger) Append(ctx context.Context, e wallet.LedgerEntry) error {
	if err := l.fault.armed(); err != nil {
		return err
	}
	return l.LedgerRepository.Append(ctx, e)
}
