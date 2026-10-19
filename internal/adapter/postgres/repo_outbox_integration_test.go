//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
)

const (
	msgRelay1     = "relay-1"
	msgRelay2     = "relay-2"
	msgRelay3     = "relay-3"
	msgShortLease = 100 * time.Millisecond
	msgLongLease  = time.Hour
)

func msgEvent(aggregate string) app.OutboxRecord {
	id := newID()
	return app.OutboxRecord{
		EventID:       id,
		AggregateType: "Wallet",
		AggregateID:   aggregate,
		EventType:     "WalletBalanceChanged",
		EventVersion:  1,
		Payload: fmt.Appendf(nil, `{"eventId":%q,"aggregateId":%q,"data":{"amount":{"amount":"1.00","currency":"BRL"}}}`,
			id.String(), aggregate),
		CorrelationID: "corr-" + id.String(),
		CausationID:   "msg-" + id.String(),
		OccurredAt:    t0,
	}
}

func msgAggregates(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = newID().String()
	}
	return ids
}

func msgAppend(t *testing.T, uow app.UnitOfWork, recs ...app.OutboxRecord) {
	t.Helper()
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Outbox().Append(ctx, recs...)
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}

func msgTryClaim(ctx context.Context, uow app.UnitOfWork, owner string, lease time.Duration, limit int) ([]app.OutboxRecord, error) {
	var recs []app.OutboxRecord
	err := uow.Do(ctx, func(ctx context.Context, r app.Repos) error {
		var err error
		recs, err = r.Outbox().ClaimHeads(ctx, owner, lease, limit)
		return err
	})
	return recs, err
}

func msgClaim(t *testing.T, uow app.UnitOfWork, owner string, lease time.Duration, limit int) []app.OutboxRecord {
	t.Helper()
	recs, err := msgTryClaim(t.Context(), uow, owner, lease, limit)
	if err != nil {
		t.Fatalf("ClaimHeads(%s) error = %v", owner, err)
	}
	return recs
}

func msgClaimOnceDue(t *testing.T, uow app.UnitOfWork, owner string) []app.OutboxRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if recs := msgClaim(t, uow, owner, msgLongLease, 10); len(recs) > 0 {
			return recs
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s claimed nothing within 5s", owner)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func msgHoldClaim(t *testing.T, uow app.UnitOfWork, owner string, limit int) ([]app.OutboxRecord, func()) {
	t.Helper()
	claimed, release := make(chan []app.OutboxRecord, 1), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- uow.Do(context.WithoutCancel(t.Context()), func(ctx context.Context, r app.Repos) error {
			recs, err := r.Outbox().ClaimHeads(ctx, owner, msgLongLease, limit)
			if err != nil {
				return err
			}
			claimed <- recs
			<-release
			return nil
		})
	}()
	var recs []app.OutboxRecord
	select {
	case recs = <-claimed:
	case err := <-done:
		t.Fatalf("ClaimHeads(%s) error = %v", owner, err)
	}

	var once sync.Once
	end := func() {
		once.Do(func() {
			close(release)
			if err := <-done; err != nil {
				t.Errorf("unit of work holding the claim of %s: %v", owner, err)
			}
		})
	}
	t.Cleanup(end)
	return recs, end
}

func msgMarkPublished(t *testing.T, uow app.UnitOfWork, id uuid.UUID, owner string) bool {
	t.Helper()
	var ok bool
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		ok, err = r.Outbox().MarkPublished(ctx, id, owner)
		return err
	})
	if err != nil {
		t.Fatalf("MarkPublished(%s, %s) error = %v", id, owner, err)
	}
	return ok
}

func msgRequirePublished(t *testing.T, uow app.UnitOfWork, id uuid.UUID, owner string) {
	t.Helper()
	if !msgMarkPublished(t, uow, id, owner) {
		t.Fatalf("MarkPublished(%s, %s) = false, want true", id, owner)
	}
}

func msgMarkFailed(t *testing.T, uow app.UnitOfWork, id uuid.UUID, owner string, retryIn time.Duration, lastErr string) bool {
	t.Helper()
	var ok bool
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		ok, err = r.Outbox().MarkFailed(ctx, id, owner, retryIn, lastErr)
		return err
	})
	if err != nil {
		t.Fatalf("MarkFailed(%s, %s) error = %v", id, owner, err)
	}
	return ok
}

func msgRequireFailed(t *testing.T, uow app.UnitOfWork, id uuid.UUID, owner string, retryIn time.Duration, lastErr string) {
	t.Helper()
	if !msgMarkFailed(t, uow, id, owner, retryIn, lastErr) {
		t.Fatalf("MarkFailed(%s, %s) = false, want true", id, owner)
	}
}

func msgStats(t *testing.T, uow app.UnitOfWork) app.OutboxStats {
	t.Helper()
	var s app.OutboxStats
	err := uow.Snapshot(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		s, err = r.Outbox().Stats(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	return s
}

func msgRequireIDs(t *testing.T, got []app.OutboxRecord, want ...app.OutboxRecord) {
	t.Helper()
	ids := func(recs []app.OutboxRecord) []uuid.UUID {
		out := make([]uuid.UUID, len(recs))
		for i, r := range recs {
			out[i] = r.EventID
		}
		return out
	}
	if gotIDs, wantIDs := ids(got), ids(want); !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("claimed %v, want %v", gotIDs, wantIDs)
	}
}

func msgRequireRecord(t *testing.T, got, want app.OutboxRecord, attempts int) {
	t.Helper()
	var gotPayload, wantPayload any
	if err := json.Unmarshal(got.Payload, &gotPayload); err != nil {
		t.Fatalf("payload %s: %v", got.Payload, err)
	}
	if err := json.Unmarshal(want.Payload, &wantPayload); err != nil {
		t.Fatalf("payload %s: %v", want.Payload, err)
	}
	if got.EventID != want.EventID || got.AggregateType != want.AggregateType || got.AggregateID != want.AggregateID ||
		got.EventType != want.EventType || got.EventVersion != want.EventVersion ||
		got.CorrelationID != want.CorrelationID || got.CausationID != want.CausationID ||
		!got.OccurredAt.Equal(want.OccurredAt) || !reflect.DeepEqual(gotPayload, wantPayload) {
		t.Fatalf("record = %+v (payload %s), want %+v (payload %s)", got, got.Payload, want, want.Payload)
	}
	if got.OccurredAt.Location() != time.UTC {
		t.Fatalf("occurred at is in %s, want UTC", got.OccurredAt.Location())
	}
	if got.Attempts != attempts {
		t.Fatalf("attempts = %d, want %d", got.Attempts, attempts)
	}
}

type msgOutboxRow struct {
	attempts    int
	lockedBy    *string
	leaseLeft   *time.Duration
	dueIn       time.Duration
	lastError   *string
	publishedAt *time.Time
}

func msgReadEvent(t *testing.T, db execer, id uuid.UUID) msgOutboxRow {
	t.Helper()
	var r msgOutboxRow
	err := db.QueryRow(t.Context(),
		`SELECT attempts, locked_by, locked_until - now(), next_attempt_at - now(), last_error, published_at
		   FROM outbox_events
		  WHERE id = $1`, id).
		Scan(&r.attempts, &r.lockedBy, &r.leaseLeft, &r.dueIn, &r.lastError, &r.publishedAt)
	if err != nil {
		t.Fatalf("read outbox event %s: %v", id, err)
	}
	return r
}

func msgRequireLeased(t *testing.T, db execer, id uuid.UUID, owner string) msgOutboxRow {
	t.Helper()
	r := msgReadEvent(t, db, id)
	if r.publishedAt != nil || r.lockedBy == nil || *r.lockedBy != owner || r.leaseLeft == nil || *r.leaseLeft <= 0 {
		t.Fatalf("event %s: locked by %v for %v, published at %v; want leased to %s",
			id, msgDeref(r.lockedBy), msgDeref(r.leaseLeft), r.publishedAt, owner)
	}
	return r
}

func msgDeref[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

func msgInsertAged(t *testing.T, db execer, age time.Duration) uuid.UUID {
	t.Helper()
	e := msgEvent(newID().String())
	mustExec(t, db,
		`INSERT INTO outbox_events
		        (id, aggregate_type, aggregate_id, event_type, event_version, payload, correlation_id,
		         occurred_at, next_attempt_at, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now() - $9::interval)`,
		e.EventID, e.AggregateType, e.AggregateID, e.EventType, e.EventVersion, e.Payload, e.CorrelationID,
		e.OccurredAt, age)
	return e.EventID
}

func TestOutbox_AppendStoresTheEventAsGiven(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	agg := msgAggregates(2)
	caused := msgEvent(agg[0])
	uncaused := msgEvent(agg[1])
	uncaused.CausationID = ""
	uncaused.Attempts = 7
	msgAppend(t, uow, caused, uncaused)
	msgAppend(t, uow)

	got := msgClaim(t, uow, msgRelay1, msgLongLease, 10)
	msgRequireIDs(t, got, caused, uncaused)
	msgRequireRecord(t, got[0], caused, 1)
	msgRequireRecord(t, got[1], uncaused, 1)
	owner := connect(t, pg.OwnerDSN)
	if n := queryInt64(t, owner, "SELECT count(*) FROM outbox_events WHERE causation_id IS NULL"); n != 1 {
		t.Fatalf("%d events store no causation, want 1: an empty one is NULL", n)
	}
}

func TestOutbox_AppendRefusesADuplicateEvent(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	agg := msgAggregates(1)[0]
	first := msgEvent(agg)
	msgAppend(t, uow, first)

	second := msgEvent(agg)
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Outbox().Append(ctx, second, first)
	})
	requireClass(t, err, app.ErrPermanent)
	requireSQLState(t, err, uniqueViolation, "outbox_events_pkey")
	if !strings.Contains(err.Error(), first.EventID.String()) {
		t.Fatalf("error = %v, want it to name event %s", err, first.EventID)
	}
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10), first)
}

func TestOutbox_ClaimHeadsOnePerAggregate(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	agg := msgAggregates(2)
	a1, a2, a3, b1 := msgEvent(agg[0]), msgEvent(agg[0]), msgEvent(agg[0]), msgEvent(agg[1])
	msgAppend(t, uow, a1, a2)
	msgAppend(t, uow, b1)
	msgAppend(t, uow, a3)

	claimed := msgClaim(t, uow, msgRelay1, msgLongLease, 10)
	msgRequireIDs(t, claimed, a1, b1)
	for _, e := range claimed {
		r := msgRequireLeased(t, owner, e.EventID, msgRelay1)
		if *r.leaseLeft > msgLongLease || *r.leaseLeft < msgLongLease-time.Minute || r.attempts != 1 {
			t.Fatalf("event %s: lease left %v, attempts %d; want about %v and 1", e.EventID, *r.leaseLeft, r.attempts, msgLongLease)
		}
	}

	msgRequirePublished(t, uow, a1.EventID, msgRelay1)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10), a2)
	msgRequirePublished(t, uow, a2.EventID, msgRelay1)
	msgRequirePublished(t, uow, b1.EventID, msgRelay1)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10), a3)
	msgRequirePublished(t, uow, a3.EventID, msgRelay1)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10))
}

func TestOutbox_LeaseBlocksOtherOwner(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	agg := msgAggregates(2)
	a1, a2, b1 := msgEvent(agg[0]), msgEvent(agg[0]), msgEvent(agg[1])
	msgAppend(t, uow, a1, a2, b1)

	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10), a1, b1)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay2, msgLongLease, 10))

	msgRequirePublished(t, uow, a1.EventID, msgRelay1)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay2, msgLongLease, 10), a2)
}

func TestOutbox_ExpiredLeaseIsReclaimed(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	e := msgEvent(msgAggregates(1)[0])
	msgAppend(t, uow, e)

	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgShortLease, 10), e)

	got := msgClaimOnceDue(t, uow, msgRelay2)
	msgRequireIDs(t, got, e)
	msgRequireRecord(t, got[0], e, 2)
	msgRequireLeased(t, owner, e.EventID, msgRelay2)
}

func TestOutbox_ClaimHeadsUsesDatabaseNow(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	agg := msgAggregates(3)
	later, leased, due := msgEvent(agg[0]), msgEvent(agg[1]), msgEvent(agg[2])
	msgAppend(t, uow, later, leased, due)
	mustExec(t, owner, "UPDATE outbox_events SET next_attempt_at = now() + interval '1 hour' WHERE id = $1", later.EventID)
	mustExec(t, owner, `UPDATE outbox_events SET locked_by = 'relay-elsewhere', locked_until = now() + interval '1 hour'
		WHERE id = $1`, leased.EventID)

	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10), due)
}

func TestOutbox_ClaimHeadsSkipsEventsBeingClaimed(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	agg := msgAggregates(3)
	a1, a2, b1, c1 := msgEvent(agg[0]), msgEvent(agg[0]), msgEvent(agg[1]), msgEvent(agg[2])
	msgAppend(t, uow, a1, a2, b1, c1)

	first, commit := msgHoldClaim(t, uow, msgRelay1, 1)
	msgRequireIDs(t, first, a1)

	msgRequireIDs(t, msgClaim(t, uow, msgRelay2, msgLongLease, 10), b1, c1)
	commit()
	msgRequireLeased(t, owner, a1.EventID, msgRelay1)
}

func TestOutbox_MarkPublishedFencing(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	e := msgEvent(msgAggregates(1)[0])
	msgAppend(t, uow, e)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgShortLease, 10), e)
	msgRequireIDs(t, msgClaimOnceDue(t, uow, msgRelay2), e)

	if msgMarkPublished(t, uow, e.EventID, msgRelay1) {
		t.Fatal("relay-1 acknowledged an event leased to relay-2")
	}
	if msgMarkFailed(t, uow, e.EventID, msgRelay1, msgLongLease, "late failure") {
		t.Fatal("relay-1 recorded a failure of an event leased to relay-2")
	}
	r := msgRequireLeased(t, owner, e.EventID, msgRelay2)
	if r.lastError != nil || r.dueIn > 0 {
		t.Fatalf("relay-1's failure was recorded: last error %v, due in %v", msgDeref(r.lastError), r.dueIn)
	}

	msgRequirePublished(t, uow, e.EventID, msgRelay2)
	if msgMarkFailed(t, uow, e.EventID, msgRelay2, msgLongLease, "late failure") {
		t.Fatal("a failure of a published event was recorded")
	}
}

func TestOutbox_MarkPublishedTwiceIsFalse(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	agg := msgAggregates(2)
	e, unclaimed := msgEvent(agg[0]), msgEvent(agg[1])
	msgAppend(t, uow, e)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10), e)
	msgRequirePublished(t, uow, e.EventID, msgRelay1)
	published := msgReadEvent(t, owner, e.EventID)
	if published.publishedAt == nil || published.lockedBy != nil || published.leaseLeft != nil {
		t.Fatalf("published event: published at %v, locked by %v until %v; want published without a lease",
			published.publishedAt, msgDeref(published.lockedBy), msgDeref(published.leaseLeft))
	}

	if msgMarkPublished(t, uow, e.EventID, msgRelay1) {
		t.Fatal("a second acknowledgement reported a publication")
	}
	if again := msgReadEvent(t, owner, e.EventID); again.publishedAt == nil || !again.publishedAt.Equal(*published.publishedAt) {
		t.Fatalf("published at moved from %v to %v", *published.publishedAt, msgDeref(again.publishedAt))
	}

	msgAppend(t, uow, unclaimed)
	if msgMarkPublished(t, uow, unclaimed.EventID, msgRelay1) {
		t.Fatal("an event nobody claimed was acknowledged")
	}
}

func TestOutbox_MarkFailedSchedulesRetry(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	agg := msgAggregates(1)[0]
	a1, a2 := msgEvent(agg), msgEvent(agg)
	msgAppend(t, uow, a1, a2)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10), a1)

	msgRequireFailed(t, uow, a1.EventID, msgRelay1, 0, "connection reset")
	again := msgClaim(t, uow, msgRelay2, msgLongLease, 10)
	msgRequireIDs(t, again, a1)
	msgRequireRecord(t, again[0], a1, 2)

	long := strings.Repeat("é", 700) + strings.Repeat("€", 700)
	msgRequireFailed(t, uow, a1.EventID, msgRelay2, time.Hour, long)
	r := msgReadEvent(t, owner, a1.EventID)
	if r.lockedBy != nil || r.leaseLeft != nil {
		t.Fatalf("the lease outlived the failure: locked by %v for %v", msgDeref(r.lockedBy), msgDeref(r.leaseLeft))
	}
	if r.dueIn > time.Hour || r.dueIn < time.Hour-time.Minute {
		t.Fatalf("due in %v, want about an hour", r.dueIn)
	}
	if want := string([]rune(long)[:1000]); r.lastError == nil || *r.lastError != want {
		t.Fatalf("last error = %q, want the first 1000 characters of the error", msgDeref(r.lastError))
	}
	if r.attempts != 2 || r.publishedAt != nil {
		t.Fatalf("attempts %d, published at %v; want 2 and unpublished", r.attempts, r.publishedAt)
	}
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10))
}

func TestOutbox_FailedHeadHoldsBackOnlyItsAggregate(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	agg := msgAggregates(3)
	a1, b1, a2, c1, b2, a3, c2, b3 := msgEvent(agg[0]), msgEvent(agg[1]), msgEvent(agg[0]), msgEvent(agg[2]),
		msgEvent(agg[1]), msgEvent(agg[0]), msgEvent(agg[2]), msgEvent(agg[1])
	msgAppend(t, uow, a1, b1, a2, c1, b2, a3, c2, b3)

	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10), a1, b1, c1)
	msgRequireFailed(t, uow, a1.EventID, msgRelay1, time.Hour, "broker refused")
	msgRequirePublished(t, uow, b1.EventID, msgRelay1)
	msgRequirePublished(t, uow, c1.EventID, msgRelay1)

	msgRequireIDs(t, msgClaim(t, uow, msgRelay2, msgLongLease, 10), b2, c2)
	msgRequirePublished(t, uow, b2.EventID, msgRelay2)
	msgRequirePublished(t, uow, c2.EventID, msgRelay2)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10), b3)
	msgRequirePublished(t, uow, b3.EventID, msgRelay1)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay2, msgLongLease, 10))

	mustExec(t, owner, "UPDATE outbox_events SET next_attempt_at = now() WHERE id = $1", a1.EventID)
	for _, next := range []struct {
		rec      app.OutboxRecord
		attempts int
	}{{a1, 2}, {a2, 1}, {a3, 1}} {
		got := msgClaim(t, uow, msgRelay2, msgLongLease, 10)
		msgRequireIDs(t, got, next.rec)
		msgRequireRecord(t, got[0], next.rec, next.attempts)
		msgRequirePublished(t, uow, next.rec.EventID, msgRelay2)
	}
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 10))
}

func TestOutbox_ReleaseEndsOnlyTheOwnersLeases(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	agg := msgAggregates(3)
	a1, b1, c1 := msgEvent(agg[0]), msgEvent(agg[1]), msgEvent(agg[2])
	msgAppend(t, uow, a1, b1, c1)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay1, msgLongLease, 2), a1, b1)
	msgRequireIDs(t, msgClaim(t, uow, msgRelay2, msgLongLease, 10), c1)

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Outbox().Release(ctx, msgRelay1)
	})
	if err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	taken := msgClaim(t, uow, msgRelay3, msgLongLease, 10)
	msgRequireIDs(t, taken, a1, b1)
	msgRequireRecord(t, taken[0], a1, 2)
	msgRequireLeased(t, owner, c1.EventID, msgRelay2)
}

func TestOutbox_Stats(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	owner := connect(t, pg.OwnerDSN)
	if s := msgStats(t, uow); s != (app.OutboxStats{}) {
		t.Fatalf("Stats() of an empty outbox = %+v, want zero", s)
	}

	published := msgInsertAged(t, owner, time.Hour)
	mustExec(t, owner, "UPDATE outbox_events SET published_at = now() WHERE id = $1", published)
	oldest := msgInsertAged(t, owner, 90*time.Second)
	msgInsertAged(t, owner, 30*time.Second)
	msgAppend(t, uow, msgEvent(msgAggregates(1)[0]))

	if s := msgStats(t, uow); s.Pending != 3 || s.OldestAge < 90*time.Second || s.OldestAge > 100*time.Second {
		t.Fatalf("Stats() = %+v, want 3 pending, the oldest about 90s old", s)
	}
	mustExec(t, owner, "UPDATE outbox_events SET published_at = now() WHERE id = $1", oldest)
	if s := msgStats(t, uow); s.Pending != 2 || s.OldestAge < 30*time.Second || s.OldestAge > 40*time.Second {
		t.Fatalf("Stats() = %+v, want 2 pending, the oldest about 30s old", s)
	}
}

func TestOutbox_RefusesWhatCannotBeRight(t *testing.T) {
	uow, _ := newUnitOfWork(t)
	claim := func(owner string, lease time.Duration, limit int) func(context.Context, app.OutboxRepository) error {
		return func(ctx context.Context, o app.OutboxRepository) error {
			_, err := o.ClaimHeads(ctx, owner, lease, limit)
			return err
		}
	}
	for name, call := range map[string]func(context.Context, app.OutboxRepository) error{
		"claim without an owner":    claim("", time.Minute, 10),
		"claim with no lease":       claim(msgRelay1, 0, 10),
		"claim with a negative one": claim(msgRelay1, -time.Second, 10),
		"claim with no limit":       claim(msgRelay1, time.Minute, 0),
		"retry in the past": func(ctx context.Context, o app.OutboxRepository) error {
			_, err := o.MarkFailed(ctx, newID(), msgRelay1, -time.Second, "boom")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error { return call(ctx, r.Outbox()) })
			requireClass(t, err, app.ErrPermanent)
		})
	}
}

func TestMessagingRepos_ErrorsAreClassified(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	id := newID()

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		calls := map[string]func() error{
			"Outbox.Append": func() error { return r.Outbox().Append(canceled, msgEvent(id.String())) },
			"Outbox.ClaimHeads": func() error {
				_, err := r.Outbox().ClaimHeads(canceled, msgRelay1, time.Minute, 10)
				return err
			},
			"Outbox.MarkPublished": func() error {
				_, err := r.Outbox().MarkPublished(canceled, id, msgRelay1)
				return err
			},
			"Outbox.MarkFailed": func() error {
				_, err := r.Outbox().MarkFailed(canceled, id, msgRelay1, time.Second, "boom")
				return err
			},
			"Outbox.Release": func() error { return r.Outbox().Release(canceled, msgRelay1) },
			"Outbox.Stats": func() error {
				_, err := r.Outbox().Stats(canceled)
				return err
			},
			"Inbox.Claim": func() error {
				_, err := r.Inbox().Claim(canceled, msgConsumer, "msg-1", msgHash(1), t0)
				return err
			},
			"Inbox.Complete": func() error {
				return r.Inbox().Complete(canceled, msgConsumer, "msg-1", uuid.NullUUID{}, "REJECTED", t1)
			},
		}
		for name, call := range calls {
			err := call()
			if !errors.Is(err, app.ErrUnavailable) || !errors.Is(err, context.Canceled) {
				t.Errorf("%s on a canceled context: %v, want app.ErrUnavailable wrapping context.Canceled", name, err)
			}
		}
		_, err := r.Outbox().Stats(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
}

func TestOutbox_AppendKeepsTheTraceOfTheUnitOfWork(t *testing.T) {
	resetDB(t)
	tr, rec := tracingtest.New(t)
	uow := newTracedUnitOfWork(t, tr)
	agg := msgAggregates(2)
	traced, untraced := msgEvent(agg[0]), msgEvent(agg[1])

	ctx, root := tracingtest.Root(t.Context(), tr)
	var appended string
	err := uow.Do(ctx, func(ctx context.Context, r app.Repos) error {
		appended = tracing.TraceParent(ctx)
		return r.Outbox().Append(ctx, traced)
	})
	root.End()
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	msgAppend(t, uow, untraced)

	unit := tracingtest.One(t, rec, "uow.do")
	sc := unit.SpanContext()
	if want := "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-01"; appended != want {
		t.Fatalf("traceparent inside the unit = %q, want the unit's own %q", appended, want)
	}
	if got := childrenOf(rec.Ended(), unit); !slices.Contains(got, "BATCH INSERT outbox_events") {
		t.Errorf("statements under the unit = %v, want the batch insert of the outbox", got)
	}
	got := msgClaim(t, uow, msgRelay1, msgLongLease, 10)
	msgRequireIDs(t, got, traced, untraced)
	if got[0].TraceParent != appended {
		t.Errorf("claimed traceparent = %q, want %q", got[0].TraceParent, appended)
	}
	if got[1].TraceParent != "" {
		t.Errorf("claimed traceparent of an untraced event = %q, want empty", got[1].TraceParent)
	}
	owner := connect(t, pg.OwnerDSN)
	if n := queryInt64(t, owner, "SELECT count(*) FROM outbox_events WHERE traceparent IS NULL"); n != 1 {
		t.Errorf("%d events store no traceparent, want 1: an empty one is NULL", n)
	}
}
