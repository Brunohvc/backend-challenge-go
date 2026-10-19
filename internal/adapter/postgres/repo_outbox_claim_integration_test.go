//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const seedOutbox = `INSERT INTO outbox_events
        (id, aggregate_type, aggregate_id, event_type, event_version, payload, correlation_id,
         occurred_at, next_attempt_at, created_at, published_at)
 SELECT gen_random_uuid(), 'Wallet', md5('wallet-' || g % $1)::uuid::text, 'WalletBalanceChanged', 1,
        jsonb_build_object('eventId', g, 'data', jsonb_build_object('amount', '1.00', 'currency', 'BRL')),
        'corr-' || g, now(), now(), now(), CASE WHEN $3 THEN now() END
   FROM generate_series(1, $2) g`

func msgSeed(ctx context.Context, tb testing.TB, db execer, aggregates, events int, published bool) {
	tb.Helper()
	if _, err := db.Exec(ctx, seedOutbox, aggregates, events, published); err != nil {
		tb.Fatalf("seed %d events over %d aggregates: %v", events, aggregates, err)
	}
}

type msgRace struct {
	mu        sync.Mutex
	position  map[uuid.UUID]int
	order     map[string][]uuid.UUID
	next      map[string]int
	inFlight  map[string]uuid.UUID
	published int
	total     int
}

func newMsgRace(total int) *msgRace {
	return &msgRace{
		position: make(map[uuid.UUID]int),
		order:    make(map[string][]uuid.UUID),
		next:     make(map[string]int),
		inFlight: make(map[string]uuid.UUID),
		total:    total,
	}
}

func (r *msgRace) appended(recs []app.OutboxRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range recs {
		r.position[rec.EventID] = len(r.position)
		r.order[rec.AggregateID] = append(r.order[rec.AggregateID], rec.EventID)
	}
}

func (r *msgRace) claimed(owner string, recs []app.OutboxRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	last := -1
	inBatch := make(map[string]bool, len(recs))
	for _, rec := range recs {
		agg := rec.AggregateID
		switch pos, known := r.position[rec.EventID]; {
		case !known:
			return fmt.Errorf("%s claimed %s, which was never appended", owner, rec.EventID)
		case pos <= last:
			return fmt.Errorf("%s claimed a batch out of append order at %s", owner, rec.EventID)
		default:
			last = pos
		}
		if inBatch[agg] {
			return fmt.Errorf("%s claimed two events of aggregate %s in one batch", owner, agg)
		}
		inBatch[agg] = true
		if held, busy := r.inFlight[agg]; busy {
			return fmt.Errorf("%s claimed %s while %s of the same aggregate was in flight", owner, rec.EventID, held)
		}
		if r.next[agg] >= len(r.order[agg]) {
			return fmt.Errorf("%s claimed %s of aggregate %s, whose events were all published", owner, rec.EventID, agg)
		}
		if want := r.order[agg][r.next[agg]]; rec.EventID != want {
			return fmt.Errorf("%s claimed %s of aggregate %s, want %s, the oldest unpublished", owner, rec.EventID, agg, want)
		}
		r.inFlight[agg] = rec.EventID
	}
	return nil
}

func (r *msgRace) settled(rec app.OutboxRecord, published bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inFlight, rec.AggregateID)
	if published {
		r.next[rec.AggregateID]++
		r.published++
	}
}

func (r *msgRace) done() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.published == r.total
}

func msgRaceAppend(ctx context.Context, uow app.UnitOfWork, race *msgRace, recs []app.OutboxRecord) error {
	return uow.Do(ctx, func(ctx context.Context, r app.Repos) error {
		if err := r.Outbox().Append(ctx, recs...); err != nil {
			return err
		}
		race.appended(recs)
		return nil
	})
}

func msgRaceRelay(ctx context.Context, uow app.UnitOfWork, race *msgRace, owner string, rnd *rand.Rand) error {
	for !race.done() {
		recs, err := msgTryClaim(ctx, uow, owner, msgLongLease, 1+rnd.IntN(4))
		if err != nil {
			return fmt.Errorf("%s: claim: %w", owner, err)
		}
		if err := race.claimed(owner, recs); err != nil {
			return err
		}
		if len(recs) == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		err = uow.Do(ctx, func(ctx context.Context, r app.Repos) error {
			for _, rec := range recs {
				publish := rnd.IntN(4) > 0
				var held bool
				var err error
				if publish {
					held, err = r.Outbox().MarkPublished(ctx, rec.EventID, owner)
				} else {
					retryIn := time.Duration(rnd.IntN(20)) * time.Millisecond
					held, err = r.Outbox().MarkFailed(ctx, rec.EventID, owner, retryIn, "broker refused")
				}
				if err != nil {
					return err
				}
				if !held {
					return fmt.Errorf("%s lost the lease of %s", owner, rec.EventID)
				}
				race.settled(rec, publish)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("%s: settle: %w", owner, err)
		}
	}
	return nil
}

func TestOutbox_RacingRelaysKeepEachAggregateInOrder(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	rnd := rand.New(rand.NewPCG(7, 11))
	agg := msgAggregates(6)
	events := make([]app.OutboxRecord, 240)
	for i := range events {
		hot := rnd.IntN(10) < 6
		if hot {
			events[i] = msgEvent(agg[rnd.IntN(2)])
		} else {
			events[i] = msgEvent(agg[2+rnd.IntN(4)])
		}
	}
	race := newMsgRace(len(events))
	upFront, later := events[:160], events[160:]
	for batch := range slices.Chunk(upFront, 40) {
		if err := msgRaceAppend(t.Context(), uow, race, batch); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	wg.Go(func() {
		for batch := range slices.Chunk(later, 8) {
			if err := msgRaceAppend(ctx, uow, race, batch); err != nil {
				errs <- fmt.Errorf("writer: %w", err)
				cancel()
				return
			}
		}
	})
	for i, owner := range []string{msgRelay1, msgRelay2, msgRelay3} {
		relayRnd := rand.New(rand.NewPCG(uint64(i), 13))
		wg.Go(func() {
			if err := msgRaceRelay(ctx, uow, race, owner, relayRnd); err != nil {
				errs <- err
				cancel()
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}
	if t.Failed() {
		return
	}

	owner := connect(t, pg.OwnerDSN)
	if n := queryInt64(t, owner, "SELECT count(*) FROM outbox_events WHERE published_at IS NULL"); n != 0 {
		t.Fatalf("%d events left unpublished", n)
	}
	for _, a := range agg {
		rows, err := owner.Query(t.Context(),
			"SELECT id FROM outbox_events WHERE aggregate_id = $1 ORDER BY published_at, seq", a)
		if err != nil {
			t.Fatalf("read the publications of %s: %v", a, err)
		}
		published, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
		if err != nil {
			t.Fatalf("read the publications of %s: %v", a, err)
		}
		if want := race.order[a]; !slices.Equal(published, want) {
			t.Errorf("aggregate %s published %v, want %v in append order", a, published, want)
		}
	}
}

type msgPlanNode struct {
	NodeType         string        `json:"Node Type"`
	RelationName     string        `json:"Relation Name"`
	ActualRows       json.Number   `json:"Actual Rows"`
	ActualLoops      json.Number   `json:"Actual Loops"`
	RemovedByFilter  json.Number   `json:"Rows Removed by Filter"`
	RemovedByRecheck json.Number   `json:"Rows Removed by Index Recheck"`
	RemovedByJoin    json.Number   `json:"Rows Removed by Join Filter"`
	Plans            []msgPlanNode `json:"Plans"`
}

func (n msgPlanNode) rowsRead(t *testing.T, relation string) int64 {
	t.Helper()
	var rows int64
	if n.RelationName == relation && strings.HasSuffix(n.NodeType, "Scan") {
		rows += msgTotal(t, n.ActualRows, n.ActualLoops) + msgTotal(t, n.RemovedByFilter, n.ActualLoops) +
			msgTotal(t, n.RemovedByRecheck, n.ActualLoops)
	}
	rows += msgTotal(t, n.RemovedByJoin, n.ActualLoops)
	for _, child := range n.Plans {
		rows += child.rowsRead(t, relation)
	}
	return rows
}

func msgTotal(t *testing.T, perLoop, loops json.Number) int64 {
	t.Helper()
	return (msgHundredths(t, perLoop)*msgHundredths(t, loops)/100 + 99) / 100
}

func msgHundredths(t *testing.T, n json.Number) int64 {
	t.Helper()
	if n == "" {
		return 0
	}
	whole, frac, _ := strings.Cut(n.String(), ".")
	v, err := strconv.ParseInt(whole+(frac + "00")[:2], 10, 64)
	if err != nil {
		t.Fatalf("plan number %q: %v", n, err)
	}
	return v
}

func msgExplainClaim(t *testing.T, limit int, generic bool) msgPlanNode {
	t.Helper()
	tx := begin(t, connect(t, pg.AppDSN))
	const explain = "EXPLAIN (ANALYZE, FORMAT JSON) "
	query, args := explain+postgres.ClaimOutboxHeads, []any{msgRelay1, msgLongLease, limit}
	if generic {
		mustExec(t, tx, "SET LOCAL plan_cache_mode = force_generic_plan")
		mustExec(t, tx, "PREPARE msg_claim AS "+postgres.ClaimOutboxHeads)
		query = fmt.Sprintf("%sEXECUTE msg_claim('%s', '%d seconds', %d)",
			explain, msgRelay1, int64(msgLongLease/time.Second), limit)
		args = nil
	}
	var out string
	if err := tx.QueryRow(t.Context(), query, args...).Scan(&out); err != nil {
		t.Fatalf("explain the claim: %v", err)
	}
	if generic {
		if n := queryInt64(t, tx, "SELECT generic_plans FROM pg_prepared_statements WHERE name = 'msg_claim'"); n != 1 {
			t.Fatalf("the claim ran %d generic plans, want 1", n)
		}
	}
	var explained []struct {
		Plan msgPlanNode `json:"Plan"`
	}
	dec := json.NewDecoder(strings.NewReader(out))
	dec.UseNumber()
	if err := dec.Decode(&explained); err != nil || len(explained) != 1 {
		t.Fatalf("decode the plan (%v): %s", err, out)
	}
	return explained[0].Plan
}

func TestOutbox_ClaimReadsAboutOneRowPerAggregateNotTheBacklog(t *testing.T) {
	const published, pending, limit = 20000, 20000, 500
	for _, aggregates := range []int{5, 200} {
		for _, statsFirst := range []bool{true, false} {
			name := fmt.Sprintf("%d aggregates, statistics taken with the backlog", aggregates)
			if statsFirst {
				name = fmt.Sprintf("%d aggregates, statistics taken before the backlog", aggregates)
			}
			t.Run(name, func(t *testing.T) {
				resetDB(t)
				owner := connect(t, pg.OwnerDSN)
				msgSeed(t.Context(), t, owner, aggregates, published, true)
				if statsFirst {
					msgAnalyze(t.Context(), t, owner)
				}
				msgSeed(t.Context(), t, owner, aggregates, pending, false)
				if !statsFirst {
					msgAnalyze(t.Context(), t, owner)
				}

				for _, generic := range []bool{false, true} {
					mode := "custom plan"
					if generic {
						mode = "generic plan"
					}
					t.Run(mode, func(t *testing.T) {
						plan := msgExplainClaim(t, limit, generic)

						if claimed := msgTotal(t, plan.ActualRows, plan.ActualLoops); claimed != int64(aggregates) {
							t.Fatalf("claimed %d events, want the head of each of the %d aggregates", claimed, aggregates)
						}
						if read, most := plan.rowsRead(t, "outbox_events"), int64(4*(aggregates+1)); read > most {
							t.Fatalf("the claim read %d rows of outbox_events for %d aggregates with %d pending events, want at most %d",
								read, aggregates, pending, most)
						}
					})
				}
			})
		}
	}
}

func BenchmarkOutbox_ClaimHeads(b *testing.B) {
	const published, pending, limit = 50000, 50000, 500
	errRolledBack := errors.New("rolled back to keep the backlog")
	for _, aggregates := range []int{5, 200} {
		for _, stats := range []string{"stats_before_the_backlog", "stats_with_the_backlog"} {
			b.Run(fmt.Sprintf("%d_aggregates/%s", aggregates, stats), func(b *testing.B) {
				ctx := b.Context()
				if err := pg.Reset(ctx); err != nil {
					b.Fatalf("reset: %v", err)
				}
				owner, err := pgx.Connect(ctx, pg.OwnerDSN)
				if err != nil {
					b.Fatalf("connect: %v", err)
				}
				defer func() { _ = owner.Close(context.WithoutCancel(ctx)) }()
				msgSeed(ctx, b, owner, aggregates, published, true)
				if stats == "stats_before_the_backlog" {
					msgAnalyze(ctx, b, owner)
				}
				msgSeed(ctx, b, owner, aggregates, pending, false)
				if stats == "stats_with_the_backlog" {
					msgAnalyze(ctx, b, owner)
				}
				pool, err := postgres.NewPool(ctx, dbConfig(), "bench")
				if err != nil {
					b.Fatalf("NewPool() error = %v", err)
				}
				defer pool.Close()
				uow := postgres.NewUnitOfWork(pool, tracing.Tracer{})

				for b.Loop() {
					var claimed int
					err := uow.Do(ctx, func(ctx context.Context, r app.Repos) error {
						recs, err := r.Outbox().ClaimHeads(ctx, msgRelay1, msgLongLease, limit)
						if err != nil {
							return err
						}
						claimed = len(recs)
						return errRolledBack
					})
					if !errors.Is(err, errRolledBack) {
						b.Fatalf("ClaimHeads() error = %v", err)
					}
					if claimed != aggregates {
						b.Fatalf("claimed %d events, want one per aggregate, %d", claimed, aggregates)
					}
				}
			})
		}
	}
}

func msgAnalyze(ctx context.Context, tb testing.TB, db execer) {
	tb.Helper()
	if _, err := db.Exec(ctx, "ANALYZE outbox_events"); err != nil {
		tb.Fatalf("analyze: %v", err)
	}
}
