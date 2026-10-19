package postgres

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

// NewPool opens the connection pool of the service and checks that it can connect.
func NewPool(ctx context.Context, cfg config.DBConfig, instanceID string, opts ...PoolOption) (*pgxpool.Pool, error) {
	pc, err := poolConfig(cfg, instanceID, opts...)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres: create the pool: %w", classify(err))
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: connect: %w", classify(err))
	}
	return pool, nil
}

func poolConfig(cfg config.DBConfig, instanceID string, opts ...PoolOption) (*pgxpool.Config, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse the database url: %w", classify(err))
	}
	pc.MaxConns = cfg.MaxConns
	params := pc.ConnConfig.RuntimeParams
	params["application_name"] = instanceID
	params["lock_timeout"] = milliseconds(cfg.LockTimeout)
	params["statement_timeout"] = milliseconds(cfg.StatementTimeout)
	for _, opt := range opts {
		opt(pc)
	}
	return pc, nil
}

func milliseconds(d time.Duration) string {
	ms := d / time.Millisecond
	if d%time.Millisecond > 0 {
		ms++
	}
	return strconv.FormatInt(int64(ms), 10)
}
