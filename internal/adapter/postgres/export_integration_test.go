//go:build integration

package postgres

import (
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
)

const ClaimOutboxHeads = claimOutboxHeads

func TxOf(r app.Repos) pgx.Tx {
	rs, ok := r.(repos)
	if !ok {
		panic(fmt.Sprintf("postgres: %T was not handed out by a UnitOfWork", r))
	}
	return rs.tx
}
