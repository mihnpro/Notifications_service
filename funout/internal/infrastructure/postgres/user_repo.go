package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notifications/funout/internal/domain/campaign"
)

type UserRepository struct {
	pool *pgxpool.Pool
}

func NewUserRepository(pool *pgxpool.Pool) *UserRepository {
	return &UserRepository{pool: pool}
}

func (r *UserRepository) FetchBatch(
	ctx context.Context,
	regionID string,
	sel campaign.RecipientSelector,
	afterID uuid.UUID,
	limit int,
) ([]uuid.UUID, error) {
	var (
		rows pgx.Rows
		err  error
	)

	switch sel.Type {
	case "user_ids":
		const q = `
			SELECT id FROM users
			WHERE  id > $1
			  AND  id = ANY($2::uuid[])
			  AND  status = 'active'
			ORDER BY id LIMIT $3`
		rows, err = r.pool.Query(ctx, q, afterID, sel.UserIDs, limit)

	case "external_ids":
		const q = `
			SELECT id FROM users
			WHERE  id > $1
			  AND  external_id = ANY($2::text[])
			  AND  status = 'active'
			ORDER BY id LIMIT $3`
		rows, err = r.pool.Query(ctx, q, afterID, sel.ExternalIDs, limit)

	default: // "all"
		const q = `
			SELECT id FROM users
			WHERE  id > $1
			  AND  region_id = $2
			  AND  status = 'active'
			ORDER BY id LIMIT $3`
		rows, err = r.pool.Query(ctx, q, afterID, regionID, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("fetch user batch: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (uuid.UUID, error) {
		var id uuid.UUID
		return id, row.Scan(&id)
	})
}
