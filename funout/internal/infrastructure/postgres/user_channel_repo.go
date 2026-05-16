package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notifications/funout/internal/domain/campaign"
)

type UserChannelRepository struct {
	pool *pgxpool.Pool
}

func NewUserChannelRepository(pool *pgxpool.Pool) *UserChannelRepository {
	return &UserChannelRepository{pool: pool}
}

func (r *UserChannelRepository) FindActiveByUsers(
	ctx context.Context,
	userIDs []uuid.UUID,
	channelCodes []string,
) ([]campaign.UserChannel, error) {
	const q = `
		SELECT uc.id, uc.user_id, uc.channel_id, c.code, c.queue_group, uc.address
		FROM   user_channels uc
		JOIN   channels c ON c.id = uc.channel_id
		WHERE  uc.user_id      = ANY($1::uuid[])
		  AND  c.code          = ANY($2::text[])
		  AND  uc.status       = 'active'
		  AND  uc.verified     = true
		  AND  c.state         = 'enabled'`

	rows, err := r.pool.Query(ctx, q, userIDs, channelCodes)
	if err != nil {
		return nil, fmt.Errorf("find active user channels: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (campaign.UserChannel, error) {
		var uc campaign.UserChannel
		err := row.Scan(&uc.ID, &uc.UserID, &uc.ChannelID, &uc.ChannelCode, &uc.QueueGroup, &uc.Address)
		return uc, err
	})
}
