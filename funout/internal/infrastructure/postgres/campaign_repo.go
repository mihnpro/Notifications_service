package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notifications/funout/internal/domain/campaign"
)

type CampaignRepository struct {
	pool *pgxpool.Pool
}

func NewCampaignRepository(pool *pgxpool.Pool) *CampaignRepository {
	return &CampaignRepository{pool: pool}
}

func (r *CampaignRepository) FindByID(ctx context.Context, id uuid.UUID) (*campaign.Campaign, error) {
	const q = `
		SELECT message_snapshot, recipient_selector, selected_channel_codes, priority
		FROM   campaigns
		WHERE  id = $1`

	var (
		c            campaign.Campaign
		selectorJSON []byte
	)
	c.ID = id

	err := r.pool.QueryRow(ctx, q, id).Scan(
		&c.MessageSnapshot,
		&selectorJSON,
		&c.ChannelCodes,
		&c.Priority,
	)
	if err == pgx.ErrNoRows {
		return nil, campaign.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find campaign: %w", err)
	}

	if err := json.Unmarshal(selectorJSON, &c.Selector); err != nil {
		return nil, fmt.Errorf("parse recipient_selector: %w", err)
	}

	return &c, nil
}

func (r *CampaignRepository) IsCancellationRequested(ctx context.Context, id uuid.UUID) (bool, error) {
	const q = `
		SELECT status
		FROM campaigns
		WHERE id = $1`

	var status string
	if err := r.pool.QueryRow(ctx, q, id).Scan(&status); err != nil {
		if err == pgx.ErrNoRows {
			return false, campaign.ErrNotFound
		}
		return false, fmt.Errorf("campaign status lookup: %w", err)
	}
	return status == "cancelling" || status == "cancelled", nil
}
