package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notifications/funout/internal/domain/campaign"
)

type RunRepository struct {
	pool *pgxpool.Pool
}

func NewRunRepository(pool *pgxpool.Pool) *RunRepository {
	return &RunRepository{pool: pool}
}

func (r *RunRepository) AcquireLock(ctx context.Context, runID uuid.UUID, workerID string) (*campaign.Run, error) {
	const q = `
		UPDATE campaign_region_runs
		SET    status               = 'fanout_running',
		       fanout_lock_owner    = $1,
		       fanout_lock_until    = NOW() + INTERVAL '90 seconds',
		       fanout_attempt_count = fanout_attempt_count + 1
		WHERE  id     = $2
		  AND  status = 'fanout_pending'
		  AND  (fanout_lock_until IS NULL OR fanout_lock_until < NOW())
		RETURNING id, campaign_id, region_id, status, fanout_lock_owner, fanout_attempt_count`

	var run campaign.Run
	var owner *string

	err := r.pool.QueryRow(ctx, q, workerID, runID).Scan(
		&run.ID, &run.CampaignID, &run.RegionID, &run.Status,
		&owner, &run.FanoutAttemptCount,
	)
	if err == pgx.ErrNoRows {
		return nil, campaign.ErrAlreadyLocked
	}
	if err != nil {
		return nil, fmt.Errorf("acquire fanout lock: %w", err)
	}

	run.FanoutLockOwner = owner
	return &run, nil
}

func (r *RunRepository) ExtendLock(ctx context.Context, runID uuid.UUID, workerID string) error {
	const q = `
		UPDATE campaign_region_runs
		SET    fanout_lock_until = NOW() + INTERVAL '90 seconds'
		WHERE  id = $1 AND fanout_lock_owner = $2`

	tag, err := r.pool.Exec(ctx, q, runID, workerID)
	if err != nil {
		return fmt.Errorf("extend fanout lock: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return campaign.ErrLockLost
	}
	return nil
}

func (r *RunRepository) MarkCompleted(ctx context.Context, runID uuid.UUID, workerID string) error {
	const q = `
		UPDATE campaign_region_runs
		SET    status       = 'fanout_completed',
		       completed_at = NOW()
		WHERE  id = $1 AND fanout_lock_owner = $2`

	tag, err := r.pool.Exec(ctx, q, runID, workerID)
	if err != nil {
		return fmt.Errorf("mark run completed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return campaign.ErrLockLost
	}
	return nil
}

func (r *RunRepository) MarkFailed(ctx context.Context, runID uuid.UUID, workerID string) error {
	const q = `
		UPDATE campaign_region_runs
		SET    status = 'fanout_failed'
		WHERE  id = $1 AND fanout_lock_owner = $2`

	_, err := r.pool.Exec(ctx, q, runID, workerID)
	return err
}
