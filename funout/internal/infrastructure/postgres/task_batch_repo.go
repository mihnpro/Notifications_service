package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notifications/funout/internal/domain/task"
)

// TaskBatchRepository implements task.BatchRepository.
// One call = one DB transaction: delivery_tasks + outbox_events + campaign_stats.
type TaskBatchRepository struct {
	pool *pgxpool.Pool
}

func NewTaskBatchRepository(pool *pgxpool.Pool) *TaskBatchRepository {
	return &TaskBatchRepository{pool: pool}
}

func (r *TaskBatchRepository) InsertBatch(ctx context.Context, tasks []*task.Task) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	inserted, err := insertDeliveryTasks(ctx, tx, tasks)
	if err != nil {
		return 0, err
	}

	if err := insertOutboxEvents(ctx, tx, tasks); err != nil {
		return 0, err
	}

	if inserted > 0 {
		if err := upsertCampaignStats(ctx, tx, tasks[0].CampaignID, inserted); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit batch: %w", err)
	}

	return inserted, nil
}

func insertDeliveryTasks(ctx context.Context, tx pgx.Tx, tasks []*task.Task) (int64, error) {
	now := time.Now().UTC()

	batch := &pgx.Batch{}
	for _, t := range tasks {
		batch.Queue(`
			INSERT INTO delivery_tasks (
				id, campaign_id, campaign_region_run_id, region_id,
				user_id, user_channel_id, channel_id,
				channel_code, queue_group,
				recipient_address_snapshot, message_snapshot,
				idempotency_key, status, priority,
				attempt_count, max_attempts, available_at, created_at
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
				'queued',$13,0,$14,$15,$15
			)
			ON CONFLICT (region_id, idempotency_key) DO NOTHING`,
			t.ID, t.CampaignID, t.CampaignRegionRunID, t.RegionID,
			t.UserID, t.UserChannelID, t.ChannelID,
			t.ChannelCode, t.QueueGroup,
			t.RecipientAddressSnapshot, t.MessageSnapshot,
			t.IdempotencyKey, t.Priority, t.MaxAttempts, now,
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()

	var inserted int64
	for range tasks {
		tag, err := br.Exec()
		if err != nil {
			return 0, fmt.Errorf("insert delivery_task: %w", err)
		}
		inserted += tag.RowsAffected()
	}

	return inserted, nil
}

func insertOutboxEvents(ctx context.Context, tx pgx.Tx, tasks []*task.Task) error {
	batch := &pgx.Batch{}
	for _, t := range tasks {
		payload := fmt.Sprintf(
			`{"event_type":"DeliveryTaskCreated","task_id":%q,"campaign_id":%q,"campaign_region_run_id":%q,"region_id":%q,"channel_code":%q,"queue_group":%q,"priority":%q}`,
			t.ID, t.CampaignID, t.CampaignRegionRunID, t.RegionID, t.ChannelCode, t.QueueGroup, t.Priority,
		)

		batch.Queue(`
			INSERT INTO outbox_events (
				region_id, event_type, payload, exchange, routing_key,
				status, dedupe_key, created_at, next_attempt_at
			) VALUES (
				$1, 'DeliveryTaskCreated', $2, 'notification.direct', $3,
				'pending', $4, NOW(), NOW()
			)
			ON CONFLICT (region_id, dedupe_key) DO NOTHING`,
			t.RegionID, []byte(payload), t.RoutingKey(), t.OutboxDedupeKey(),
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()

	for range tasks {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("insert outbox_event: %w", err)
		}
	}

	return nil
}

func upsertCampaignStats(ctx context.Context, tx pgx.Tx, campaignID interface{}, count int64) error {
	const q = `
		INSERT INTO campaign_stats (campaign_id, total_tasks, queued, updated_at)
		VALUES ($1, $2, $2, NOW())
		ON CONFLICT (campaign_id) DO UPDATE
		SET total_tasks = campaign_stats.total_tasks + EXCLUDED.total_tasks,
		    queued      = campaign_stats.queued      + EXCLUDED.queued,
		    updated_at  = NOW()`

	_, err := tx.Exec(ctx, q, campaignID, count)
	return err
}
