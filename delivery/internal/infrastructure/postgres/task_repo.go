package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notifications/delivery/internal/domain/task"
)

// TaskRepository implements task.Repository.
type TaskRepository struct {
	pool     *pgxpool.Pool
	workerID string
}

func NewTaskRepository(pool *pgxpool.Pool, workerID string) *TaskRepository {
	return &TaskRepository{pool: pool, workerID: workerID}
}

// ─── AcquireLease ────────────────────────────────────────────────────────────

func (r *TaskRepository) AcquireLease(ctx context.Context, taskID uuid.UUID, workerID string) (*task.Task, *task.Attempt, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin lease tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	t, err := acquireLeaseOnTask(ctx, tx, taskID, workerID)
	if err != nil {
		return nil, nil, err
	}

	attempt, err := insertAttempt(ctx, tx, t, workerID)
	if err != nil {
		return nil, nil, fmt.Errorf("insert attempt: %w", err)
	}

	if err := incrementSending(ctx, tx, t.CampaignID); err != nil {
		return nil, nil, fmt.Errorf("increment sending stats: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("commit lease tx: %w", err)
	}

	return t, attempt, nil
}

func acquireLeaseOnTask(ctx context.Context, tx pgx.Tx, taskID uuid.UUID, workerID string) (*task.Task, error) {
	const q = `
		UPDATE delivery_tasks
		SET    status        = 'sending',
		       lease_owner   = $1,
		       lease_token   = gen_random_uuid(),
		       lease_until   = NOW() + INTERVAL '120 seconds',
		       attempt_count = attempt_count + 1,
		       started_at    = NOW()
		WHERE  id           = $2
		  AND  status       IN ('queued', 'retry_scheduled')
		  AND  available_at  <= NOW()
		  AND  (lease_until IS NULL OR lease_until < NOW())
		RETURNING
		    id, campaign_id, campaign_region_run_id, region_id,
		    channel_code, queue_group,
		    recipient_address_snapshot, message_snapshot,
		    idempotency_key, status, priority,
		    attempt_count, max_attempts,
		    lease_owner, lease_token, lease_until, available_at`

	var t task.Task
	err := tx.QueryRow(ctx, q, workerID, taskID).Scan(
		&t.ID, &t.CampaignID, &t.CampaignRegionRunID, &t.RegionID,
		&t.ChannelCode, &t.QueueGroup,
		&t.RecipientAddressSnapshot, &t.MessageSnapshot,
		&t.IdempotencyKey, &t.Status, &t.Priority,
		&t.AttemptCount, &t.MaxAttempts,
		&t.LeaseOwner, &t.LeaseToken, &t.LeaseUntil, &t.AvailableAt,
	)
	if err == pgx.ErrNoRows {
		// Distinguish: check if task exists and why it wasn't updated.
		return nil, classifyLeaseFailure(ctx, tx, taskID)
	}
	if err != nil {
		return nil, fmt.Errorf("acquire lease update: %w", err)
	}

	return &t, nil
}

// classifyLeaseFailure reads the task to return a meaningful domain error.
func classifyLeaseFailure(ctx context.Context, tx pgx.Tx, taskID uuid.UUID) error {
	const q = `SELECT status, available_at, lease_until FROM delivery_tasks WHERE id = $1`

	var (
		status      string
		availableAt time.Time
		leaseUntil  *time.Time
	)

	err := tx.QueryRow(ctx, q, taskID).Scan(&status, &availableAt, &leaseUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return task.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("classify lease failure: %w", err)
	}

	if leaseUntil != nil && leaseUntil.After(time.Now()) {
		return task.ErrAlreadyLeased
	}
	if availableAt.After(time.Now()) {
		return task.ErrNotAvailable
	}
	// Task exists but status is terminal (succeeded/failed/etc.) — treat as already leased.
	return task.ErrAlreadyLeased
}

func insertAttempt(ctx context.Context, tx pgx.Tx, t *task.Task, workerID string) (*task.Attempt, error) {
	const q = `
		INSERT INTO delivery_attempts (task_id, campaign_id, attempt_no, worker_id, channel_code, status, started_at)
		VALUES ($1, $2, $3, $4, $5, 'started', NOW())
		RETURNING id`

	var attempt task.Attempt
	attempt.TaskID = t.ID
	attempt.CampaignID = t.CampaignID
	attempt.AttemptNo = t.AttemptCount
	attempt.WorkerID = workerID
	attempt.ChannelCode = t.ChannelCode
	attempt.Status = task.AttemptStarted

	err := tx.QueryRow(ctx, q, t.ID, t.CampaignID, t.AttemptCount, workerID, t.ChannelCode).Scan(&attempt.ID)
	return &attempt, err
}

func incrementSending(ctx context.Context, tx pgx.Tx, campaignID uuid.UUID) error {
	const q = `
		UPDATE campaign_stats
		SET sending    = sending + 1,
		    queued     = queued  - 1,
		    updated_at = NOW()
		WHERE campaign_id = $1`
	_, err := tx.Exec(ctx, q, campaignID)
	return err
}

// ─── ExtendLease ─────────────────────────────────────────────────────────────

func (r *TaskRepository) ExtendLease(ctx context.Context, taskID uuid.UUID, leaseToken uuid.UUID) error {
	const q = `
		UPDATE delivery_tasks
		SET    lease_until = NOW() + INTERVAL '120 seconds'
		WHERE  id          = $1
		  AND  lease_token = $2`

	tag, err := r.pool.Exec(ctx, q, taskID, leaseToken)
	if err != nil {
		return fmt.Errorf("extend lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return task.ErrLeaseExpired
	}
	return nil
}

// ─── Finalize ─────────────────────────────────────────────────────────────────

func (r *TaskRepository) Finalize(ctx context.Context, params task.FinalizeParams) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin finalize tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// 1. CAS update on delivery_tasks.
	if err := finalizeTask(ctx, tx, params); err != nil {
		return err // ErrLeaseExpired propagates without wrapping
	}

	// 2. Update delivery_attempt.
	if err := finalizeAttempt(ctx, tx, params); err != nil {
		return fmt.Errorf("finalize attempt: %w", err)
	}

	// 3. Update campaign_stats.
	if err := updateStats(ctx, tx, params); err != nil {
		return fmt.Errorf("update stats: %w", err)
	}

	// 4. Insert dlq_item if dead-lettered.
	if params.NewTaskStatus == task.StatusDeadLettered {
		if err := insertDLQItem(ctx, tx, params); err != nil {
			return fmt.Errorf("insert dlq item: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func finalizeTask(ctx context.Context, tx pgx.Tx, p task.FinalizeParams) error {
	var availableAt any
	if p.RetryAvailableAt != nil {
		availableAt = *p.RetryAvailableAt
	}

	const q = `
		UPDATE delivery_tasks
		SET    status       = $1,
		       lease_token  = NULL,
		       lease_until  = NULL,
		       lease_owner  = NULL,
		       available_at = COALESCE($2, available_at),
		       completed_at = CASE WHEN $1 IN ('succeeded','failed','dead_lettered') THEN NOW() ELSE NULL END
		WHERE  id          = $3
		  AND  lease_token = $4`

	tag, err := tx.Exec(ctx, q, string(p.NewTaskStatus), availableAt, p.TaskID, p.LeaseToken)
	if err != nil {
		return fmt.Errorf("finalize task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return task.ErrLeaseExpired
	}
	return nil
}

func finalizeAttempt(ctx context.Context, tx pgx.Tx, p task.FinalizeParams) error {
	const q = `
		UPDATE delivery_attempts
		SET    status              = $1,
		       completed_at        = NOW(),
		       provider_request_id = $2,
		       error_type          = $3,
		       error_code          = $4,
		       error_message       = $5
		WHERE  id = $6`

	_, err := tx.Exec(ctx, q,
		string(p.NewAttemptStatus),
		p.ProviderRequestID,
		p.ErrorType,
		p.ErrorCode,
		p.ErrorMessage,
		p.AttemptID,
	)
	return err
}

func updateStats(ctx context.Context, tx pgx.Tx, p task.FinalizeParams) error {
	col := statsColumn(p.NewTaskStatus)
	if col == "" {
		return nil
	}

	q := fmt.Sprintf(`
		UPDATE campaign_stats
		SET sending    = sending - 1,
		    %s         = %s + 1,
		    updated_at = NOW()
		WHERE campaign_id = $1`, col, col)

	_, err := tx.Exec(ctx, q, p.CampaignID)
	return err
}

func statsColumn(s task.Status) string {
	switch s {
	case task.StatusSucceeded:
		return "succeeded"
	case task.StatusFailed:
		return "failed"
	case task.StatusRetryScheduled:
		return "retry_scheduled"
	case task.StatusDeadLettered:
		return "dead_lettered"
	default:
		return ""
	}
}

func insertDLQItem(ctx context.Context, tx pgx.Tx, p task.FinalizeParams) error {
	const q = `
		INSERT INTO dlq_items (task_id, campaign_id, channel_code, reason_code, error_code, error_message, status, created_at)
		VALUES ($1, $2, $3, 'max_attempts_exceeded', $4, $5, 'open', NOW())
		ON CONFLICT (task_id) WHERE status = 'open' DO NOTHING`

	_, err := tx.Exec(ctx, q, p.TaskID, p.CampaignID, p.ChannelCode, p.ErrorCode, p.ErrorMessage)
	return err
}
