package campaign

import (
	"context"

	"github.com/google/uuid"
)

// RunRepository manages fanout lock lifecycle on campaign_region_runs.
type RunRepository interface {
	// AcquireLock atomically claims the fanout lock and returns the run.
	// Returns ErrAlreadyLocked if another worker holds the lock.
	AcquireLock(ctx context.Context, runID uuid.UUID, workerID string) (*Run, error)

	// ExtendLock prolongs the lock TTL between pipeline batches.
	// Returns ErrLockLost if the lock is no longer ours.
	ExtendLock(ctx context.Context, runID uuid.UUID, workerID string) error

	// MarkCompleted transitions the run to fanout_completed.
	MarkCompleted(ctx context.Context, runID uuid.UUID, workerID string) error

	// MarkFailed transitions the run to fanout_failed for recovery.
	MarkFailed(ctx context.Context, runID uuid.UUID, workerID string) error

	// MarkCancelled transitions the run to cancelled and releases lock ownership.
	MarkCancelled(ctx context.Context, runID uuid.UUID, workerID string) error
}

// CampaignRepository loads campaign data needed to build delivery tasks.
type CampaignRepository interface {
	FindByID(ctx context.Context, id uuid.UUID) (*Campaign, error)
	IsCancellationRequested(ctx context.Context, id uuid.UUID) (bool, error)
}

// UserRepository provides keyset-paginated access to target users.
type UserRepository interface {
	// FetchBatch returns up to limit user IDs with id > afterID.
	FetchBatch(
		ctx context.Context,
		regionID string,
		sel RecipientSelector,
		afterID uuid.UUID,
		limit int,
	) ([]uuid.UUID, error)
}

// UserChannelRepository loads active contacts for a set of users.
type UserChannelRepository interface {
	FindActiveByUsers(
		ctx context.Context,
		userIDs []uuid.UUID,
		channelCodes []string,
	) ([]UserChannel, error)
}
