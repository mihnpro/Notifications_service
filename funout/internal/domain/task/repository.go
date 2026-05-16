package task

import "context"

// BatchRepository persists a batch of tasks along with their outbox events
// and updates campaign stats — all in one atomic transaction.
// Splitting these into three separate repositories would require a Unit of Work;
// this single interface keeps the transactional contract explicit.
type BatchRepository interface {
	// InsertBatch inserts tasks with ON CONFLICT DO NOTHING on (region_id, idempotency_key).
	// Returns the number of newly inserted rows (0 means all were duplicates).
	InsertBatch(ctx context.Context, tasks []*Task) (int64, error)
}

// StatsRepository updates campaign delivery counters.
type StatsRepository interface {
	IncrementQueued(ctx context.Context, campaignID, totalInserted interface{}) error
}
