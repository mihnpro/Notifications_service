package task

import (
	"context"

	"github.com/google/uuid"
)

// Repository is the single port for all task persistence operations.
type Repository interface {
	// AcquireLease atomically:
	//   1. UPDATEs delivery_tasks → status=sending, sets lease_token + lease_until
	//   2. INSERTs delivery_attempts with status=started
	//   3. INSERTs campaign_stats sending += 1
	// Returns ErrAlreadyLeased or ErrNotAvailable when the task cannot be taken.
	AcquireLease(ctx context.Context, taskID uuid.UUID, workerID string) (*Task, *Attempt, error)

	// ExtendLease prolongs lease_until by 120 s. Called by the heartbeat every 30 s.
	// Returns ErrLeaseExpired if the lease_token no longer matches (recovery reclaimed it).
	ExtendLease(ctx context.Context, taskID uuid.UUID, leaseToken uuid.UUID) error

	// Finalize atomically commits the result of a provider call:
	//   1. CAS UPDATE delivery_tasks WHERE lease_token = params.LeaseToken
	//   2. UPDATE delivery_attempts
	//   3. UPDATE campaign_stats (sending-1, newStatus+1)
	//   4. INSERT dlq_items       (only when dead_lettered)
	//   5. INSERT outbox_events   (only when retry_scheduled)
	// Returns ErrLeaseExpired if the CAS fails (0 rows updated).
	Finalize(ctx context.Context, params FinalizeParams) error
}
