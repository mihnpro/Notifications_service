package campaign

import (
	"time"

	"github.com/google/uuid"
)

type RunStatus string

const (
	RunStatusPending   RunStatus = "fanout_pending"
	RunStatusRunning   RunStatus = "fanout_running"
	RunStatusCompleted RunStatus = "fanout_completed"
	RunStatusFailed    RunStatus = "fanout_failed"
)

// Run represents the per-region execution of a campaign fanout.
type Run struct {
	ID                 uuid.UUID
	CampaignID         uuid.UUID
	RegionID           string
	Status             RunStatus
	FanoutLockOwner    *string
	FanoutLockUntil    *time.Time
	FanoutAttemptCount int
}

// IsLocked returns true if another worker currently holds the fanout lock.
func (r *Run) IsLocked(now time.Time) bool {
	return r.FanoutLockUntil != nil && r.FanoutLockUntil.After(now)
}

func (r *Run) IsCompleted() bool {
	return r.Status == RunStatusCompleted
}
