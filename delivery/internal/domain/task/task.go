package task

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusQueued        Status = "queued"
	StatusSending       Status = "sending"
	StatusSucceeded     Status = "succeeded"
	StatusFailed        Status = "failed"
	StatusRetryScheduled Status = "retry_scheduled"
	StatusDeadLettered  Status = "dead_lettered"
	StatusCancelled     Status = "cancelled"
)

// Task is the delivery unit loaded from DB before provider call.
type Task struct {
	ID                       uuid.UUID
	CampaignID               uuid.UUID
	CampaignRegionRunID      uuid.UUID
	RegionID                 string
	UserID                   uuid.UUID
	ChannelCode              string
	QueueGroup               string
	RecipientAddressSnapshot string
	MessageSnapshot          json.RawMessage
	IdempotencyKey           string
	Status                   Status
	Priority                 string
	AttemptCount             int
	MaxAttempts              int
	AvailableAt              time.Time
	LeaseOwner               *string
	LeaseToken               *uuid.UUID
	LeaseUntil               *time.Time
}

// CanRetry reports whether the task has remaining attempts after this one.
func (t *Task) CanRetry() bool {
	return t.AttemptCount < t.MaxAttempts
}

// IsAvailableNow reports whether the retry delay has passed.
func (t *Task) IsAvailableNow() bool {
	return !time.Now().Before(t.AvailableAt)
}

// Ref is the addressing triple required to identify a task across workers.
type Ref struct {
	TaskID uuid.UUID
}
