package task

import "github.com/google/uuid"

type AttemptStatus string

const (
	AttemptStarted   AttemptStatus = "started"
	AttemptSucceeded AttemptStatus = "succeeded"
	AttemptFailed    AttemptStatus = "failed"
	AttemptTimedOut  AttemptStatus = "timed_out"
	AttemptStale     AttemptStatus = "stale"
)

// Attempt is the record created at the start of each provider call.
type Attempt struct {
	ID          uuid.UUID
	TaskID      uuid.UUID
	CampaignID  uuid.UUID
	AttemptNo   int
	WorkerID    string
	ChannelCode string
	Status      AttemptStatus
}
