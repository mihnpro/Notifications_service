package task

import (
	"time"

	"github.com/google/uuid"
)

// FinalizeParams is a value object that describes every change made
// in the single atomic finalization transaction after a provider call.
type FinalizeParams struct {
	TaskID     uuid.UUID
	LeaseToken uuid.UUID // CAS guard — if mismatch, finalization is skipped
	AttemptID  uuid.UUID
	CampaignID uuid.UUID

	NewTaskStatus    Status
	NewAttemptStatus AttemptStatus

	// Non-nil only when NewTaskStatus == StatusRetryScheduled.
	RetryAvailableAt *time.Time

	// Provider outcome.
	ProviderRequestID *string
	ErrorType         *string // "transient" | "permanent" | "unknown"
	ErrorCode         *string
	ErrorMessage      *string

	// Stats transition: always sending→NewTaskStatus.
	PrevStatus Status // always StatusSending after lease

	// Non-empty only when NewTaskStatus == StatusRetryScheduled.
	// Used to build the outbox_events routing_key.
	RetryRoutingKey string
}
