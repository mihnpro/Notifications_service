package task

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/notifications/funout/internal/domain/campaign"
)

// taskIDNamespace is a fixed UUID v5 namespace for deterministic task ID generation.
// Changing this value would invalidate all existing idempotency keys.
var taskIDNamespace = uuid.MustParse("7f3e2a1b-4c5d-6e7f-8a9b-0c1d2e3f4a5b")

// Task is the delivery unit created for each (user, channel) pair during fanout.
type Task struct {
	ID                       uuid.UUID
	CampaignID               uuid.UUID
	CampaignRegionRunID      uuid.UUID
	RegionID                 string
	UserID                   uuid.UUID
	UserChannelID            uuid.UUID
	ChannelID                uuid.UUID
	ChannelCode              string
	QueueGroup               string
	RecipientAddressSnapshot string
	MessageSnapshot          json.RawMessage
	IdempotencyKey           string
	Priority                 string
	MaxAttempts              int
}

// New builds a Task from a campaign run and a user's channel contact.
// The task ID is deterministic: re-running fanout after a crash produces the same IDs,
// so ON CONFLICT DO NOTHING on idempotency_key prevents duplicates.
func New(run *campaign.Run, camp *campaign.Campaign, uc campaign.UserChannel) *Task {
	id := uuid.NewSHA1(taskIDNamespace, []byte(run.ID.String()+":"+uc.ID.String()))

	return &Task{
		ID:                       id,
		CampaignID:               camp.ID,
		CampaignRegionRunID:      run.ID,
		RegionID:                 run.RegionID,
		UserID:                   uc.UserID,
		UserChannelID:            uc.ID,
		ChannelID:                uc.ChannelID,
		ChannelCode:              uc.ChannelCode,
		QueueGroup:               uc.QueueGroup,
		RecipientAddressSnapshot: uc.Address,
		MessageSnapshot:          camp.MessageSnapshot,
		IdempotencyKey:           fmt.Sprintf("fanout:%s:%s", run.ID, uc.ID),
		Priority:                 camp.Priority,
		MaxAttempts:              defaultMaxAttempts,
	}
}

// RoutingKey returns the RabbitMQ routing key for this task's queue.
func (t *Task) RoutingKey() string {
	return fmt.Sprintf("notification.%s.%s.%s", t.RegionID, t.QueueGroup, t.Priority)
}

// OutboxDedupeKey returns a unique key for the DeliveryTaskCreated outbox event.
func (t *Task) OutboxDedupeKey() string {
	return fmt.Sprintf("delivery_task_created:%s", t.ID)
}

const defaultMaxAttempts = 5
