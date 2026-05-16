package delivery

import "github.com/google/uuid"

// Message is the inbound DTO parsed from a RabbitMQ DeliveryTaskCreated payload.
// Published by the outbox publisher after the fanout worker creates delivery_tasks.
type Message struct {
	EventType           string    `json:"event_type"`
	TaskID              uuid.UUID `json:"task_id"`
	CampaignID          uuid.UUID `json:"campaign_id"`
	CampaignRegionRunID uuid.UUID `json:"campaign_region_run_id"`
	RegionID            string    `json:"region_id"`
	ChannelCode         string    `json:"channel_code"`
	QueueGroup          string    `json:"queue_group"`
	Priority            string    `json:"priority"`
}
