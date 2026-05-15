package fanout

import "github.com/google/uuid"

// Message is the inbound DTO parsed from a RabbitMQ CampaignRegionRunRequested payload.
type Message struct {
	EventType           string    `json:"event_type"`
	CampaignRegionRunID uuid.UUID `json:"campaign_region_run_id"`
	CampaignID          uuid.UUID `json:"campaign_id"`
	RegionID            string    `json:"region_id"`
}
