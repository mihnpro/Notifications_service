package fanout

import "github.com/google/uuid"

// Message is the inbound DTO parsed from a RabbitMQ CampaignRegionRunRequested payload.
type Message struct {
	EventType           string    `json:"messageType"`
	CampaignRegionRunID uuid.UUID `json:"campaignRegionRunId"`
	CampaignID          uuid.UUID `json:"campaignId"`
	RegionID            string    `json:"regionId"`
}
