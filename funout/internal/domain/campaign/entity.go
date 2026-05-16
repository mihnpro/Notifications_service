package campaign

import (
	"encoding/json"

	"github.com/google/uuid"
)

// Campaign is a read-only snapshot loaded during fanout.
type Campaign struct {
	ID              uuid.UUID
	MessageSnapshot json.RawMessage
	Selector        RecipientSelector
	ChannelCodes    []string
	Priority        string
}

// RecipientSelector is a value object describing who receives the campaign.
type RecipientSelector struct {
	Type        string      `json:"type"` // "all" | "user_ids" | "external_ids"
	UserIDs     []uuid.UUID `json:"user_ids,omitempty"`
	ExternalIDs []string    `json:"external_ids,omitempty"`
}

func (s RecipientSelector) IsAll() bool {
	return s.Type == "" || s.Type == "all"
}

// UserChannel is a value object representing one user's contact on a channel.
type UserChannel struct {
	ID          uuid.UUID
	UserID      uuid.UUID
	ChannelID   uuid.UUID
	ChannelCode string
	QueueGroup  string
	Address     string
}
