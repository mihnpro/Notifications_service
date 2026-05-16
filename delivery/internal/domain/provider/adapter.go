package provider

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
)

// ErrorType classifies provider failures for retry decision.
type ErrorType string

const (
	ErrorTypeTransient ErrorType = "transient"
	ErrorTypePermanent ErrorType = "permanent"
	ErrorTypeUnknown   ErrorType = "unknown"
)

// Error is a typed provider failure returned by Adapter.Send.
type Error struct {
	Type    ErrorType
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Payload is what the delivery worker passes to the provider adapter.
type Payload struct {
	TaskID         uuid.UUID
	ChannelCode    string
	RecipientAddr  string
	Message        json.RawMessage
	IdempotencyKey string
}

// Result is returned on successful delivery.
type Result struct {
	ProviderRequestID string
}

// Adapter is the port for sending notifications through an external channel.
// Implementations live in infrastructure/provider.
type Adapter interface {
	Send(ctx context.Context, payload Payload) (Result, error)
}
