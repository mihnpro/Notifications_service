package provider

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	domainprovider "github.com/notifications/delivery/internal/domain/provider"
)

// StubAdapter simulates a real provider with configurable latency and error rates.
// Used for testing and demos; satisfies domain/provider.Adapter.
type StubAdapter struct {
	SuccessRate   float64       // 0.0–1.0, probability of success
	TransientRate float64       // of failures, probability of transient vs permanent
	MinLatency    time.Duration
	MaxLatency    time.Duration
}

func NewStubAdapter() *StubAdapter {
	return &StubAdapter{
		SuccessRate:   0.8,
		TransientRate: 0.7,
		MinLatency:    2 * time.Second,
		MaxLatency:    300 * time.Second,
	}
}

func (s *StubAdapter) Send(ctx context.Context, payload domainprovider.Payload) (domainprovider.Result, error) {
	latency := s.randomLatency()

	select {
	case <-time.After(latency):
	case <-ctx.Done():
		return domainprovider.Result{}, ctx.Err()
	}

	if rand.Float64() < s.SuccessRate {
		return domainprovider.Result{
			ProviderRequestID: fmt.Sprintf("stub-%s", payload.TaskID),
		}, nil
	}

	if rand.Float64() < s.TransientRate {
		return domainprovider.Result{}, &domainprovider.Error{
			Type:    domainprovider.ErrorTypeTransient,
			Code:    "STUB_TRANSIENT",
			Message: "stub: transient failure (will retry)",
		}
	}

	return domainprovider.Result{}, &domainprovider.Error{
		Type:    domainprovider.ErrorTypePermanent,
		Code:    "STUB_PERMANENT",
		Message: "stub: permanent failure (will DLQ)",
	}
}

func (s *StubAdapter) randomLatency() time.Duration {
	delta := s.MaxLatency - s.MinLatency
	return s.MinLatency + time.Duration(rand.Int64N(int64(delta)))
}
