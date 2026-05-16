package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/notifications/delivery/internal/domain/provider"
	"github.com/notifications/delivery/internal/domain/task"
)

// Service orchestrates the full delivery flow for one message.
// Depends only on domain interfaces — no pgx, no amqp.
type Service struct {
	tasks    task.Repository
	adapter  provider.Adapter
	workerID string
}

func NewService(tasks task.Repository, adapter provider.Adapter, workerID string) *Service {
	return &Service{tasks: tasks, adapter: adapter, workerID: workerID}
}

// Process handles one raw RabbitMQ delivery body.
// Returns nil → caller should Ack.
// Returns ErrAlreadyLeased / ErrNotAvailable → caller should Ack (safe to drop).
// Returns any other error → caller should Nack (infrastructure or transient failure).
func (s *Service) Process(ctx context.Context, body []byte) error {
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return fmt.Errorf("parse delivery message: %w", err)
	}

	log := slog.With("task_id", msg.TaskID, "channel", msg.ChannelCode, "worker", s.workerID)

	// ── 1. Acquire lease ────────────────────────────────────────────────────
	t, attempt, err := s.tasks.AcquireLease(ctx, msg.TaskID, s.workerID)
	switch {
	case errors.Is(err, task.ErrAlreadyLeased):
		log.Info("delivery skipped: task already leased")
		return task.ErrAlreadyLeased
	case errors.Is(err, task.ErrNotAvailable):
		log.Info("delivery skipped: task not yet available")
		return task.ErrNotAvailable
	case err != nil:
		return fmt.Errorf("acquire lease: %w", err)
	}

	log.Info("lease acquired", "attempt", t.AttemptCount)

	// ── 2. Heartbeat ─────────────────────────────────────────────────────────
	hbCtx, hbCancel := context.WithCancel(ctx)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		runHeartbeat(hbCtx, s.tasks, t.ID, *t.LeaseToken)
	}()

	// ── 3. Provider call ──────────────────────────────────────────────────────
	provCtx, provCancel := context.WithTimeout(ctx, providerTimeout)
	result, provErr := s.adapter.Send(provCtx, provider.Payload{
		TaskID:         t.ID,
		ChannelCode:    t.ChannelCode,
		RecipientAddr:  t.RecipientAddressSnapshot,
		Message:        t.MessageSnapshot,
		IdempotencyKey: t.IdempotencyKey,
	})
	provCancel()

	// Stop heartbeat and wait for the goroutine to exit before finalizing.
	hbCancel()
	<-hbDone

	// ── 4. Build finalization params ─────────────────────────────────────────
	params := s.buildFinalizeParams(t, attempt, msg, result, provErr)

	// Campaign cancellation policy: no new retries should be scheduled once
	// cancellation was requested; force a terminal transition instead.
	if params.NewTaskStatus == task.StatusRetryScheduled {
		cancelRequested, err := s.tasks.IsCampaignCancellationRequested(ctx, t.CampaignID)
		if err != nil {
			return fmt.Errorf("check campaign cancellation: %w", err)
		}
		if cancelRequested {
			params.NewTaskStatus = task.StatusCancelled
			params.RetryAvailableAt = nil
		}
	}

	// ── 5. Finalize ───────────────────────────────────────────────────────────
	if err := s.tasks.Finalize(ctx, params); errors.Is(err, task.ErrLeaseExpired) {
		// Recovery reclaimed the task while we were calling the provider.
		// Ack the message — recovery will reschedule it.
		log.Warn("finalization skipped: lease was reclaimed by recovery")
		return nil
	} else if err != nil {
		return fmt.Errorf("finalize: %w", err)
	}

	log.Info("delivery finalized", "new_status", params.NewTaskStatus)
	return nil
}

func (s *Service) buildFinalizeParams(
	t *task.Task,
	attempt *task.Attempt,
	msg Message,
	result provider.Result,
	provErr error,
) task.FinalizeParams {
	params := task.FinalizeParams{
		TaskID:     t.ID,
		LeaseToken: *t.LeaseToken,
		AttemptID:  attempt.ID,
		CampaignID: t.CampaignID,
		PrevStatus: task.StatusSending,
	}

	if provErr == nil {
		// ── Success ──────────────────────────────────────────────────────────
		params.NewTaskStatus = task.StatusSucceeded
		params.NewAttemptStatus = task.AttemptSucceeded
		params.ProviderRequestID = &result.ProviderRequestID
		return params
	}

	var pErr *provider.Error
	if !errors.As(provErr, &pErr) {
		// Context cancelled or infrastructure error — treat as transient.
		pErr = &provider.Error{Type: provider.ErrorTypeTransient, Code: "internal", Message: provErr.Error()}
	}

	errType := string(pErr.Type)
	params.ErrorType = &errType
	params.ErrorCode = &pErr.Code
	params.ErrorMessage = &pErr.Message
	params.NewAttemptStatus = task.AttemptFailed
	if errors.Is(provErr, context.DeadlineExceeded) {
		params.NewAttemptStatus = task.AttemptTimedOut
	}

	if pErr.Type == provider.ErrorTypePermanent || !t.CanRetry() {
		// ── Dead letter ───────────────────────────────────────────────────────
		params.NewTaskStatus = task.StatusDeadLettered
		params.ChannelCode = t.ChannelCode
		return params
	}

	// ── Retry ─────────────────────────────────────────────────────────────────
	retryAt := time.Now().Add(retryDelay(t.AttemptCount))

	params.NewTaskStatus = task.StatusRetryScheduled
	params.RetryAvailableAt = &retryAt
	return params
}
