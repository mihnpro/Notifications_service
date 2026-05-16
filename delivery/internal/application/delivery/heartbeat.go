package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/notifications/delivery/internal/domain/task"
)

const (
	heartbeatInterval = 30 * time.Second
	providerTimeout   = 320 * time.Second
)

// runHeartbeat extends the task lease every 30 s until ctx is cancelled.
// It stops silently if the lease is lost (recovery reclaimed the task).
// Must be run in a separate goroutine; cancel hbCtx to stop it after provider returns.
func runHeartbeat(ctx context.Context, repo task.Repository, taskID, leaseToken uuid.UUID) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := repo.ExtendLease(ctx, taskID, leaseToken); err != nil {
				slog.Warn("heartbeat: lease lost or context cancelled",
					"task_id", taskID,
					"error", err,
				)
				return
			}
			slog.Debug("heartbeat: lease extended", "task_id", taskID)

		case <-ctx.Done():
			return
		}
	}
}
