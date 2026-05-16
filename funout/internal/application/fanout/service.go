package fanout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/notifications/funout/internal/infrastructure/metrics"

	"github.com/notifications/funout/internal/domain/campaign"
	"github.com/notifications/funout/internal/domain/task"
)

// Service orchestrates the full fanout flow for one CampaignRegionRun.
// It depends only on domain interfaces — no pgx, no amqp.
type Service struct {
	runs         campaign.RunRepository
	campaigns    campaign.CampaignRepository
	users        campaign.UserRepository
	userChannels campaign.UserChannelRepository
	tasks        task.BatchRepository
	workerID     string
	batchSize    int
}

func NewService(
	runs campaign.RunRepository,
	campaigns campaign.CampaignRepository,
	users campaign.UserRepository,
	userChannels campaign.UserChannelRepository,
	tasks task.BatchRepository,
	workerID string,
	batchSize int,
) *Service {
	return &Service{
		runs:         runs,
		campaigns:    campaigns,
		users:        users,
		userChannels: userChannels,
		tasks:        tasks,
		workerID:     workerID,
		batchSize:    batchSize,
	}
}

// Execute runs the fanout pipeline for the given message.
// Returns nil on success or ErrAlreadyLocked if another worker owns the run.
func (s *Service) Execute(ctx context.Context, body []byte) error {
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return fmt.Errorf("parse fanout message: %w", err)
	}

	log := slog.With(
		"run_id", msg.CampaignRegionRunID,
		"campaign_id", msg.CampaignID,
		"worker_id", s.workerID,
	)

	start := time.Now()

	run, err := s.runs.AcquireLock(ctx, msg.CampaignRegionRunID, s.workerID)
	if errors.Is(err, campaign.ErrAlreadyLocked) {
		log.Info("fanout skipped: run already locked")
		metrics.RunsTotal.WithLabelValues("skipped").Inc()
		return campaign.ErrAlreadyLocked
	}
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}

	log.Info("fanout lock acquired", "attempt", run.FanoutAttemptCount)

	camp, err := s.campaigns.FindByID(ctx, run.CampaignID)
	if err != nil {
		return fmt.Errorf("load campaign: %w", err)
	}

	total, pipelineErr := s.runPipeline(ctx, run, camp)
	dur := time.Since(start).Seconds()

	if pipelineErr != nil {
		log.Error("fanout pipeline failed", "error", pipelineErr)
		metrics.RunsTotal.WithLabelValues("failed").Inc()
		metrics.RunDuration.WithLabelValues("failed").Observe(dur)
		if mfErr := s.runs.MarkFailed(ctx, run.ID, s.workerID); mfErr != nil {
			log.Warn("could not mark run as failed", "error", mfErr)
		}
		return fmt.Errorf("pipeline: %w", pipelineErr)
	}

	if err := s.runs.MarkCompleted(ctx, run.ID, s.workerID); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}

	metrics.RunsTotal.WithLabelValues("completed").Inc()
	metrics.RunDuration.WithLabelValues("completed").Observe(dur)
	metrics.TasksInserted.Add(float64(total))
	log.Info("fanout completed", "total_tasks_inserted", total, "duration_ms", int(dur*1000))
	return nil
}

// runPipeline executes the three-stage fanout pipeline.
// Defined in pipeline.go to keep this file focused on orchestration.
func (s *Service) runPipeline(ctx context.Context, run *campaign.Run, camp *campaign.Campaign) (int64, error) {
	return executePipeline(ctx, s, run, camp)
}

// buildTasks converts enriched user_channel rows into domain Task objects.
func buildTasks(run *campaign.Run, camp *campaign.Campaign, ucs []campaign.UserChannel) []*task.Task {
	tasks := make([]*task.Task, 0, len(ucs))
	for _, uc := range ucs {
		tasks = append(tasks, task.New(run, camp, uc))
	}
	return tasks
}

// zeroUUID is the keyset cursor start value (sorts before all real UUIDs).
var zeroUUID = uuid.Nil
