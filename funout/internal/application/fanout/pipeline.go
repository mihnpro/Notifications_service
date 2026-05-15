package fanout

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/notifications/funout/internal/domain/campaign"
	"github.com/notifications/funout/internal/domain/task"
)

// executePipeline runs a 3-stage concurrent pipeline and returns total inserted tasks.
//
// Stage 1 — cursor:  reads user IDs from DB in keyset-paginated batches → userBatchCh
// Stage 2 — enrich:  joins user_channels for each batch of users        → taskBatchCh
// Stage 3 — insert:  bulk-inserts delivery_tasks + outbox_events        → totalInserted
//
// Buffered channels (size 2) let stages overlap: while Stage 3 is inserting batch N,
// Stage 2 enriches batch N+1, and Stage 1 fetches batch N+2 from the DB.
func executePipeline(ctx context.Context, svc *Service, run *campaign.Run, camp *campaign.Campaign) (int64, error) {
	userBatchCh := make(chan []uuid.UUID, 2)
	taskBatchCh := make(chan []*task.Task, 2)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(userBatchCh)
		return stageCursor(gCtx, svc, run, camp, userBatchCh)
	})

	g.Go(func() error {
		defer close(taskBatchCh)
		return stageEnrich(gCtx, svc, run, camp, userBatchCh, taskBatchCh)
	})

	var total int64
	g.Go(func() error {
		return stageInsert(gCtx, svc, run, taskBatchCh, &total)
	})

	if err := g.Wait(); err != nil {
		return 0, err
	}
	return total, nil
}

// stageCursor pages through target users and sends ID batches downstream.
func stageCursor(
	ctx context.Context,
	svc *Service,
	run *campaign.Run,
	camp *campaign.Campaign,
	out chan<- []uuid.UUID,
) error {
	lastID := zeroUUID

	for {
		ids, err := svc.users.FetchBatch(ctx, run.RegionID, camp.Selector, lastID, svc.batchSize)
		if err != nil {
			return fmt.Errorf("cursor fetch: %w", err)
		}
		if len(ids) == 0 {
			return nil
		}

		select {
		case out <- ids:
		case <-ctx.Done():
			return ctx.Err()
		}

		lastID = ids[len(ids)-1]
		if len(ids) < svc.batchSize {
			return nil // last partial batch
		}
	}
}

// stageEnrich receives user ID batches, loads their active channels, and emits task batches.
func stageEnrich(
	ctx context.Context,
	svc *Service,
	run *campaign.Run,
	camp *campaign.Campaign,
	in <-chan []uuid.UUID,
	out chan<- []*task.Task,
) error {
	for {
		select {
		case ids, ok := <-in:
			if !ok {
				return nil
			}

			ucs, err := svc.userChannels.FindActiveByUsers(ctx, ids, camp.ChannelCodes)
			if err != nil {
				return fmt.Errorf("enrich user channels: %w", err)
			}
			if len(ucs) == 0 {
				continue
			}

			tasks := buildTasks(run, camp, ucs)

			select {
			case out <- tasks:
			case <-ctx.Done():
				return ctx.Err()
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// stageInsert bulk-inserts each task batch and extends the fanout lock afterwards.
func stageInsert(
	ctx context.Context,
	svc *Service,
	run *campaign.Run,
	in <-chan []*task.Task,
	total *int64,
) error {
	for {
		select {
		case tasks, ok := <-in:
			if !ok {
				return nil
			}

			n, err := svc.tasks.InsertBatch(ctx, tasks)
			if err != nil {
				return fmt.Errorf("insert batch: %w", err)
			}
			*total += n

			slog.Info("fanout batch committed",
				"run_id", run.ID,
				"batch_tasks", len(tasks),
				"newly_inserted", n,
				"total_so_far", *total,
			)

			if err := svc.runs.ExtendLock(ctx, run.ID, svc.workerID); err != nil {
				return fmt.Errorf("lock lost mid-pipeline: %w", err)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
