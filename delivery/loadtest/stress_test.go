package loadtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	appdelivery "github.com/notifications/delivery/internal/application/delivery"
	"github.com/notifications/delivery/internal/domain/task"
	"github.com/notifications/delivery/internal/infrastructure/postgres"
)

// TestLeaseContention verifies the CAS guarantee: when N goroutines race to
// acquire the same task simultaneously, exactly one succeeds and all others
// receive ErrAlreadyLeased.
//
// Run: go test -run TestLeaseContention -v ./loadtest/
func TestLeaseContention(t *testing.T) {
	const workers = 32

	ctx := context.Background()
	fix := setupFixture(t)
	ids := seedTasks(t, ctx, fix, 1)
	taskID := ids[0]

	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerID := fmt.Sprintf("contention-worker-%d", i)
			repo := postgres.NewTaskRepository(dbPool, workerID)
			<-start
			_, _, errs[i] = repo.AcquireLease(ctx, taskID, workerID)
		}()
	}

	close(start)
	wg.Wait()

	var successes int
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, task.ErrAlreadyLeased):
			// expected: task taken by another goroutine
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}

	if successes != 1 {
		t.Errorf("expected exactly 1 successful lease acquisition, got %d", successes)
	}
}

// TestNoDuplicateDelivery simulates RabbitMQ at-least-once redelivery:
// each task ID is put into the work channel TWICE, but only one delivery
// should change the task status to 'succeeded'.
//
// Asserts: attempt_count == 1 and status == 'succeeded' for every task.
//
// Run: go test -run TestNoDuplicateDelivery -v ./loadtest/
func TestNoDuplicateDelivery(t *testing.T) {
	const (
		taskCount = 400
		workers   = 8
	)

	ctx := context.Background()
	fix := setupFixture(t)
	ids := seedTasks(t, ctx, fix, taskCount)

	// Put each task ID twice — simulates duplicate message delivery from RabbitMQ.
	taskCh := make(chan uuid.UUID, taskCount*2)
	for _, id := range ids {
		taskCh <- id
		taskCh <- id
	}
	close(taskCh)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerID := fmt.Sprintf("nodup-worker-%d", i)
			repo := postgres.NewTaskRepository(dbPool, workerID)
			svc := appdelivery.NewService(repo, &instantAdapter{}, workerID)

			for taskID := range taskCh {
				body := makeMessage(taskID, fix.CampaignID, fix.RunID, fix.ChannelCode)
				if err := svc.Process(ctx, body); err != nil &&
					!errors.Is(err, task.ErrAlreadyLeased) &&
					!errors.Is(err, task.ErrNotAvailable) {
					t.Errorf("worker %d: unexpected Process error: %v", i, err)
				}
			}
		}()
	}
	wg.Wait()

	rows, err := dbPool.Query(ctx,
		`SELECT id, status, attempt_count FROM delivery_tasks WHERE campaign_id = $1`,
		fix.CampaignID)
	if err != nil {
		t.Fatalf("query tasks: %v", err)
	}
	defer rows.Close()

	var total, succeeded, multiAttempt int
	for rows.Next() {
		var (
			id       uuid.UUID
			status   string
			attempts int
		)
		if err := rows.Scan(&id, &status, &attempts); err != nil {
			t.Fatal(err)
		}
		total++
		if status == "succeeded" {
			succeeded++
		}
		if attempts > 1 {
			multiAttempt++
			t.Errorf("task %s was processed %d times (expected 1)", id, attempts)
		}
	}

	t.Logf("total=%d  succeeded=%d  double-processed=%d", total, succeeded, multiAttempt)
	if succeeded != taskCount {
		t.Errorf("expected %d succeeded tasks, got %d", taskCount, succeeded)
	}
}

// TestThroughput measures end-to-end tasks/second across different worker
// counts. Uses t.Log for metrics — run with -v to see them.
// Each sub-test is independent (own fixture + task pool).
//
// Run: go test -run TestThroughput -v ./loadtest/
func TestThroughput(t *testing.T) {
	for _, workers := range []int{1, 4, 8, 16} {
		workers := workers
		t.Run(fmt.Sprintf("workers_%02d", workers), func(t *testing.T) {
			t.Parallel()

			const taskCount = 800

			ctx := context.Background()
			fix := setupFixture(t)
			ids := seedTasks(t, ctx, fix, taskCount)

			taskCh := make(chan uuid.UUID, taskCount)
			for _, id := range ids {
				taskCh <- id
			}
			close(taskCh)

			start := time.Now()

			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				i := i
				wg.Add(1)
				go func() {
					defer wg.Done()
					workerID := fmt.Sprintf("tp%d-worker-%d", workers, i)
					repo := postgres.NewTaskRepository(dbPool, workerID)
					svc := appdelivery.NewService(repo, &instantAdapter{}, workerID)

					for taskID := range taskCh {
						body := makeMessage(taskID, fix.CampaignID, fix.RunID, fix.ChannelCode)
						if err := svc.Process(ctx, body); err != nil &&
							!errors.Is(err, task.ErrAlreadyLeased) &&
							!errors.Is(err, task.ErrNotAvailable) {
							t.Errorf("process: %v", err)
						}
					}
				}()
			}
			wg.Wait()

			elapsed := time.Since(start)
			tps := float64(taskCount) / elapsed.Seconds()
			t.Logf("workers=%2d  tasks=%d  elapsed=%-10v  tps=%.0f",
				workers, taskCount, elapsed.Round(time.Millisecond), tps)

			var count int
			if err := dbPool.QueryRow(ctx,
				`SELECT COUNT(*) FROM delivery_tasks WHERE campaign_id = $1 AND status = 'succeeded'`,
				fix.CampaignID,
			).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != taskCount {
				t.Errorf("expected %d succeeded tasks, got %d", taskCount, count)
			}
		})
	}
}

// TestRetryPath_Throughput measures the retry code path under load:
// a transient error causes Finalize to write outbox_events + set
// status=retry_scheduled. Asserts all tasks end in retry_scheduled.
//
// Run: go test -run TestRetryPath_Throughput -v ./loadtest/
func TestRetryPath_Throughput(t *testing.T) {
	const (
		taskCount = 300
		workers   = 4
	)

	ctx := context.Background()
	fix := setupFixture(t)
	ids := seedTasks(t, ctx, fix, taskCount)

	taskCh := make(chan uuid.UUID, taskCount)
	for _, id := range ids {
		taskCh <- id
	}
	close(taskCh)

	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerID := fmt.Sprintf("retry-worker-%d", i)
			repo := postgres.NewTaskRepository(dbPool, workerID)
			svc := appdelivery.NewService(repo, &transientAdapter{}, workerID)

			for taskID := range taskCh {
				body := makeMessage(taskID, fix.CampaignID, fix.RunID, fix.ChannelCode)
				svc.Process(ctx, body) //nolint:errcheck
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	t.Logf("retry path: workers=%d  tasks=%d  elapsed=%v  tps=%.0f",
		workers, taskCount, elapsed.Round(time.Millisecond),
		float64(taskCount)/elapsed.Seconds())

	var count int
	if err := dbPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM delivery_tasks WHERE campaign_id = $1 AND status = 'retry_scheduled'`,
		fix.CampaignID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != taskCount {
		t.Errorf("expected %d retry_scheduled tasks, got %d", taskCount, count)
	}
}

// TestDLQ_Throughput measures the dead-letter path:
// a permanent error on a task with no remaining attempts → status=dead_lettered
// + dlq_items row.
//
// Run: go test -run TestDLQ_Throughput -v ./loadtest/
func TestDLQ_Throughput(t *testing.T) {
	const (
		taskCount = 200
		workers   = 4
	)

	ctx := context.Background()
	fix := setupFixture(t)
	ids := seedTasks(t, ctx, fix, taskCount)

	// Set max_attempts=1 so the first (and only) attempt triggers DLQ.
	if _, err := dbPool.Exec(ctx,
		`UPDATE delivery_tasks SET max_attempts = 1 WHERE campaign_id = $1`,
		fix.CampaignID,
	); err != nil {
		t.Fatalf("set max_attempts: %v", err)
	}

	taskCh := make(chan uuid.UUID, taskCount)
	for _, id := range ids {
		taskCh <- id
	}
	close(taskCh)

	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerID := fmt.Sprintf("dlq-worker-%d", i)
			repo := postgres.NewTaskRepository(dbPool, workerID)
			svc := appdelivery.NewService(repo, &permanentAdapter{}, workerID)

			for taskID := range taskCh {
				body := makeMessage(taskID, fix.CampaignID, fix.RunID, fix.ChannelCode)
				svc.Process(ctx, body) //nolint:errcheck
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	t.Logf("DLQ path: workers=%d  tasks=%d  elapsed=%v  tps=%.0f",
		workers, taskCount, elapsed.Round(time.Millisecond),
		float64(taskCount)/elapsed.Seconds())

	var dlqCount, taskStatusCount int
	dbPool.QueryRow(ctx, //nolint:errcheck
		`SELECT COUNT(*) FROM dlq_items WHERE campaign_id = $1`, fix.CampaignID,
	).Scan(&dlqCount)
	dbPool.QueryRow(ctx, //nolint:errcheck
		`SELECT COUNT(*) FROM delivery_tasks WHERE campaign_id = $1 AND status = 'dead_lettered'`,
		fix.CampaignID,
	).Scan(&taskStatusCount)

	t.Logf("dlq_items=%d  dead_lettered_tasks=%d", dlqCount, taskStatusCount)

	if taskStatusCount != taskCount {
		t.Errorf("expected %d dead_lettered tasks, got %d", taskCount, taskStatusCount)
	}
	if dlqCount != taskCount {
		t.Errorf("expected %d dlq_items, got %d", taskCount, dlqCount)
	}
}
