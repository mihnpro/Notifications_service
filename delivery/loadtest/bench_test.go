package loadtest

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/notifications/delivery/internal/domain/provider"
	"github.com/notifications/delivery/internal/domain/task"
	"github.com/notifications/delivery/internal/infrastructure/postgres"
)

// ── Stub adapters ─────────────────────────────────────────────────────────────

// instantAdapter returns success with zero latency — isolates DB performance.
type instantAdapter struct{}

func (a *instantAdapter) Send(_ context.Context, p provider.Payload) (provider.Result, error) {
	return provider.Result{ProviderRequestID: "lt-" + p.TaskID.String()}, nil
}

// transientAdapter always fails transiently — exercises the retry code path.
type transientAdapter struct{}

func (a *transientAdapter) Send(_ context.Context, _ provider.Payload) (provider.Result, error) {
	return provider.Result{}, &provider.Error{
		Type:    provider.ErrorTypeTransient,
		Code:    "LT_TRANSIENT",
		Message: "load-test: simulated transient failure",
	}
}

// permanentAdapter always fails permanently — exercises the DLQ code path.
type permanentAdapter struct{}

func (a *permanentAdapter) Send(_ context.Context, _ provider.Payload) (provider.Result, error) {
	return provider.Result{}, &provider.Error{
		Type:    provider.ErrorTypePermanent,
		Code:    "LT_PERMANENT",
		Message: "load-test: simulated permanent failure",
	}
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

// BenchmarkAcquireLease measures the throughput of AcquireLease in a single
// goroutine. Represents the minimum per-task cost of the lease transaction.
//
// Run: go test -bench=BenchmarkAcquireLease -benchtime=10s ./loadtest/
func BenchmarkAcquireLease(b *testing.B) {
	ctx := context.Background()
	fix := setupFixture(b)
	ids := seedTasks(b, ctx, fix, b.N)
	repo := postgres.NewTaskRepository(dbPool, "bench-acquire")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, _, err := repo.AcquireLease(ctx, ids[i], "bench-acquire"); err != nil {
			b.Fatalf("iter %d: %v", i, err)
		}
	}
}

// BenchmarkFinalizeSuccess measures Finalize throughput on the success path.
// Leases are acquired in the setup phase so they don't inflate the measurement.
//
// Run: go test -bench=BenchmarkFinalizeSuccess -benchtime=10s ./loadtest/
func BenchmarkFinalizeSuccess(b *testing.B) {
	ctx := context.Background()
	fix := setupFixture(b)
	ids := seedTasks(b, ctx, fix, b.N)
	repo := postgres.NewTaskRepository(dbPool, "bench-finalize")

	type leased struct {
		t       *task.Task
		attempt *task.Attempt
	}
	leases := make([]leased, b.N)
	for i, id := range ids {
		t, att, err := repo.AcquireLease(ctx, id, "bench-finalize")
		if err != nil {
			b.Fatalf("pre-acquire iter %d: %v", i, err)
		}
		leases[i] = leased{t, att}
	}

	reqID := "lt-ok"
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		l := leases[i]
		params := task.FinalizeParams{
			TaskID:            l.t.ID,
			LeaseToken:        *l.t.LeaseToken,
			AttemptID:         l.attempt.ID,
			CampaignID:        l.t.CampaignID,
			NewTaskStatus:     task.StatusSucceeded,
			NewAttemptStatus:  task.AttemptSucceeded,
			PrevStatus:        task.StatusSending,
			ProviderRequestID: &reqID,
		}
		if err := repo.Finalize(ctx, params); err != nil {
			b.Fatalf("iter %d: %v", i, err)
		}
	}
}

// BenchmarkFullCycle_Sequential measures the complete AcquireLease→Finalize
// round-trip in a single goroutine.
//
// Run: go test -bench=BenchmarkFullCycle_Sequential -benchtime=10s ./loadtest/
func BenchmarkFullCycle_Sequential(b *testing.B) {
	ctx := context.Background()
	fix := setupFixture(b)
	ids := seedTasks(b, ctx, fix, b.N)
	repo := postgres.NewTaskRepository(dbPool, "bench-cycle-seq")

	reqID := "lt-ok"
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		t, attempt, err := repo.AcquireLease(ctx, ids[i], "bench-cycle-seq")
		if err != nil {
			b.Fatalf("acquire iter %d: %v", i, err)
		}
		params := task.FinalizeParams{
			TaskID:            t.ID,
			LeaseToken:        *t.LeaseToken,
			AttemptID:         attempt.ID,
			CampaignID:        t.CampaignID,
			NewTaskStatus:     task.StatusSucceeded,
			NewAttemptStatus:  task.AttemptSucceeded,
			PrevStatus:        task.StatusSending,
			ProviderRequestID: &reqID,
		}
		if err := repo.Finalize(ctx, params); err != nil {
			b.Fatalf("finalize iter %d: %v", i, err)
		}
	}
}

// BenchmarkFullCycle_Parallel runs the AcquireLease→Finalize cycle with
// GOMAXPROCS goroutines. Shows contention behaviour on the DB connection pool.
//
// Run: go test -bench=BenchmarkFullCycle_Parallel -benchtime=10s -cpu=1,4,8,16 ./loadtest/
func BenchmarkFullCycle_Parallel(b *testing.B) {
	ctx := context.Background()
	fix := setupFixture(b)
	ids := seedTasks(b, ctx, fix, b.N)

	var idx atomic.Int64
	reqID := "lt-ok"
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		repo := postgres.NewTaskRepository(dbPool, "bench-cycle-par")
		for pb.Next() {
			i := int(idx.Add(1)) - 1
			t, attempt, err := repo.AcquireLease(ctx, ids[i], "bench-cycle-par")
			if err != nil {
				b.Error(err)
				return
			}
			params := task.FinalizeParams{
				TaskID:            t.ID,
				LeaseToken:        *t.LeaseToken,
				AttemptID:         attempt.ID,
				CampaignID:        t.CampaignID,
				NewTaskStatus:     task.StatusSucceeded,
				NewAttemptStatus:  task.AttemptSucceeded,
				PrevStatus:        task.StatusSending,
				ProviderRequestID: &reqID,
			}
			if err := repo.Finalize(ctx, params); err != nil {
				b.Error(err)
			}
		}
	})
}

// BenchmarkExtendLease measures heartbeat throughput — how fast the worker
// can keep leases alive under high concurrency.
//
// Run: go test -bench=BenchmarkExtendLease -benchtime=10s ./loadtest/
func BenchmarkExtendLease(b *testing.B) {
	ctx := context.Background()
	fix := setupFixture(b)
	ids := seedTasks(b, ctx, fix, b.N)
	repo := postgres.NewTaskRepository(dbPool, "bench-heartbeat")

	// Pre-acquire all leases so the benchmark measures only ExtendLease.
	leased := make([]task.Task, b.N)
	for i, id := range ids {
		t, _, err := repo.AcquireLease(ctx, id, "bench-heartbeat")
		if err != nil {
			b.Fatalf("pre-acquire iter %d: %v", i, err)
		}
		leased[i] = *t
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := repo.ExtendLease(ctx, leased[i].ID, *leased[i].LeaseToken); err != nil {
			b.Fatalf("iter %d: %v", i, err)
		}
	}
}

// BenchmarkFinalizeRetry measures Finalize on the retry path
// (StatusRetryScheduled — updates task status + stats; recover handles re-enqueue).
//
// Run: go test -bench=BenchmarkFinalizeRetry -benchtime=10s ./loadtest/
func BenchmarkFinalizeRetry(b *testing.B) {
	ctx := context.Background()
	fix := setupFixture(b)
	ids := seedTasks(b, ctx, fix, b.N)
	repo := postgres.NewTaskRepository(dbPool, "bench-retry")

	type leased struct {
		t       *task.Task
		attempt *task.Attempt
	}
	leases := make([]leased, b.N)
	for i, id := range ids {
		t, att, err := repo.AcquireLease(ctx, id, "bench-retry")
		if err != nil {
			b.Fatalf("pre-acquire iter %d: %v", i, err)
		}
		leases[i] = leased{t, att}
	}

	errType := "transient"
	errCode := "LT_TRANSIENT"
	errMsg := "bench retry"
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		l := leases[i]
		retryAt := time.Now().Add(30 * time.Second)
		params := task.FinalizeParams{
			TaskID:           l.t.ID,
			LeaseToken:       *l.t.LeaseToken,
			AttemptID:        l.attempt.ID,
			CampaignID:       l.t.CampaignID,
			NewTaskStatus:    task.StatusRetryScheduled,
			NewAttemptStatus: task.AttemptFailed,
			PrevStatus:       task.StatusSending,
			ErrorType:        &errType,
			ErrorCode:        &errCode,
			ErrorMessage:     &errMsg,
			RetryAvailableAt: &retryAt,
		}
		if err := repo.Finalize(ctx, params); err != nil {
			b.Fatalf("iter %d: %v", i, err)
		}
	}
}
