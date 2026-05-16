package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	appfanout "github.com/notifications/funout/internal/application/fanout"
	"github.com/notifications/funout/internal/infrastructure/config"
	"github.com/notifications/funout/internal/infrastructure/postgres"
	"github.com/notifications/funout/internal/infrastructure/rabbitmq"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("create db pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		slog.Error("db ping", "error", err)
		os.Exit(1)
	}

	// Wire up all layers.
	svc := appfanout.NewService(
		postgres.NewRunRepository(pool),
		postgres.NewCampaignRepository(pool),
		postgres.NewUserRepository(pool),
		postgres.NewUserChannelRepository(pool),
		postgres.NewTaskBatchRepository(pool),
		cfg.WorkerID,
		cfg.BatchSize,
	)

	worker := rabbitmq.NewWorker(
		cfg.RabbitMQURL,
		cfg.RabbitMQVhost,
		cfg.Concurrency,
		svc,
	)

	slog.Info("fanout worker starting",
		"worker_id", cfg.WorkerID,
		"batch_size", cfg.BatchSize,
		"concurrency", cfg.Concurrency,
	)

	if err := worker.Run(ctx); err != nil {
		slog.Error("worker exited", "error", err)
		os.Exit(1)
	}

	slog.Info("fanout worker stopped")
}
