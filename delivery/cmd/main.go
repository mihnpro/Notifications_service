package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	appdelivery "github.com/notifications/delivery/internal/application/delivery"
	"github.com/notifications/delivery/internal/infrastructure/config"
	"github.com/notifications/delivery/internal/infrastructure/postgres"
	infraprovider "github.com/notifications/delivery/internal/infrastructure/provider"
	"github.com/notifications/delivery/internal/infrastructure/rabbitmq"
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

	queues := cfg.DeliveryQueues()

	// Wire up all layers.
	taskRepo := postgres.NewTaskRepository(pool, cfg.WorkerID)
	adapter := infraprovider.NewStubAdapter()
	svc := appdelivery.NewService(taskRepo, adapter, cfg.WorkerID)

	worker := rabbitmq.NewWorker(
		cfg.RabbitMQURL,
		cfg.RabbitMQVhost,
		queues,
		cfg.Concurrency,
		svc,
	)

	slog.Info("delivery worker starting",
		"worker_id", cfg.WorkerID,
		"queues", queues,
		"concurrency", cfg.Concurrency,
	)

	if err := worker.Run(ctx); err != nil {
		slog.Error("worker exited", "error", err)
		os.Exit(1)
	}

	slog.Info("delivery worker stopped")
}
