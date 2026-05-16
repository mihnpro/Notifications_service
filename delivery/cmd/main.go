package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	appdelivery "github.com/notifications/delivery/internal/application/delivery"
	"github.com/notifications/delivery/internal/domain/provider"
	"github.com/notifications/delivery/internal/infrastructure/config"
	"github.com/notifications/delivery/internal/infrastructure/metrics"
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

	var adapter provider.Adapter
	if cfg.ProviderURL != "" {
		adapter = infraprovider.NewHTTPAdapter(cfg.ProviderURL)
		slog.Info("using HTTP provider adapter", "url", cfg.ProviderURL)
	} else {
		adapter = infraprovider.NewStubAdapter()
		slog.Info("using stub provider adapter")
	}

	svc := appdelivery.NewService(taskRepo, adapter, cfg.WorkerID)

	worker := rabbitmq.NewWorker(
		cfg.RabbitMQURL,
		cfg.RabbitMQVhost,
		queues,
		cfg.Concurrency,
		svc,
	)

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9093"
	}
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metrics.Handler()}
	go func() {
		slog.Info("metrics server listening", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "error", err)
		}
	}()
	defer metricsSrv.Close()

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
