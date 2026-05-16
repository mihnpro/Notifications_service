package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	DatabaseURL   string
	RabbitMQURL   string
	RabbitMQVhost string
	WorkerID      string
	BatchSize     int
	Concurrency   int
}

func Load() (*Config, error) {
	cfg := &Config{
		WorkerID:      getEnv("WORKER_ID", "funout-1"),
		RabbitMQURL:   getEnv("RABBITMQ_URL", "amqp://notifications:notifications@localhost:5672"),
		RabbitMQVhost: getEnv("RABBITMQ_VHOST", "/notifications"),
		BatchSize:     getEnvInt("FANOUT_BATCH_SIZE", 1000),
		Concurrency:   getEnvInt("FANOUT_CONCURRENCY", 4),
	}

	cfg.DatabaseURL = fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		getEnv("POSTGRES_USER", "notifications"),
		getEnv("POSTGRES_PASSWORD", "notifications"),
		getEnv("POSTGRES_HOST", "localhost"),
		getEnv("POSTGRES_PORT", "5433"),
		getEnv("POSTGRES_DB", "notifications"),
	)

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
