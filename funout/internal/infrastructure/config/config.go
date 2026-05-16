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
	Region        string
}

// FanoutQueues returns the list of RabbitMQ queue names this worker should consume.
// Matches the routing keys written by the API: notification.{region}.fanout.{priority}.
func (c *Config) FanoutQueues() []string {
	priorities := []string{"high", "normal", "low"}
	queues := make([]string, 0, len(priorities))
	for _, p := range priorities {
		queues = append(queues, fmt.Sprintf("notification.%s.fanout.%s", c.Region, p))
	}
	return queues
}

func Load() (*Config, error) {
	cfg := &Config{
		WorkerID:      getEnv("WORKER_ID", "funout-1"),
		RabbitMQURL:   getEnv("RABBITMQ_URL", "amqp://notifications:notifications@localhost:5672"),
		RabbitMQVhost: getEnv("RABBITMQ_VHOST", "/notifications"),
		BatchSize:     getEnvInt("FANOUT_BATCH_SIZE", 1000),
		Concurrency:   getEnvInt("FANOUT_CONCURRENCY", 4),
		Region:        getEnv("REGION", "default"),
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
