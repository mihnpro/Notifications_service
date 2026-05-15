package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DatabaseURL   string
	RabbitMQURL   string
	RabbitMQVhost string
	WorkerID      string
	Concurrency   int
	Region        string
	QueueGroups   []string
}

// DeliveryQueues derives the RabbitMQ queue names from region + queue_groups.
func (c *Config) DeliveryQueues() []string {
	priorities := []string{"high", "normal"}
	queues := make([]string, 0, len(c.QueueGroups)*len(priorities))
	for _, g := range c.QueueGroups {
		for _, p := range priorities {
			queues = append(queues, fmt.Sprintf("notification.%s.%s.%s", c.Region, g, p))
		}
	}
	return queues
}

func Load() (*Config, error) {
	cfg := &Config{
		WorkerID:      getEnv("WORKER_ID", "delivery-1"),
		RabbitMQURL:   getEnv("RABBITMQ_URL", "amqp://notifications:notifications@localhost:5672"),
		RabbitMQVhost: getEnv("RABBITMQ_VHOST", "/notifications"),
		Concurrency:   getEnvInt("DELIVERY_CONCURRENCY", 16),
		Region:        getEnv("REGION", "eu"),
		QueueGroups:   strings.Split(getEnv("QUEUE_GROUPS", "default,messenger"), ","),
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
