package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	RunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fanout_runs_total",
		Help: "Total fanout runs by outcome (completed, failed, skipped).",
	}, []string{"outcome"})

	RunDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fanout_run_duration_seconds",
		Help:    "Fanout pipeline wall-clock time.",
		Buckets: prometheus.DefBuckets,
	}, []string{"outcome"})

	TasksInserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fanout_tasks_inserted_total",
		Help: "Total delivery tasks inserted across all fanout runs.",
	})

	MessagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fanout_messages_total",
		Help: "RabbitMQ messages processed by the fanout worker.",
	}, []string{"outcome"}) // ack, nack, skipped
)

// Handler returns the Prometheus HTTP handler for /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
