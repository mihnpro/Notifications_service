package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Task-level outcomes tracked by the RabbitMQ worker (queue label = queue name).
	MessagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "delivery_messages_total",
		Help: "RabbitMQ messages processed by the delivery worker.",
	}, []string{"queue", "outcome"}) // outcome: ack, nack, skipped

	// Business-level delivery outcomes from the application service.
	TasksFinalized = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "delivery_tasks_finalized_total",
		Help: "Delivery tasks finalized by outcome.",
	}, []string{"channel", "outcome"}) // outcome: succeeded, dead_lettered, retry_scheduled

	TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "delivery_task_duration_seconds",
		Help:    "End-to-end delivery task processing time (lease → finalize).",
		Buckets: prometheus.DefBuckets,
	}, []string{"channel"})

	// Provider call metrics.
	ProviderCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "delivery_provider_calls_total",
		Help: "Outbound provider calls by channel and outcome.",
	}, []string{"channel", "outcome"}) // outcome: success, transient, permanent, timeout

	ProviderCallDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "delivery_provider_call_duration_seconds",
		Help:    "Provider HTTP call latency.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"channel"})
)

// Handler returns the Prometheus HTTP handler for /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
