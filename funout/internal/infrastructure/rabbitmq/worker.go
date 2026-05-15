package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/semaphore"

	"github.com/notifications/funout/internal/domain/campaign"
)

const (
	fanoutQueue    = "notification.fanout"
	prefetchCount  = 8
	reconnectDelay = 5 * time.Second
)

// MessageHandler processes a single raw RabbitMQ message body.
// Implemented by application/fanout.Service.
type MessageHandler interface {
	Execute(ctx context.Context, body []byte) error
}

// Worker manages the RabbitMQ connection and dispatches messages to the handler.
type Worker struct {
	amqpURL     string
	vhost       string
	concurrency int64
	handler     MessageHandler
}

func NewWorker(amqpURL, vhost string, concurrency int, handler MessageHandler) *Worker {
	return &Worker{
		amqpURL:     amqpURL,
		vhost:       vhost,
		concurrency: int64(concurrency),
		handler:     handler,
	}
}

// Run starts the consume loop with automatic reconnection.
// Blocks until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if err := w.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("worker connection lost, reconnecting", "error", err, "delay", reconnectDelay)
		}

		select {
		case <-time.After(reconnectDelay):
		case <-ctx.Done():
			return nil
		}
	}
}

func (w *Worker) runOnce(ctx context.Context) error {
	conn, err := amqp.DialConfig(w.amqpURL, amqp.Config{Vhost: w.vhost})
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	msgs, err := ch.Consume(fanoutQueue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	slog.Info("fanout worker ready", "queue", fanoutQueue)

	sem := semaphore.NewWeighted(w.concurrency)
	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("deliveries channel closed")
			}
			if err := sem.Acquire(ctx, 1); err != nil {
				d.Nack(false, true) //nolint:errcheck
				return err
			}
			go w.handle(ctx, d, sem)

		case amqpErr := <-connClosed:
			return fmt.Errorf("connection closed: %v", amqpErr)

		case <-ctx.Done():
			slog.Info("draining in-flight fanout pipelines")
			// Acquire all slots — blocks until every goroutine finishes.
			sem.Acquire(context.Background(), w.concurrency) //nolint:errcheck
			return nil
		}
	}
}

func (w *Worker) handle(ctx context.Context, d amqp.Delivery, sem *semaphore.Weighted) {
	defer sem.Release(1)

	err := w.handler.Execute(ctx, d.Body)

	switch {
	case err == nil:
		d.Ack(false) //nolint:errcheck

	case errors.Is(err, campaign.ErrAlreadyLocked):
		// Duplicate signal — safe to ack; another worker owns this run.
		d.Ack(false) //nolint:errcheck

	default:
		slog.Error("fanout failed, nacking", "error", err)
		// requeue=false: rely on outbox recovery to re-emit the signal.
		d.Nack(false, false) //nolint:errcheck
	}
}
