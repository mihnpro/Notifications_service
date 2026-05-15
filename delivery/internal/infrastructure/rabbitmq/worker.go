package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/semaphore"

	"github.com/notifications/delivery/internal/domain/task"
)

const reconnectDelay = 5 * time.Second

// MessageHandler processes a single raw RabbitMQ delivery body.
// Implemented by application/delivery.Service.
type MessageHandler interface {
	Process(ctx context.Context, body []byte) error
}

// Worker consumes from one or more delivery queues and dispatches messages to handler.
type Worker struct {
	amqpURL     string
	vhost       string
	queues      []string // e.g. ["notification.eu.default.normal", ...]
	concurrency int64
	handler     MessageHandler
}

func NewWorker(amqpURL, vhost string, queues []string, concurrency int, handler MessageHandler) *Worker {
	return &Worker{
		amqpURL:     amqpURL,
		vhost:       vhost,
		queues:      queues,
		concurrency: int64(concurrency),
		handler:     handler,
	}
}

// Run starts the consume loop with automatic reconnection.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if err := w.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("delivery worker disconnected, reconnecting", "error", err, "delay", reconnectDelay)
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

	// prefetch = concurrency: never hold more unacked msgs than we can process.
	if err := ch.Qos(int(w.concurrency), 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	// Fan-in: merge all queue consumers into one channel.
	merged := make(chan amqp.Delivery, int(w.concurrency))
	for _, q := range w.queues {
		msgs, err := ch.Consume(q, "", false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("consume %s: %w", q, err)
		}
		go forwardDeliveries(ctx, msgs, merged)
	}

	slog.Info("delivery worker ready", "queues", w.queues)

	sem := semaphore.NewWeighted(w.concurrency)
	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case d, ok := <-merged:
			if !ok {
				return fmt.Errorf("merged deliveries channel closed")
			}
			if err := sem.Acquire(ctx, 1); err != nil {
				d.Nack(false, true) //nolint:errcheck
				return err
			}
			go w.handle(ctx, d, sem)

		case amqpErr := <-connClosed:
			return fmt.Errorf("connection closed: %v", amqpErr)

		case <-ctx.Done():
			slog.Info("delivery worker shutting down, draining in-flight deliveries")
			sem.Acquire(context.Background(), w.concurrency) //nolint:errcheck
			return nil
		}
	}
}

func (w *Worker) handle(ctx context.Context, d amqp.Delivery, sem *semaphore.Weighted) {
	defer sem.Release(1)

	err := w.handler.Process(ctx, d.Body)

	switch {
	case err == nil:
		d.Ack(false) //nolint:errcheck

	// These are "safe to discard" domain outcomes — no data loss risk.
	case errors.Is(err, task.ErrAlreadyLeased),
		errors.Is(err, task.ErrNotAvailable),
		errors.Is(err, task.ErrNotFound):
		d.Ack(false) //nolint:errcheck

	default:
		// Infrastructure error — nack without requeue.
		// Recovery jobs re-emit signals for stuck tasks.
		slog.Error("delivery failed, nacking", "error", err)
		d.Nack(false, false) //nolint:errcheck
	}
}

// forwardDeliveries copies from a single queue consumer to the merged channel.
func forwardDeliveries(ctx context.Context, src <-chan amqp.Delivery, dst chan<- amqp.Delivery) {
	for {
		select {
		case d, ok := <-src:
			if !ok {
				return
			}
			select {
			case dst <- d:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
