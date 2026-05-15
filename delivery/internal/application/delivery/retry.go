package delivery

import (
	"fmt"
	"time"
)

// retryBuckets maps attempt number to delay. Pure domain logic, no I/O.
var retryBuckets = []time.Duration{
	30 * time.Second,   // attempt 1
	1 * time.Minute,    // attempt 2
	5 * time.Minute,    // attempt 3
	15 * time.Minute,   // attempt 4+
}

// retryDelay returns the delay before the next attempt.
func retryDelay(attemptCount int) time.Duration {
	idx := attemptCount - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(retryBuckets) {
		idx = len(retryBuckets) - 1
	}
	return retryBuckets[idx]
}

// retryRoutingKey returns the RabbitMQ routing key for the appropriate retry bucket.
func retryRoutingKey(regionID, queueGroup string, attemptCount int) string {
	bucket := retryBucketName(attemptCount)
	return fmt.Sprintf("notification.%s.%s.retry.%s", regionID, queueGroup, bucket)
}

func retryBucketName(attemptCount int) string {
	switch {
	case attemptCount <= 1:
		return "30s"
	case attemptCount == 2:
		return "1m"
	case attemptCount == 3:
		return "5m"
	default:
		return "15m"
	}
}
