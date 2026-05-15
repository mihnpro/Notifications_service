package task

import "errors"

var (
	// ErrAlreadyLeased is returned when the task is already held by another worker.
	ErrAlreadyLeased = errors.New("task lease is already held by another worker")

	// ErrNotAvailable is returned when available_at is in the future (retry arrived early).
	ErrNotAvailable = errors.New("task is not yet available (retry guard)")

	// ErrLeaseExpired is returned during finalization when our lease_token no longer matches.
	// This means a recovery job reclaimed the task while we were calling the provider.
	ErrLeaseExpired = errors.New("lease token mismatch: task was reclaimed by recovery")

	// ErrNotFound is returned when the task does not exist in the DB.
	ErrNotFound = errors.New("task not found")
)
