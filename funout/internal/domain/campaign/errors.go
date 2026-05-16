package campaign

import "errors"

var (
	ErrAlreadyLocked = errors.New("campaign_region_run is already locked by another worker")
	ErrLockLost      = errors.New("fanout lock was lost (expired or stolen by recovery)")
	ErrNotFound      = errors.New("campaign not found")
)
