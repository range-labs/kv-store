package kvstore

import (
	"errors"
	"fmt"
)

var (
	// ErrMismatch is returned when a `SetIf` call conditions aren't met.
	ErrMismatch = fmt.Errorf("record mismatch")

	// ErrKeyNotFound is returned if a key is requested that doesn't exist.
	ErrKeyNotFound = errors.New("key not found")

	// ErrFailedToAcquireLock is returned when distributed lock is held by another process.
	ErrFailedToAcquireLock = errors.New("lock held by other process")
)
