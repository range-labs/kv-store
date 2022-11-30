package kvstore

import (
	"errors"
	"time"
)

// ClearLock is the value used to clear the lock, since time.Time{} is considered a zero value
// in the database layer and makes partial updates/queries difficult.
var ClearLock = time.Unix(0, 0)

// AcquireExpiringLock acquires a lock and sets it to expire in expiry time.
func AcquireExpiringLock(store Store, lock string, expiry time.Duration) (func() error, error) {
	originalLock, err := store.GetTime(lock)
	if err != nil {
		return nil, errors.New("failed to query lock")
	} else if time.Since(originalLock) < expiry {
		return nil, ErrFailedToAcquireLock
	}

	now := time.Now()
	if originalLock.IsZero() {
		if err := store.SetIfNotExists(lock, now); err != nil {
			return nil, errors.New("failed to set initial lock")
		}
	} else {
		if err := store.SetIf(lock, now, originalLock); err != nil {
			if err == ErrMismatch {
				return nil, ErrFailedToAcquireLock
			}
			return nil, errors.New("failed to set new lock")
		}
	}

	clear := func() error {
		if err := store.SetIf(lock, ClearLock, now); err != nil {
			if err == ErrMismatch {
				// Another process acquired the lock after it expired, but before clear was called.
				return nil
			}
			return errors.New("failed to clear lock")
		}
		return nil
	}
	return clear, nil
}
