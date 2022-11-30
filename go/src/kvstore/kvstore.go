package kvstore

import (
	"time"
)

// Store is an interface for getting/setting key/value configs. The intent is
// the implementation can be switched out as performance characteristics
// change.
type Store interface {
	// Sets a config value.
	Set(key string, value interface{}) error

	// Sets a config value, and schedules it to expiire after a certain duration.
	SetX(key string, value interface{}, maxAge time.Duration) error

	// Sets a config value if, and only if, the current value matches the expected
	// value. dberror.ErrMismatch is returned if condition isn't met.
	SetIf(key string, value interface{}, condition interface{}) error

	// Sets a config value, if and only if, the key does not currently exist.
	// dberror.ErrMismatch is returned if the condition isn't met.
	SetIfNotExists(key string, value interface{}) error

	// Remove a key/value pair
	Delete(key string) error

	// Get fetches a configuration's value and stores it in the value pointed to
	// by v. If an item isn't found dberror.ErrKeyNotFound is returned.
	Get(key string, value interface{}) error

	// GetString returns a configuration's value as a string. If there isn't a
	// stored value, empty string is returned instead of dberror.ErrKeyNotFound.
	GetString(key string) (string, error)

	// GetTime returns a datetime that had been stored in RFC3339 format. If there
	// isn't a stored value, zero time is returned instead of dberror.ErrKeyNotFound.
	GetTime(key string) (time.Time, error)

	// GetInt32 returns a configuration's value as an int32. If there
	// isn't a stored value, zero is returned instead of dberror.ErrKeyNotFound.
	GetInt32(key string) (int32, error)

	// GetInt64 returns a configuration's value as an int64. If there
	// isn't a stored value, zero is returned instead of dberror.ErrKeyNotFound.
	GetInt64(key string) (int64, error)
}

// Helper is intended to be composed by implementations of Store where
// GetXxx are implemented in terms of a single Getter.
type Helper struct {
	Getter func(key string, v interface{}) error
}

func (s *Helper) GetString(key string) (string, error) {
	var v string
	err := s.Getter(key, &v)
	if err == ErrKeyNotFound {
		return "", nil
	}
	return v, err
}

func (s *Helper) GetTime(key string) (time.Time, error) {
	var t time.Time
	err := s.Getter(key, &t)
	if err == ErrKeyNotFound {
		return time.Time{}, nil
	} else if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

func (s *Helper) GetInt32(key string) (int32, error) {
	var i int32
	err := s.Getter(key, &i)
	if err == ErrKeyNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return i, nil
}

func (s *Helper) GetInt64(key string) (int64, error) {
	var i int64
	err := s.Getter(key, &i)
	if err == ErrKeyNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return i, nil
}
