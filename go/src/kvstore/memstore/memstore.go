package memstore

import (
	"kvstore/kvstore"
	"reflect"
	"time"
)

// NewMemoryStore returns a config.Store backed by a map in memory.
func NewMemoryStore() kvstore.Store {
	return &memstore{
		Map:         make(map[string]interface{}),
		Expirations: make(map[string]time.Time),
	}
}

type memstore struct {
	Map         map[string]interface{}
	Expirations map[string]time.Time
}

func (m *memstore) Set(key string, value interface{}) error {
	m.Map[key] = value
	return nil
}

func (m *memstore) SetX(key string, value interface{}, expiration time.Duration) error {
	m.Map[key] = value
	m.Expirations[key] = time.Now().Add(expiration)
	return nil
}

func (m *memstore) SetIf(key string, value interface{}, condition interface{}) error {
	if !reflect.DeepEqual(m.Map[key], condition) {
		return kvstore.ErrMismatch
	}
	m.Map[key] = value
	return nil
}

func (m *memstore) SetIfNotExists(key string, value interface{}) error {
	if _, exists := m.Map[key]; exists {
		return kvstore.ErrMismatch
	}
	m.Map[key] = value
	return nil
}

func (m *memstore) Delete(key string) error {
	delete(m.Map, key)
	delete(m.Expirations, key)
	return nil
}

func (m *memstore) Get(key string, v interface{}) error {
	value, exists := m.Map[key]
	if !exists {
		return kvstore.ErrKeyNotFound
	}
	expiration, hasExpiry := m.Expirations[key]
	if hasExpiry && expiration.Before(time.Now()) {
		m.Delete(key)
		return kvstore.ErrKeyNotFound
	}
	switch value.(type) {
	case string:
		*v.(*string) = value.(string)
	case time.Time:
		*v.(*time.Time) = value.(time.Time)
	case int32:
		*v.(*int32) = value.(int32)
	case int64:
		*v.(*int64) = value.(int64)
	default:
		pVal := reflect.ValueOf(v)
		vVal := reflect.ValueOf(value)
		if pVal.Kind() == reflect.Ptr && pVal.Elem().Type() == vVal.Type() {
			// If v is a pointer to a value and the types match, assign value to the pointer address.
			pVal.Elem().Set(vVal)
		} else {
			// Fall back to treating the pointer as a pointer to a generic interface.
			*v.(*interface{}) = value
		}
	}
	return nil
}

func (m *memstore) GetString(key string) (string, error) {
	var s string
	err := m.Get(key, &s)
	if err == kvstore.ErrKeyNotFound {
		return "", nil
	}
	return s, err
}

func (m *memstore) GetTime(key string) (time.Time, error) {
	var t time.Time
	err := m.Get(key, &t)
	if err == kvstore.ErrKeyNotFound {
		return time.Time{}, nil
	}
	return t, err
}

func (m *memstore) GetInt32(key string) (int32, error) {
	var i int32
	err := m.Get(key, &i)
	if err == kvstore.ErrKeyNotFound {
		return 0, nil
	}
	return i, err
}

func (m *memstore) GetInt64(key string) (int64, error) {
	var i int64
	err := m.Get(key, &i)
	if err == kvstore.ErrKeyNotFound {
		return 0, nil
	}
	return i, err
}
