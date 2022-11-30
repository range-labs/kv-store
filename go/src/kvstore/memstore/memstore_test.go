package memstore

import (
	"testing"
	"time"
)

func TestStrings(t *testing.T) {
	s := NewMemoryStore()
	s.Set("key", "value")
	v, err := s.GetString("key")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if v != "value" {
		t.Errorf("Unexpected value for 'v': %s", v)
	}
	v2, err := s.GetString("unknown")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if v2 != "" {
		t.Errorf("Unexpected value for 'v2': %s", v)
	}
}

func TestTime(t *testing.T) {
	s := NewMemoryStore()
	z := time.Now()
	s.Set("key", z)
	z2, err := s.GetTime("key")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if z != z2 {
		t.Errorf("Unexpected value for 'z2', wanted %v was %v", z, z2)
	}
	z3, err := s.GetTime("unknown")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if !z3.IsZero() {
		t.Errorf("Unexpected value for 'z3': %v", z3)
	}
}

func TestSetX(t *testing.T) {
	s := NewMemoryStore()
	s.SetX("key", "value", time.Millisecond*100)

	v, err := s.GetString("key")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if v != "value" {
		t.Errorf("Unexpected value for 'v': %s", v)
	}

	time.Sleep(time.Millisecond * 150)

	v2, err := s.GetString("key")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if v2 != "" {
		t.Errorf("Unexpected value to have expired, got: %s", v)
	}
}
