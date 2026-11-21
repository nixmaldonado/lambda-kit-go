package idempotency

import (
	"bytes"
	"context"
	"sync"
	"time"
)

// MemoryStore is a [Store] that keeps records in process memory. It is meant
// for tests and local runs: records are lost when the process exits and are
// not shared between Lambda execution environments.
type MemoryStore struct {
	mu      sync.Mutex
	records map[string]Record
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: map[string]Record{}}
}

// Create implements [Store].
func (s *MemoryStore) Create(_ context.Context, r Record, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if old, ok := s.records[r.Key]; ok && old.live(now) {
		return &AlreadyExistsError{Record: clone(old)}
	}
	s.records[r.Key] = clone(r)

	return nil
}

// Update implements [Store].
func (s *MemoryStore) Update(_ context.Context, r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if old, ok := s.records[r.Key]; !ok || old.Token != r.Token {
		return ErrNotOwner
	}
	s.records[r.Key] = clone(r)

	return nil
}

// Delete implements [Store].
func (s *MemoryStore) Delete(_ context.Context, r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	old, ok := s.records[r.Key]
	if !ok {
		return nil
	}
	if old.Token != r.Token {
		return ErrNotOwner
	}
	delete(s.records, r.Key)

	return nil
}

func clone(r Record) Record {
	r.Response = bytes.Clone(r.Response)

	return r
}
