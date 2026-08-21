package kvstore

import (
	"errors"
	"sync"
)

// ErrKeyNotFound is returned when a GET or DELETE targets a key that doesn't exist.
var ErrKeyNotFound = errors.New("key not found")

// Store is a simple thread-safe in-memory key-value store.
// In later phases, this becomes the "state machine" that the Raft log applies
// committed entries to. For now, it's just a protected map.
type Store struct {
	mu   sync.RWMutex      // protects data from concurrent access
	data map[string]string // the actual key-value pairs
}

// NewStore creates an empty, ready-to-use Store.
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Put inserts or overwrites the value for a key.
// Uses a write lock: only one Put/Delete can run at a time, and no Get
// can run while a Put/Delete is in progress.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves the value for a key.
// Uses a read lock: multiple Gets can run concurrently, but they all block
// if a Put/Delete is currently running.
func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return value, nil
}

// Delete removes a key. Returns ErrKeyNotFound if the key didn't exist.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return ErrKeyNotFound
	}
	delete(s.data, key)
	return nil
}