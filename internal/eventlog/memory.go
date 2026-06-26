package eventlog

import (
	"context"
	"sort"
	"sync"
)

// Memory is an in-memory Log for tests and single-process runtimes. It is safe
// for concurrent use. Durable adapters (e.g. SQLite) live outside this package
// and satisfy the same contract.
type Memory struct {
	mu      sync.RWMutex
	streams map[Scope][]Event
}

// NewMemory returns an empty in-memory log.
func NewMemory() *Memory {
	return &Memory{streams: make(map[Scope][]Event)}
}

// Append implements Log.
func (m *Memory) Append(_ context.Context, scope Scope, events ...Event) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing := m.streams[scope]
	head := uint64(len(existing))
	if len(events) == 0 {
		return head, nil
	}
	// Copy the payloads so callers cannot mutate stored events after the fact.
	appended := make([]Event, len(events))
	for i, ev := range events {
		head++
		ev.Seq = head
		ev.Data = append([]byte(nil), ev.Data...)
		appended[i] = ev
	}
	m.streams[scope] = append(existing, appended...)
	return head, nil
}

// Read implements Log.
func (m *Memory) Read(_ context.Context, scope Scope, after uint64) ([]Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stream := m.streams[scope]
	if after >= uint64(len(stream)) {
		return nil, nil
	}
	out := make([]Event, 0, uint64(len(stream))-after)
	for _, ev := range stream[after:] {
		ev.Data = append([]byte(nil), ev.Data...)
		out = append(out, ev)
	}
	return out, nil
}

// Streams implements Log.
func (m *Memory) Streams(_ context.Context, tenantID string) ([]Scope, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []Scope
	for scope, stream := range m.streams {
		if scope.TenantID == tenantID && len(stream) > 0 {
			out = append(out, scope)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ThreadID < out[j].ThreadID })
	return out, nil
}
