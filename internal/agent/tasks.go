package agent

import (
	"context"
	"crypto/hmac"
	"sort"
	"sync"
	"time"

	"aurora-capcompute/internal/eventlog"
	"aurora-capcompute/internal/task"
)

// eventTaskStore implements task.Store over the append-only event log. It keeps
// an in-memory projection (seeded from the log on restore, updated on each op)
// and appends a task.created / task.resolved / task.executed event for every
// mutation, so durable task state lives only in the log.
type eventTaskStore struct {
	log eventlog.Log
	now func() time.Time

	mu      sync.Mutex
	records map[string]task.Record // keyed by tenant/taskID
}

func newEventTaskStore(log eventlog.Log, now func() time.Time) *eventTaskStore {
	return &eventTaskStore{log: log, now: now, records: make(map[string]task.Record)}
}

func taskKey(tenantID, taskID string) string { return tenantID + "/" + taskID }

// seed installs records folded from the log during restore.
func (s *eventTaskStore) seed(records []task.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range records {
		s.records[taskKey(record.Scope.TenantID, record.ID)] = cloneTaskRecord(record)
	}
}

func (s *eventTaskStore) scope(record task.Record) eventlog.Scope {
	return eventlog.Scope{TenantID: record.Scope.TenantID, ThreadID: record.Scope.ThreadID}
}

func (s *eventTaskStore) Find(_ context.Context, scope task.Scope, position int, callHash string) (task.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range s.records {
		if record.Scope == scope && record.JournalPosition == position && record.CallHash == callHash {
			return cloneTaskRecord(record), true, nil
		}
	}
	return task.Record{}, false, nil
}

func (s *eventTaskStore) Create(ctx context.Context, record task.Record) error {
	s.mu.Lock()
	key := taskKey(record.Scope.TenantID, record.ID)
	if _, exists := s.records[key]; exists {
		s.mu.Unlock()
		return task.ErrConflict
	}
	ev, err := taskCreatedEvent(s.now().UTC(), record)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if _, err := s.log.Append(ctx, s.scope(record), ev); err != nil {
		s.mu.Unlock()
		return err
	}
	s.records[key] = cloneTaskRecord(record)
	s.mu.Unlock()
	return nil
}

func (s *eventTaskStore) Get(_ context.Context, tenantID, taskID string) (task.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[taskKey(tenantID, taskID)]
	if !ok {
		return task.Record{}, task.ErrNotFound
	}
	return cloneTaskRecord(record), nil
}

func (s *eventTaskStore) List(_ context.Context, tenantID, runID string) ([]task.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []task.Record
	for _, record := range s.records {
		if record.Scope.TenantID == tenantID && (runID == "" || record.Scope.RunID == runID) {
			out = append(out, cloneTaskRecord(record))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *eventTaskStore) Resolve(ctx context.Context, tenantID, taskID string, tokenHash []byte, resolution task.Resolution, now time.Time) (task.Record, error) {
	s.mu.Lock()
	key := taskKey(tenantID, taskID)
	record, ok := s.records[key]
	if !ok {
		s.mu.Unlock()
		return task.Record{}, task.ErrNotFound
	}
	if !hmac.Equal(record.TokenHash, tokenHash) {
		s.mu.Unlock()
		return task.Record{}, task.ErrUnauthorized
	}
	if record.State != task.StatePending {
		// Idempotent re-resolution with the same decision returns the existing
		// record; a conflicting decision is rejected.
		if record.Resolution.Decision == resolution.Decision &&
			string(record.Resolution.Data) == string(resolution.Data) &&
			record.Resolution.Reason == resolution.Reason {
			s.mu.Unlock()
			return cloneTaskRecord(record), nil
		}
		s.mu.Unlock()
		return task.Record{}, task.ErrConflict
	}
	if record.ExpiresAt != nil && !now.Before(*record.ExpiresAt) {
		s.mu.Unlock()
		return task.Record{}, task.ErrGone
	}
	record.State = resolution.Decision
	record.Resolution = resolution
	resolvedAt := now
	record.ResolvedAt = &resolvedAt
	ev, err := taskResolvedEvent(s.now().UTC(), record)
	if err != nil {
		s.mu.Unlock()
		return task.Record{}, err
	}
	if _, err := s.log.Append(ctx, s.scope(record), ev); err != nil {
		s.mu.Unlock()
		return task.Record{}, err
	}
	s.records[key] = cloneTaskRecord(record)
	s.mu.Unlock()
	return cloneTaskRecord(record), nil
}

func (s *eventTaskStore) MarkExecuted(ctx context.Context, tenantID, taskID string, _ time.Time) error {
	s.mu.Lock()
	key := taskKey(tenantID, taskID)
	record, ok := s.records[key]
	if !ok {
		s.mu.Unlock()
		return task.ErrNotFound
	}
	record.State = task.StateExecuted
	ev, err := taskExecutedEvent(s.now().UTC(), record.Scope.RunID, record.Scope.Revision, taskID)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if _, err := s.log.Append(ctx, s.scope(record), ev); err != nil {
		s.mu.Unlock()
		return err
	}
	s.records[key] = cloneTaskRecord(record)
	s.mu.Unlock()
	return nil
}

func cloneTaskRecord(record task.Record) task.Record {
	clone := record
	clone.Call = record.Call.Copy()
	clone.TokenHash = append([]byte(nil), record.TokenHash...)
	clone.Resolution.Data = append([]byte(nil), record.Resolution.Data...)
	clone.ExpiresAt = copyTime(record.ExpiresAt)
	clone.ResolvedAt = copyTime(record.ResolvedAt)
	return clone
}
