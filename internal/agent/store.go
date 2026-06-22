package agent

import (
	"context"
	"fmt"
	"time"

	"aurora-capcompute/internal/task"
	"capcompute/dispatcher/replay/tape/journaled"
)

const DefaultTenantID = "local"

type RunContext struct {
	TenantID string `json:"tenant_id"`
	ThreadID string `json:"thread_id"`
	RunID    string `json:"run_id"`
	Revision uint64 `json:"revision"`
}

func (r RunContext) SessionKey() string {
	return fmt.Sprintf("%s/%s/%d", r.TenantID, r.RunID, r.Revision)
}

type StoredThread struct {
	TenantID    string
	ID          string
	Title       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Manifest    Manifest
	ActiveRunID string
}

type StoredRun struct {
	TenantID          string
	ID                string
	ThreadID          string
	Revision          uint64
	Message           string
	Status            RunStatus
	Attempt           int
	CreatedAt         time.Time
	UpdatedAt         time.Time
	StartedAt         *time.Time
	CompletedAt       *time.Time
	Answer            string
	Error             string
	EffectiveManifest Manifest
	BrainDigest       string
}

type StoredMessage struct {
	TenantID string
	ThreadID string
	Position int
	Role     string
	Content  string
}

type StoredState struct {
	Threads  []StoredThread
	Runs     []StoredRun
	Messages []StoredMessage
}

type StateStore interface {
	Load(context.Context, string) (StoredState, error)
	SaveThread(context.Context, StoredThread) error
	SaveRun(context.Context, StoredRun) error
	AppendMessages(context.Context, string, string, []HistoryMessage) error
	OpenJournal(context.Context, RunContext) (journaled.Journal, error)
	ResetJournal(context.Context, RunContext) error
	AcquireLease(context.Context, string, string, string, string, time.Time, time.Duration) (bool, error)
	ReleaseLease(context.Context, string, string, string, string) error
}

type TaskStore = task.Store

type Store interface {
	StateStore
	TaskStore
}
