package agent

import (
	"context"
	"fmt"
	"time"
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

// StoredThread is a thread's durable state, carried by thread.state events and
// folded back into memory on restore.
type StoredThread struct {
	TenantID    string
	ID          string
	Title       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Manifest    Manifest
	ActiveRunID string
	// Tags are arbitrary key-value labels set at creation time (e.g.
	// "telegram:chat_id" → "12345"). Channel bridges use them to find their
	// threads from the log without maintaining a separate mapping store.
	Tags map[string]string
	// Revision is a monotonic counter bumped each time a replay creates a new
	// thread timeline (i.e. truncates the run list to a fork point).
	Revision uint64 `json:"revision,omitempty"`
	// RunIDs is the canonical ordered list of runs in the current timeline.
	// When non-empty it supersedes the CreatedAt sort used by older logs.
	RunIDs []string `json:"run_ids,omitempty"`
}

// StoredRun is a run's durable state, carried by run.state events and folded
// back into memory on restore.
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
	// ParentRunID links a delegated child run back to the run that spawned it;
	// ChildRunIDs records, in spawn order, the child runs this run delegated to.
	ParentRunID string
	ChildRunIDs []string
	// ChildSpawnOffsets records the journal position each child was spawned at,
	// parallel to ChildRunIDs. FailureOffset is the journal length captured when
	// the run last failed, used to fork a hard retry just before the failing step.
	ChildSpawnOffsets []int
	FailureOffset     int
}

// Leases coordinates exclusive run and task execution across runtime instances.
// It is deliberately separate from the event log: a lease is ephemeral
// coordination (a fencing token with a TTL), not part of a thread's immutable
// history.
type Leases interface {
	Acquire(ctx context.Context, tenantID, kind, resourceID, holder string, now time.Time, ttl time.Duration) (bool, error)
	Release(ctx context.Context, tenantID, kind, resourceID, holder string) error
}
