package agent

import (
	"capcompute"
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay/tape/journaled"
	"encoding/json"
	"errors"
	"sync"
	"time"

	internalhost "aurora-capcompute/internal/host"
	"aurora-capcompute/internal/task"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("lifecycle conflict")
	ErrInvalid  = errors.New("invalid request")
)

type RunKey = RunContext

type RunStatus string

const (
	RunQueued      RunStatus = "queued"
	RunRunning     RunStatus = "running"
	RunStopping    RunStatus = "stopping"
	RunYielded     RunStatus = "yielded"
	RunWaitingTask RunStatus = "waiting_for_task"
	RunInterrupted RunStatus = "interrupted"
	RunCompleted   RunStatus = "completed"
	RunStopped     RunStatus = "stopped"
	RunFailed      RunStatus = "failed"
)

type RetryMode string

const (
	RetryResume  RetryMode = "resume"
	RetryRestart RetryMode = "restart"
)

type HistoryMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Config struct {
	Brains       BrainProvider
	Dispatchers  DispatcherProvider
	StateStore   StateStore
	TaskStore    TaskStore
	SessionStore capcompute.SessionStore[string, RunKey]
	IDSource     func(prefix string) (string, error)
	Now          func() time.Time
	EventSize    int
	TenantID     string
	TaskSecret   []byte
	TaskTTL      time.Duration
	InstanceID   string
	LeaseTTL     time.Duration
}

type Runtime struct {
	mu                sync.Mutex
	computes          map[string]*capcompute.ComputeCompiledPlugin[string, RunKey]
	brains            *loadedBrains
	sessionStore      capcompute.SessionStore[string, RunKey]
	stateStore        StateStore
	taskStore         TaskStore
	tenantID          string
	threads           map[string]*threadState
	runs              map[string]*runState
	subscribers       map[string]map[uint64]chan Event
	nextSubID         uint64
	idSource          func(string) (string, error)
	now               func() time.Time
	eventSize         int
	taskSecret        []byte
	taskTTL           time.Duration
	instanceID        string
	leaseTTL          time.Duration
	dispatchers       DispatcherProvider
	dispatcherFactory internalhost.Factory[RunKey]
	wg                sync.WaitGroup
	closed            bool
}

type threadState struct {
	id          string
	title       string
	createdAt   time.Time
	updatedAt   time.Time
	history     []HistoryMessage
	runIDs      []string
	activeRunID string
	manifest    Manifest
}

type runState struct {
	id                string
	threadID          string
	message           string
	history           []HistoryMessage
	status            RunStatus
	attempt           int
	createdAt         time.Time
	updatedAt         time.Time
	startedAt         *time.Time
	completedAt       *time.Time
	answer            string
	err               string
	journal           journaled.Journal
	session           *capcompute.Session[RunKey]
	handle            *capcompute.PlayHandle[RunKey]
	stopRequested     bool
	preserveSession   bool
	effectiveManifest Manifest
	revision          uint64
	brainDigest       string
	// parentRunID and childRunIDs make delegated runs addressable: a child knows
	// the run that spawned it, and a parent records its children in spawn order.
	parentRunID string
	childRunIDs []string
	// childSpawnOffsets records, parallel to childRunIDs, the journal position at
	// which each child was spawned. It lets a fork-from-offset retry start the
	// cascade cursor past children whose spawn call is replayed from the shared
	// prefix, so only re-executed children are reused.
	childSpawnOffsets []int
	// failureOffset is the journal length captured when the run last failed; a hard
	// retry forks just before it so the failing step re-executes over a shared
	// copy-on-write prefix rather than re-running from scratch.
	failureOffset int
	// cascade re-execution state: when a run is restarted, cascade is set so the
	// delegation router reuses (retries) the existing children at cascadeCursor in
	// spawn order rather than spawning fresh ones.
	cascade       bool
	cascadeCursor int
	// failure forces the run to finish as failed regardless of how its play ends;
	// set when a delegated child fails under an OnFailurePropagate policy.
	failure error
}

type agentInput struct {
	Message      string                  `json:"message"`
	History      []HistoryMessage        `json:"history,omitempty"`
	SystemPrompt string                  `json:"system_prompt,omitempty"`
	Capabilities []dispatcher.Capability `json:"capabilities,omitempty"`
}

type agentOutput struct {
	Status string `json:"status"`
	Answer string `json:"answer"`
}

type ThreadSummary struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	RunCount    int       `json:"run_count"`
	ActiveRunID string    `json:"active_run_id,omitempty"`
	Manifest    Manifest  `json:"manifest"`
}

type ThreadSnapshot struct {
	ThreadSummary
	History []HistoryMessage `json:"history"`
	Runs    []RunSnapshot    `json:"runs"`
}

type RunSnapshot struct {
	ID                string     `json:"id"`
	ThreadID          string     `json:"thread_id"`
	Message           string     `json:"message"`
	Status            RunStatus  `json:"status"`
	Attempt           int        `json:"attempt"`
	Revision          uint64     `json:"revision"`
	Answer            string     `json:"answer,omitempty"`
	Error             string     `json:"error,omitempty"`
	JournalLength     int        `json:"journal_length"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
	EffectiveManifest Manifest   `json:"effective_manifest"`
	BrainDigest       string     `json:"brain_digest"`
}

type TaskSnapshot struct {
	ID              string          `json:"id"`
	RunID           string          `json:"run_id"`
	Revision        uint64          `json:"revision"`
	JournalPosition int             `json:"journal_position"`
	Call            dispatcher.Call `json:"call"`
	Summary         string          `json:"summary"`
	State           task.State      `json:"state"`
	Resolution      task.Resolution `json:"resolution,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
	ResolvedAt      *time.Time      `json:"resolved_at,omitempty"`
	WebhookToken    string          `json:"webhook_token"`
}

type JournalEntry struct {
	Index   int             `json:"index"`
	Call    dispatcher.Call `json:"call"`
	Outcome JournalOutcome  `json:"outcome"`
}

type JournalOutcome struct {
	Status  dispatcher.OutcomeKind `json:"status"`
	Result  json.RawMessage        `json:"result,omitempty"`
	Message string                 `json:"message,omitempty"`
}

type Event struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type JournalEvent struct {
	RunID         string                 `json:"run_id"`
	Index         int                    `json:"index"`
	Call          string                 `json:"call"`
	OutcomeStatus dispatcher.OutcomeKind `json:"outcome_status"`
	OutcomeSize   int                    `json:"outcome_size"`
}
