package aurora

import (
	"github.com/aurora-capcompute/aurora-capcompute/internal/agent"
	"github.com/aurora-capcompute/aurora-capcompute/internal/eventlog"
	"github.com/aurora-capcompute/aurora-capcompute/internal/task"
	"github.com/aurora-capcompute/capcompute/dispatcher"
)

// DTOs

type Manifest = agent.Manifest
type Tool = agent.Tool
type AgentSettings = agent.AgentSettings
type ThreadSummary = agent.ThreadSummary
type ThreadSnapshot = agent.ThreadSnapshot
type HistoryMessage = agent.HistoryMessage
type RunSnapshot = agent.RunSnapshot
type TaskSnapshot = agent.TaskSnapshot
type JournalEntry = agent.JournalEntry
type JournalOutcome = agent.JournalOutcome
type RunGraphNode = agent.RunGraphNode
type ThreadGraph = agent.ThreadGraph
type ThreadGraphRun = agent.ThreadGraphRun
type Event = agent.Event
type JournalEvent = agent.JournalEvent
type ProgressEvent = agent.ProgressEvent
type BrainArtifact = agent.BrainArtifact

// Status types

type RunStatus = agent.RunStatus

const (
	RunQueued      = agent.RunQueued
	RunRunning     = agent.RunRunning
	RunStopping    = agent.RunStopping
	RunYielded     = agent.RunYielded
	RunWaitingTask = agent.RunWaitingTask
	RunInterrupted = agent.RunInterrupted
	RunCompleted   = agent.RunCompleted
	RunStopped     = agent.RunStopped
	RunFailed      = agent.RunFailed
)

type RetryMode = agent.RetryMode

const (
	RetryResume  = agent.RetryResume
	RetryRestart = agent.RetryRestart
)

// Construction

type Config = agent.Config
type BrainSource = agent.BrainSource
type BrainProvider = agent.BrainProvider
type DispatcherProvider = agent.DispatcherProvider

// Event log: the single append-only source of truth. Applications provide an
// EventLog implementation (and a Leases implementation for cross-instance
// coordination); the runtime folds the log into thread/run/task projections.

type EventLog = eventlog.Log
type LogEvent = eventlog.Event
type LogScope = eventlog.Scope
type Leases = agent.Leases
type RunContext = agent.RunContext

// Task types

type TaskScope = task.Scope
type TaskRecord = task.Record
type TaskState = dispatcher.Decision
type Resolution = dispatcher.Authorization

const (
	TaskStatePending   = task.StatePending
	TaskStateApproved  = task.StateApproved
	TaskStateCompleted = task.StateCompleted
	TaskStateFailed    = task.StateFailed
	TaskStateDenied    = task.StateDenied
	TaskStateCancelled = task.StateCancelled
	TaskStateExpired   = task.StateExpired
	TaskStateExecuted  = task.StateExecuted
)

// Error sentinels

var (
	ErrNotFound = agent.ErrNotFound
	ErrConflict = agent.ErrConflict
	ErrInvalid  = agent.ErrInvalid

	ErrTaskNotFound     = task.ErrNotFound
	ErrTaskConflict     = task.ErrConflict
	ErrTaskGone         = task.ErrGone
	ErrTaskUnauthorized = task.ErrUnauthorized
)

// Constants

const (
	DefaultTenantID    = agent.DefaultTenantID
	DefaultBrainID     = agent.DefaultBrainID
	ManifestVersion    = agent.ManifestVersion
	AgentToolType      = agent.AgentToolType
	OnFailureReport    = agent.OnFailureReport
	OnFailurePropagate = agent.OnFailurePropagate
)
