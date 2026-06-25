package aurora

import (
	"aurora-capcompute/internal/agent"
	"aurora-capcompute/internal/task"
	"aurora-dispatchers/resolution"
)

// DTOs

type Manifest = agent.Manifest
type ChildManifest = agent.ChildManifest
type CapabilityConfig = agent.CapabilityConfig
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
type RevisionView = agent.RevisionView
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

// Store interface and its type surface

type Store = agent.Store
type StateStore = agent.StateStore
type StoredState = agent.StoredState
type StoredThread = agent.StoredThread
type StoredRun = agent.StoredRun
type StoredMessage = agent.StoredMessage
type RunContext = agent.RunContext

// Task types

type TaskScope = task.Scope
type TaskRecord = task.Record
type TaskState = task.State
type Resolution = resolution.Resolution
type TaskStore = agent.TaskStore

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
	DefaultTenantID       = agent.DefaultTenantID
	DefaultBrainID        = agent.DefaultBrainID
	ManifestVersion       = agent.ManifestVersion
	LegacyManifestVersion = agent.LegacyManifestVersion
)
