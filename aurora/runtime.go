package aurora

import "context"

type Runtime interface {
	CreateThread(manifest Manifest) (ThreadSnapshot, error)
	ListThreads() []ThreadSummary
	Brains() []BrainArtifact
	GetThread(threadID string) (ThreadSnapshot, error)
	CreateRun(threadID string, message string, overrides []CapabilityConfig) (RunSnapshot, error)
	GetRun(runID string) (RunSnapshot, error)
	Journal(runID string) ([]JournalEntry, error)
	CallGraph(runID string) (RunGraphNode, error)
	ThreadGraph(threadID string) (ThreadGraph, error)
	Tasks(runID string) ([]TaskSnapshot, error)
	ResolveTask(taskID string, token string, resolution Resolution) (TaskSnapshot, error)
	Stop(runID string) (RunSnapshot, error)
	Retry(runID string, mode RetryMode, overrides []CapabilityConfig) (RunSnapshot, error)
	Subscribe(threadID string) (Event, <-chan Event, func(), error)
	Close(ctx context.Context) error
}
