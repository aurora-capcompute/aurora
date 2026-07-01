package agent

// Read projections: the *Locked helpers that fold in-memory thread and run state
// into the immutable snapshots the public API returns, plus the small pure
// helpers (titles, visible capabilities, defensive copies) they lean on.

import (
	"crypto/rand"
	"encoding/hex"
	"github.com/aurora-capcompute/capcompute/dispatcher"
	"strings"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/internal/task"
)

// Read projections: building API snapshots and StoredThread/StoredRun event
// payloads from in-memory state, plus small shared helpers.
func (r *Runtime) threadSummaryLocked(thread *threadState) ThreadSummary {
	return ThreadSummary{
		ID:          thread.id,
		Title:       thread.title,
		CreatedAt:   thread.createdAt,
		UpdatedAt:   thread.updatedAt,
		RunCount:    len(thread.runIDs),
		ActiveRunID: thread.activeRunID,
		Tags:        cloneTags(thread.tags),
	}
}

func threadTitle(message string) string {
	fields := strings.Fields(message)
	if len(fields) == 0 {
		return "New thread"
	}
	title := strings.Join(fields, " ")
	runes := []rune(title)
	if len(runes) <= 60 {
		return title
	}
	return string(runes[:60]) + "…"
}

func (r *Runtime) threadSnapshotLocked(thread *threadState) ThreadSnapshot {
	runs := make([]RunSnapshot, 0, len(thread.runIDs))
	for _, runID := range thread.runIDs {
		if run := r.runs[runID]; run != nil {
			runs = append(runs, r.runSnapshotLocked(run))
		}
	}
	return ThreadSnapshot{
		ThreadSummary: r.threadSummaryLocked(thread),
		History:       append([]HistoryMessage(nil), thread.history...),
		Runs:          runs,
	}
}

func (r *Runtime) runSnapshotLocked(run *runState) RunSnapshot {
	journalLength := 0
	if run.journal != nil {
		journalLength = run.journal.Length()
	}
	return RunSnapshot{
		ID:            run.id,
		ThreadID:      run.threadID,
		Message:       run.message,
		Status:        run.status,
		Attempt:       run.attempt,
		Revision:      run.revision,
		Answer:        run.answer,
		Error:         run.err,
		JournalLength: journalLength,
		CreatedAt:     run.createdAt,
		UpdatedAt:     run.updatedAt,
		StartedAt:     copyTime(run.startedAt),
		CompletedAt:   copyTime(run.completedAt),
		Manifest:      cloneManifest(run.manifest),
		BrainDigest:   run.brainDigest,
	}
}

func (r *Runtime) taskSnapshot(record task.Record) TaskSnapshot {
	return TaskSnapshot{
		ID:              record.ID,
		RunID:           record.Scope.RunID,
		Revision:        record.Scope.Revision,
		JournalPosition: record.JournalPosition,
		Call:            record.Call.Copy(),
		Summary:         record.Summary,
		State:           record.State,
		Resolution:      record.Resolution,
		CreatedAt:       record.CreatedAt,
		ExpiresAt:       copyTime(record.ExpiresAt),
		ResolvedAt:      copyTime(record.ResolvedAt),
		WebhookToken:    task.Token(r.taskSecret, record.Scope.TenantID, record.ID),
	}
}

func hasPendingTask(records []task.Record) bool {
	for _, record := range records {
		if record.State == task.StatePending {
			return true
		}
	}
	return false
}

func copyTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// visibleCapabilities drops capabilities marked Hidden (e.g. the LLM cognition
// tool) from the brain's discoverable menu. Hidden is set at build time on each
// published capability, so it works even when a tool's published operation names
// differ from its local name.
func visibleCapabilities(caps []dispatcher.Capability) []dispatcher.Capability {
	visible := make([]dispatcher.Capability, 0, len(caps))
	for _, c := range caps {
		if !c.Hidden {
			visible = append(visible, c)
		}
	}
	return visible
}

func (r *Runtime) runContext(run *runState) RunContext {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runContextLocked(run)
}

func (r *Runtime) runContextLocked(run *runState) RunContext {
	return RunContext{
		TenantID: r.tenantID,
		ThreadID: run.threadID,
		RunID:    run.id,
		Revision: run.revision,
	}
}

func (r *Runtime) storedRunLocked(run *runState) StoredRun {
	var tags map[string]string
	if thread := r.threads[run.threadID]; thread != nil {
		tags = cloneTags(thread.tags)
	}
	return StoredRun{
		TenantID:          r.tenantID,
		ID:                run.id,
		ThreadID:          run.threadID,
		Revision:          run.revision,
		Message:           run.message,
		Status:            run.status,
		Attempt:           run.attempt,
		CreatedAt:         run.createdAt,
		UpdatedAt:         run.updatedAt,
		StartedAt:         copyTime(run.startedAt),
		CompletedAt:       copyTime(run.completedAt),
		Answer:            run.answer,
		Error:             run.err,
		Manifest:          cloneManifest(run.manifest),
		BrainDigest:       run.brainDigest,
		Tags:              tags,
		ParentRunID:       run.parentRunID,
		ChildRunIDs:       append([]string(nil), run.childRunIDs...),
		ChildSpawnOffsets: append([]int(nil), run.childSpawnOffsets...),
		ForkOffset:        run.forkOffset,
	}
}

func randomID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(raw[:]), nil
}

func cloneTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}
