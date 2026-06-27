package agent

// Read projections: the *Locked helpers that fold in-memory thread and run state
// into the immutable snapshots the public API returns, plus the small pure
// helpers (titles, visible capabilities, defensive copies) they lean on.

import (
	"capcompute/dispatcher"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"

	"aurora-capcompute/internal/task"
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
		Manifest:    cloneManifest(thread.manifest),
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
		ID:                run.id,
		ThreadID:          run.threadID,
		Message:           run.message,
		Status:            run.status,
		Attempt:           run.attempt,
		Revision:          run.revision,
		Answer:            run.answer,
		Error:             run.err,
		JournalLength:     journalLength,
		CreatedAt:         run.createdAt,
		UpdatedAt:         run.updatedAt,
		StartedAt:         copyTime(run.startedAt),
		CompletedAt:       copyTime(run.completedAt),
		EffectiveManifest: cloneManifest(run.effectiveManifest),
		BrainDigest:       run.brainDigest,
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

func visibleCapabilities(caps []dispatcher.Capability, manifest Manifest) []dispatcher.Capability {
	hidden := make(map[string]bool, len(manifest.Capabilities))
	for _, c := range manifest.Capabilities {
		if c.Hidden {
			hidden[c.Name] = true
		}
	}
	visible := make([]dispatcher.Capability, 0, len(caps))
	for _, c := range caps {
		if !hidden[c.Name] {
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

func (r *Runtime) storedThreadLocked(thread *threadState) StoredThread {
	return StoredThread{
		TenantID:    r.tenantID,
		ID:          thread.id,
		Title:       thread.title,
		CreatedAt:   thread.createdAt,
		UpdatedAt:   thread.updatedAt,
		Manifest:    cloneManifest(thread.manifest),
		ActiveRunID: thread.activeRunID,
	}
}

func (r *Runtime) storedRunLocked(run *runState) StoredRun {
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
		EffectiveManifest: cloneManifest(run.effectiveManifest),
		BrainDigest:       run.brainDigest,
		ParentRunID:       run.parentRunID,
		ChildRunIDs:       append([]string(nil), run.childRunIDs...),
		ChildSpawnOffsets: append([]int(nil), run.childSpawnOffsets...),
		FailureOffset:     run.failureOffset,
	}
}

func randomID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(raw[:]), nil
}
