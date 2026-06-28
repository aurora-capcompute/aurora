package agent

// Restore: rebuild the runtime's in-memory state on startup by folding each
// thread's event stream back into thread, run, and task projections.

import (
	"context"
	"log/slog"
	"sort"
)

// Restore: rebuild in-memory state by folding each thread's event stream.
func (r *Runtime) restore(ctx context.Context) error {
	scopes, err := r.log.Streams(ctx, r.tenantID)
	if err != nil {
		return err
	}
	for _, scope := range scopes {
		events, err := r.log.Read(ctx, scope, 0)
		if err != nil {
			return err
		}
		proj, err := Fold(events)
		if err != nil {
			return err
		}
		journals, histories, err := foldJournals(events, r.log, scope, r.journalNow, r.journalAppendPublisher(scope.ThreadID))
		if err != nil {
			return err
		}
		if err := r.restoreThread(proj, journals, histories); err != nil {
			return err
		}
		r.tasks.seed(proj.TaskList())
	}
	return nil
}

// restoreThread folds one thread's projection back into memory: it rebuilds the
// thread, its runs (in creation order, deriving conversation history from
// completed runs), and attaches each run's journal revision. Runs left mid-flight
// by a crash are marked interrupted and re-recorded.
func (r *Runtime) restoreThread(proj Projection, journals map[string]map[uint64]*logJournal, histories map[string]*runHistory) error {
	stored := proj.Thread
	if stored.ID == "" {
		return nil
	}
	if stored.Manifest.Brain == "" {
		stored.Manifest.Brain = r.brains.DefaultID()
	}
	manifest, err := ValidateManifest(stored.Manifest, r.dispatchers)
	if err != nil {
		return err
	}
	if _, err := r.brains.Resolve(manifest.Brain); err != nil {
		// Brain not yet registered (dynamic brain loading: SetBrains called after
		// restore). Skip the thread; it will be unreachable until the brain is
		// loaded. This also naturally discards threads from a previous brain ID
		// scheme that no longer matches any registered brain.
		slog.Info("skipping thread restore: brain not registered",
			"thread_id", stored.ID, "brain", manifest.Brain)
		return nil
	}
	thread := &threadState{
		id:          stored.ID,
		title:       stored.Title,
		createdAt:   stored.CreatedAt,
		updatedAt:   stored.UpdatedAt,
		activeRunID: stored.ActiveRunID,
		manifest:    cloneManifest(manifest),
		tags:        cloneTags(stored.Tags),
		revision:    stored.Revision,
	}
	r.threads[thread.id] = thread

	// restoreRun restores a single StoredRun into r.runs. Returns (ok, err):
	// ok=false means the run was silently skipped (brain mismatch/missing).
	restoreRun := func(sr StoredRun) (ok bool, err error) {
		if sr.EffectiveManifest.Brain == "" {
			sr.EffectiveManifest.Brain = r.brains.DefaultID()
		}
		em, err := ValidateManifest(sr.EffectiveManifest, r.dispatchers)
		if err != nil {
			return false, err
		}
		brain, err := r.brains.Resolve(em.Brain)
		if err != nil {
			slog.Info("skipping run restore: brain not registered",
				"run_id", sr.ID, "brain", em.Brain)
			return false, nil
		}
		if sr.BrainDigest != "" && sr.BrainDigest != brain.Digest {
			slog.Info("skipping run with outdated brain digest",
				"run_id", sr.ID, "brain", brain.ID,
				"stored_digest", sr.BrainDigest, "current_digest", brain.Digest)
			return false, nil
		}
		status := sr.Status
		if status == RunQueued || status == RunRunning || status == RunStopping {
			status = RunInterrupted
		}
		run := &runState{
			id:                sr.ID,
			threadID:          sr.ThreadID,
			message:           sr.Message,
			status:            status,
			attempt:           sr.Attempt,
			revision:          sr.Revision,
			createdAt:         sr.CreatedAt,
			updatedAt:         sr.UpdatedAt,
			startedAt:         copyTime(sr.StartedAt),
			completedAt:       copyTime(sr.CompletedAt),
			answer:            sr.Answer,
			err:               sr.Error,
			effectiveManifest: cloneManifest(em),
			brainDigest:       brain.Digest,
			parentRunID:       sr.ParentRunID,
			childRunIDs:       append([]string(nil), sr.ChildRunIDs...),
			childSpawnOffsets: append([]int(nil), sr.ChildSpawnOffsets...),
			failureOffset:     sr.FailureOffset,
		}
		if run.revision == 0 {
			run.revision = 1
		}
		if j := journals[run.id][run.revision]; j != nil {
			run.journal = j
		} else {
			history := histories[run.id]
			if history == nil {
				history = newRunHistory()
			}
			forkOffset := 0
			if sr.FailureOffset > 0 {
				forkOffset = sr.FailureOffset - 1
			}
			run.journal, err = r.newJournal(run, history, forkOffset)
			if err != nil {
				return false, err
			}
		}
		r.runs[run.id] = run
		if status != sr.Status {
			if err := r.appendRun(run); err != nil {
				return false, err
			}
		}
		return true, nil
	}

	// Restore ALL runs into r.runs so delegated children are reachable for the
	// cascade resume path, regardless of whether they are in the current timeline.
	allRuns := make([]StoredRun, 0, len(proj.Runs))
	for _, sr := range proj.Runs {
		allRuns = append(allRuns, sr)
	}
	sort.Slice(allRuns, func(i, j int) bool { return allRuns[i].CreatedAt.Before(allRuns[j].CreatedAt) })
	for _, sr := range allRuns {
		if _, err := restoreRun(sr); err != nil {
			return err
		}
	}

	// Build thread.runIDs and thread.history from the canonical timeline.
	// When stored.RunIDs is non-empty it carries the post-replay list; otherwise
	// fall back to the CreatedAt-sorted order (backward compat for old logs).
	timelineIDs := stored.RunIDs
	if len(timelineIDs) == 0 {
		for _, sr := range allRuns {
			timelineIDs = append(timelineIDs, sr.ID)
		}
	}
	for _, id := range timelineIDs {
		run := r.runs[id]
		if run == nil {
			continue // skipped (brain mismatch or orphaned)
		}
		run.history = append([]HistoryMessage(nil), thread.history...)
		thread.runIDs = append(thread.runIDs, id)
		if run.status == RunCompleted {
			thread.history = append(thread.history,
				HistoryMessage{Role: "user", Content: run.message},
				HistoryMessage{Role: "assistant", Content: run.answer},
			)
		}
	}
	if thread.activeRunID != "" && r.runs[thread.activeRunID] == nil {
		slog.Info("clearing active run from thread due to brain digest mismatch",
			"thread_id", thread.id, "run_id", thread.activeRunID)
		thread.activeRunID = ""
	}
	return nil
}
