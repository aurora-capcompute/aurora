// Package agent is the runtime: it owns thread and run lifecycle, the play
// goroutine that drives a compiled Wasm brain, durable approval tasks, retries,
// event subscriptions, and the read projections (snapshots, journal, call graph)
// the public API exposes. All durable state is a fold of one append-only event
// stream per thread — the runtime keeps no mutable row store, and restore
// rebuilds in-memory state by replaying each stream from the beginning.
//
// It does not own capability implementations, brain bytes, or the concrete log:
// dispatchers, brains, the event log, leases, and the session store are all
// injected. The aurora package re-exports this package's types as the module's
// public surface.
package agent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aurora-capcompute/capcompute"
	"github.com/aurora-capcompute/capcompute/dispatcher"
	"github.com/aurora-capcompute/capcompute/dispatcher/replay/tape/journaled"
	"strings"
	"sync"
	"time"

	internalhost "github.com/aurora-capcompute/aurora-capcompute/internal/host"
	"github.com/aurora-capcompute/aurora-capcompute/internal/task"

	extism "github.com/extism/go-sdk"
)

func NewRuntime(ctx context.Context, config Config) (*Runtime, error) {
	if config.Dispatchers == nil {
		return nil, fmt.Errorf("%w: dispatcher provider is required", ErrInvalid)
	}
	if config.Log == nil {
		return nil, fmt.Errorf("%w: event log is required", ErrInvalid)
	}
	if config.Leases == nil {
		return nil, fmt.Errorf("%w: leases are required", ErrInvalid)
	}
	if config.SessionStore == nil {
		return nil, fmt.Errorf("%w: session store is required", ErrInvalid)
	}
	if len(config.TaskSecret) == 0 {
		return nil, fmt.Errorf("%w: task secret is required", ErrInvalid)
	}
	brains, err := loadBrains(ctx, config.Brains)
	if err != nil {
		return nil, err
	}
	runtime := &Runtime{
		computes:     make(map[string]*capcompute.ComputeCompiledPlugin[string, RunKey]),
		brains:       brains,
		sessionStore: config.SessionStore,
		log:          config.Log,
		leases:       config.Leases,
		tenantID:     strings.TrimSpace(config.TenantID),
		threads:      make(map[string]*threadState),
		runs:         make(map[string]*runState),
		subscribers:  make(map[string]map[uint64]chan Event),
		idSource:     config.IDSource,
		now:          config.Now,
		eventSize:    config.EventSize,
		taskSecret:   append([]byte(nil), config.TaskSecret...),
		taskTTL:      config.TaskTTL,
		instanceID:   strings.TrimSpace(config.InstanceID),
		leaseTTL:     config.LeaseTTL,
		dispatchers:  config.Dispatchers,
	}
	if runtime.tenantID == "" {
		runtime.tenantID = DefaultTenantID
	}
	if runtime.idSource == nil {
		runtime.idSource = randomID
	}
	if runtime.now == nil {
		runtime.now = time.Now
	}
	runtime.tasks = newEventTaskStore(runtime.log, runtime.now)
	if runtime.eventSize <= 0 {
		runtime.eventSize = 32
	}
	if runtime.taskTTL <= 0 {
		runtime.taskTTL = 24 * time.Hour
	}
	if runtime.instanceID == "" {
		instanceID, err := randomID("instance_")
		if err != nil {
			return nil, err
		}
		runtime.instanceID = instanceID
	}
	if runtime.leaseTTL <= 0 {
		runtime.leaseTTL = time.Hour
	}
	if err := runtime.restore(ctx); err != nil {
		return nil, fmt.Errorf("restore runtime: %w", err)
	}

	dispatcherFactory := internalhost.Factory[RunKey]{
		Base: func(resolveCtx context.Context, key RunKey) (dispatcher.Dispatcher[RunKey], error) {
			runtime.mu.Lock()
			run := runtime.runs[key.RunID]
			var manifest Manifest
			var message string
			var history []HistoryMessage
			if run != nil {
				manifest = cloneManifest(run.manifest)
				message = run.message
				history = append([]HistoryMessage(nil), run.history...)
			}
			runtime.mu.Unlock()
			if run == nil {
				return nil, fmt.Errorf("%w: run %s", ErrNotFound, key.RunID)
			}
			base, err := runtime.dispatchers.NewDispatcher(resolveCtx, key, manifest)
			if err != nil {
				return nil, err
			}
			var d dispatcher.Dispatcher[RunKey] = base
			d = newProgressDispatcher(d, runtime.publish, key.ThreadID, key.RunID)
			if agents := manifest.agentTools(); len(agents) > 0 {
				router, err := newAgentRouter(d, agents, runtime)
				if err != nil {
					return nil, err
				}
				d = router
			}
			// Wrap with the lifecycle dispatcher so agent.input/agent.finish are
			// recorded on the replay journal alongside capability calls.
			return newLifecycleDispatcher(d, message, history, manifest), nil
		},
		NewJournal: func(_ context.Context, key RunKey) (journaled.Journal, error) {
			runtime.mu.Lock()
			run := runtime.runs[key.RunID]
			runtime.mu.Unlock()
			if run != nil && run.journal != nil {
				return run.journal, nil
			}
			return newLogJournal(runtime.log, runtime.scope(key.ThreadID), key.RunID, key.Revision,
				newRunHistory(), 0, runtime.journalNow, runtime.journalAppendPublisher(key.ThreadID)), nil
		},
		Tasks:      runtime.tasks,
		TaskSecret: runtime.taskSecret,
		TaskTTL:    runtime.taskTTL,
		TaskScope: func(key RunKey) task.Scope {
			return task.Scope{
				TenantID: key.TenantID,
				ThreadID: key.ThreadID,
				RunID:    key.RunID,
				Revision: key.Revision,
			}
		},
		OnTaskCreated: func(record task.Record) {
			runtime.publish(record.Scope.ThreadID, Event{
				Type: "task.created",
				Data: runtime.taskSnapshot(record),
			})
		},
	}
	runtime.dispatcherFactory = dispatcherFactory
	for _, artifact := range brains.List() {
		source, err := brains.Source(artifact.ID)
		if err != nil {
			return nil, err
		}
		compute, err := runtime.compileBrain(ctx, artifact.ID, source.Wasm, artifact.Digest)
		if err != nil {
			for _, opened := range runtime.computes {
				_ = opened.CloseCompiled(context.Background())
			}
			return nil, err
		}
		runtime.computes[artifact.ID] = compute
	}
	return runtime, nil
}

// compileBrain compiles a brain's wasm into a runnable compute plugin. It is
// pure with respect to runtime state (it only reads the session store), so it
// can be called outside the runtime mutex while preparing a SetBrains swap.
func (r *Runtime) compileBrain(ctx context.Context, id string, wasm []byte, digest string) (*capcompute.ComputeCompiledPlugin[string, RunKey], error) {
	compute, err := capcompute.NewComputeCompiledPlugin[string, RunKey](ctx, capcompute.Config[string, RunKey]{
		Manifest: extism.Manifest{
			Wasm: []extism.Wasm{extism.WasmData{Data: wasm, Hash: digest, Name: id}},
		},
		PluginConfig: extism.PluginConfig{EnableWasi: true},
		SessionStore: r.sessionStore,
	})
	if err != nil {
		return nil, fmt.Errorf("compile brain %q: %w", id, err)
	}
	return compute, nil
}

// SetBrains declaratively reconciles the registered brains to the given set:
// brains absent from the set are removed, new or content-changed brains are
// (re)compiled, and unchanged brains are left running. It is safe to call at any
// time; the control plane uses it to hot-load brains from Brain CRDs without a
// restart. Compilation happens outside the runtime mutex so dispatch is only
// briefly paused for the swap. If any brain fails to compile, no change is
// applied. Removing a brain that an in-flight run is using is best-effort: that
// run fails on its next step.
func (r *Runtime) SetBrains(ctx context.Context, sources []BrainSource) error {
	current := r.brains.digests()
	desired := make(map[string]struct{}, len(sources))

	// Compile additions/replacements outside the lock; fail atomically.
	type compiled struct {
		id      string
		wasm    []byte
		digest  string
		compute *capcompute.ComputeCompiledPlugin[string, RunKey]
	}
	var fresh []compiled
	for _, src := range sources {
		id := strings.TrimSpace(src.ID)
		if id == "" || len(src.Wasm) == 0 {
			return fmt.Errorf("%w: brain id and wasm bytes are required", ErrInvalid)
		}
		if _, dup := desired[id]; dup {
			return fmt.Errorf("%w: duplicate brain %q", ErrInvalid, id)
		}
		desired[id] = struct{}{}
		wasm := append([]byte(nil), src.Wasm...)
		digest := digestOf(wasm)
		if cur, ok := current[id]; ok && cur == digest {
			continue // unchanged
		}
		compute, err := r.compileBrain(ctx, id, wasm, digest)
		if err != nil {
			for _, c := range fresh {
				_ = c.compute.CloseCompiled(context.Background())
			}
			return err
		}
		fresh = append(fresh, compiled{id: id, wasm: wasm, digest: digest, compute: compute})
	}

	// Swap under the runtime mutex (which guards r.computes), collecting the
	// compute plugins that are being replaced or removed so they can be closed
	// after the lock is released.
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		for _, c := range fresh {
			_ = c.compute.CloseCompiled(context.Background())
		}
		return fmt.Errorf("%w: runtime is closed", ErrConflict)
	}
	var retired []*capcompute.ComputeCompiledPlugin[string, RunKey]
	for _, c := range fresh {
		if old := r.computes[c.id]; old != nil {
			retired = append(retired, old)
		}
		r.computes[c.id] = c.compute
		r.brains.put(c.id, c.wasm, c.digest)
	}
	for id := range current {
		if _, keep := desired[id]; keep {
			continue
		}
		if old := r.computes[id]; old != nil {
			retired = append(retired, old)
		}
		delete(r.computes, id)
		r.brains.remove(id)
	}
	r.mu.Unlock()

	for _, old := range retired {
		_ = old.CloseCompiled(context.Background())
	}
	return nil
}

func (r *Runtime) CreateThread(tags map[string]string) (ThreadSnapshot, error) {
	id, err := r.idSource("thr_")
	if err != nil {
		return ThreadSnapshot{}, err
	}
	now := r.now().UTC()
	thread := &threadState{id: id, title: "New thread", createdAt: now, updatedAt: now, tags: cloneTags(tags)}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ThreadSnapshot{}, fmt.Errorf("%w: runtime is closed", ErrConflict)
	}
	r.threads[id] = thread
	return r.threadSnapshotLocked(thread), nil
}

func (r *Runtime) ListThreads() []ThreadSummary {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ThreadSummary, 0, len(r.threads))
	for _, thread := range r.threads {
		out = append(out, r.threadSummaryLocked(thread))
	}
	return out
}

func (r *Runtime) Brains() []BrainArtifact {
	return r.brains.List()
}

func (r *Runtime) GetThread(threadID string) (ThreadSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	thread := r.threads[threadID]
	if thread == nil {
		return ThreadSnapshot{}, fmt.Errorf("%w: thread %s", ErrNotFound, threadID)
	}
	return r.threadSnapshotLocked(thread), nil
}

func (r *Runtime) CreateRun(threadID string, message string, manifest Manifest) (RunSnapshot, error) {
	if message == "" {
		return RunSnapshot{}, fmt.Errorf("%w: message is required", ErrInvalid)
	}
	if strings.TrimSpace(manifest.Brain) == "" {
		manifest.Brain = r.brains.DefaultID()
	}
	manifest, err := ValidateManifest(manifest, r.dispatchers)
	if err != nil {
		return RunSnapshot{}, err
	}
	brain, err := r.brains.Resolve(manifest.Brain)
	if err != nil {
		return RunSnapshot{}, err
	}
	runID, err := r.idSource("run_")
	if err != nil {
		return RunSnapshot{}, err
	}
	now := r.now().UTC()

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: runtime is closed", ErrConflict)
	}
	thread := r.threads[threadID]
	if thread == nil {
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: thread %s", ErrNotFound, threadID)
	}
	if thread.activeRunID != "" {
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: thread already has active run %s", ErrConflict, thread.activeRunID)
	}
	run := &runState{
		id:          runID,
		threadID:    threadID,
		message:     message,
		history:     append([]HistoryMessage(nil), thread.history...),
		status:      RunQueued,
		attempt:     1,
		createdAt:   now,
		updatedAt:   now,
		manifest:    manifest,
		revision:    1,
		brainDigest: brain.Digest,
	}
	run.journal, err = r.newJournal(run, newRunHistory(), 0)
	if err != nil {
		r.mu.Unlock()
		return RunSnapshot{}, err
	}
	r.runs[runID] = run
	thread.runIDs = append(thread.runIDs, runID)
	if len(thread.runIDs) == 1 {
		thread.title = threadTitle(message)
	}
	thread.activeRunID = runID
	thread.updatedAt = now
	if err := r.appendRun(run); err != nil {
		delete(r.runs, runID)
		thread.runIDs = thread.runIDs[:len(thread.runIDs)-1]
		thread.activeRunID = ""
		r.mu.Unlock()
		return RunSnapshot{}, err
	}
	snapshot := r.runSnapshotLocked(run)
	r.mu.Unlock()

	r.publish(threadID, Event{Type: "run.updated", Data: snapshot})
	r.wg.Add(1)
	go r.execute(runID)
	return snapshot, nil
}

func (r *Runtime) GetRun(runID string) (RunSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	run := r.runs[runID]
	if run == nil {
		return RunSnapshot{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	return r.runSnapshotLocked(run), nil
}

func (r *Runtime) Journal(runID string) ([]JournalEntry, error) {
	r.mu.Lock()
	run := r.runs[runID]
	r.mu.Unlock()
	if run == nil {
		return nil, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	j, ok := run.journal.(*logJournal)
	if !ok {
		return nil, fmt.Errorf("%w: run %s has no readable journal", ErrNotFound, runID)
	}
	length := j.Length()
	entries := make([]JournalEntry, 0, length)
	for i := 0; i < length; i++ {
		record, err := j.Load(i)
		if err != nil {
			return nil, err
		}
		rev := j.rev
		if i < j.forkOffset {
			if r, ok2 := j.history.revAt(i, j.rev); ok2 {
				rev = r
			}
		}
		entries = append(entries, JournalEntry{
			Index:    i,
			Revision: rev,
			Call:     record.Call,
			Outcome: JournalOutcome{
				Status:  record.Outcome.Kind(),
				Result:  record.Outcome.Result(),
				Message: record.Outcome.Message(),
			},
		})
	}
	return entries, nil
}

// JournalRevisions returns a per-revision snapshot of the run's journal.
// For each revision r the snapshot contains, at every position, the entry with
// the highest revision ≤ r — i.e. the effective state of the run at that point.
// The entry's Revision field reflects when it was first written, so callers can
// distinguish steps carried forward from earlier revisions versus steps first
// executed at revision r (including any that failed in a prior attempt).
func (r *Runtime) JournalRevisions(runID string) (map[uint64][]JournalEntry, error) {
	r.mu.Lock()
	run := r.runs[runID]
	r.mu.Unlock()
	if run == nil {
		return nil, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	j, ok := run.journal.(*logJournal)
	if !ok {
		return nil, fmt.Errorf("%w: run %s has no readable journal", ErrNotFound, runID)
	}
	revs := j.history.allRevisions()
	positions := j.history.allPositions()
	result := make(map[uint64][]JournalEntry, len(revs))
	for _, rev := range revs {
		entries := make([]JournalEntry, 0, len(positions))
		for _, pos := range positions {
			rec, ok := j.history.getAt(pos, rev)
			if !ok {
				continue
			}
			actualRev, _ := j.history.revAt(pos, rev)
			entries = append(entries, JournalEntry{
				Index:    pos,
				Revision: actualRev,
				Call:     rec.Call,
				Outcome: JournalOutcome{
					Status:  rec.Outcome.Kind(),
					Result:  rec.Outcome.Result(),
					Message: rec.Outcome.Message(),
				},
			})
		}
		result[rev] = entries
	}
	return result, nil
}

func (r *Runtime) Tasks(runID string) ([]TaskSnapshot, error) {
	r.mu.Lock()
	run := r.runs[runID]
	r.mu.Unlock()
	if run == nil {
		return nil, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	records, err := r.tasks.List(context.Background(), r.tenantID, runID)
	if err != nil {
		return nil, err
	}
	out := make([]TaskSnapshot, 0, len(records))
	for _, record := range records {
		out = append(out, r.taskSnapshot(record))
	}
	return out, nil
}

func (r *Runtime) ResolveTask(taskID, token string, resolution task.Resolution) (TaskSnapshot, error) {
	switch resolution.Decision {
	case task.StateApproved, task.StateCompleted, task.StateFailed, task.StateDenied, task.StateCancelled:
	default:
		return TaskSnapshot{}, fmt.Errorf("%w: unsupported task decision %q", ErrInvalid, resolution.Decision)
	}
	if resolution.Decision == task.StateCompleted && !json.Valid(resolution.Data) {
		return TaskSnapshot{}, fmt.Errorf("%w: completed task data must be valid JSON", ErrInvalid)
	}
	acquired, err := r.leases.Acquire(
		context.Background(), r.tenantID, "task", taskID,
		r.instanceID, r.now().UTC(), time.Minute,
	)
	if err != nil {
		return TaskSnapshot{}, err
	}
	if !acquired {
		return TaskSnapshot{}, fmt.Errorf("%w: task is being resolved", ErrConflict)
	}
	defer r.leases.Release(context.Background(), r.tenantID, "task", taskID, r.instanceID)

	sum := sha256.Sum256([]byte(token))
	record, err := r.tasks.Resolve(
		context.Background(), r.tenantID, taskID, sum[:], resolution, r.now().UTC(),
	)
	if err != nil {
		return TaskSnapshot{}, err
	}
	r.publish(record.Scope.ThreadID, Event{Type: "task.updated", Data: r.taskSnapshot(record)})

	r.mu.Lock()
	run := r.runs[record.Scope.RunID]
	shouldResume := run != nil && run.status == RunWaitingTask
	r.mu.Unlock()
	if shouldResume {
		if _, retryErr := r.Retry(record.Scope.RunID, RetryResume); retryErr != nil {
			return TaskSnapshot{}, retryErr
		}
	}
	return r.taskSnapshot(record), nil
}

func (r *Runtime) Stop(runID string) (RunSnapshot, error) {
	r.mu.Lock()
	run := r.runs[runID]
	if run == nil {
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	var closeSession *capcompute.Session[RunKey]
	switch run.status {
	case RunQueued:
		run.stopRequested = true
		run.status = RunStopping
		run.updatedAt = r.now().UTC()
	case RunRunning:
		run.stopRequested = true
		run.status = RunStopping
		run.updatedAt = r.now().UTC()
		if run.handle != nil {
			run.handle.Stop()
		}
	case RunYielded, RunWaitingTask:
		closeSession = run.session
		r.finishLocked(run, RunStopped, "", context.Canceled)
	case RunStopping, RunStopped:
	default:
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: run %s cannot be stopped from %s", ErrConflict, runID, run.status)
	}
	snapshot := r.runSnapshotLocked(run)
	_ = r.appendRun(run)
	r.mu.Unlock()
	if closeSession != nil {
		_ = closeSession.Close(context.Background())
	}
	r.publish(run.threadID, Event{Type: "run.updated", Data: snapshot})
	return snapshot, nil
}

func (r *Runtime) Retry(runID string, mode RetryMode) (RunSnapshot, error) {
	if mode != RetryResume && mode != RetryRestart {
		return RunSnapshot{}, fmt.Errorf("%w: retry mode must be resume or restart", ErrInvalid)
	}

	r.mu.Lock()
	run := r.runs[runID]
	if run == nil {
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	switch run.status {
	case RunYielded, RunWaitingTask, RunStopped, RunFailed, RunInterrupted:
	case RunCompleted:
		// A completed run has nothing to resume, but it can be restarted from
		// scratch (re-run as a new copy-on-write revision). This also lets a
		// parent restart cascade into already-completed children.
		if mode != RetryRestart {
			r.mu.Unlock()
			return RunSnapshot{}, fmt.Errorf("%w: completed run %s can only be restarted, not resumed", ErrConflict, runID)
		}
	default:
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: run %s cannot retry from %s", ErrConflict, runID, run.status)
	}
	thread := r.threads[run.threadID]
	if run.parentRunID == "" {
		// Root runs may only be retried if no later user-initiated run has arrived.
		// Child runs that were added to the same thread by delegation do not count.
		lastRootID := ""
		for i := len(thread.runIDs) - 1; i >= 0; i-- {
			if r.runs[thread.runIDs[i]] != nil && r.runs[thread.runIDs[i]].parentRunID == "" {
				lastRootID = thread.runIDs[i]
				break
			}
		}
		if lastRootID == "" || lastRootID != run.id {
			r.mu.Unlock()
			return RunSnapshot{}, fmt.Errorf("%w: only the latest thread run can be retried", ErrConflict)
		}
	}
	// Allow cascade retry of a child while its parent holds activeRunID.
	if thread.activeRunID != "" && thread.activeRunID != run.id &&
		(run.parentRunID == "" || thread.activeRunID != run.parentRunID) {
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: thread already has active run %s", ErrConflict, thread.activeRunID)
	}

	if mode == RetryRestart {
		// Hard restart: always fork from the beginning (the agent.input step),
		// giving the brain a completely fresh revision with no shared prefix.
		if err := r.forkJournalLocked(run, 0, RetryRestart); err != nil {
			r.mu.Unlock()
			return RunSnapshot{}, err
		}
	} else {
		isSessionPreserved := run.status == RunYielded || run.status == RunWaitingTask
		run.preserveSession = isSessionPreserved
		if isSessionPreserved {
			if run.reconnectChildren {
				// Resuming a parent suspended on a delegated child's approval. This is
				// a continuation, not a retry: the delegation call yielded and was
				// never recorded, so re-executing it appends to the SAME revision
				// (no fork — forking would gratuitously bump every step from the
				// delegation call onward). Enable cascade reconnection so that call
				// reuses the now-finished child instead of spawning a fresh one,
				// positioning the cursor past children already replayed from the
				// recorded prefix.
				run.cascade = true
				run.cascadeMode = RetryResume
				run.cascadeCursor = childrenBefore(run.childSpawnOffsets, run.journal.Length())
			} else {
				run.cascade = false
			}
		} else {
			j, ok := run.journal.(*logJournal)
			if !ok {
				r.mu.Unlock()
				return RunSnapshot{}, fmt.Errorf("%w: run %s has no forkable journal", ErrConflict, run.id)
			}
			// Default: fork at the end of the journal and let the brain continue,
			// replaying every recorded outcome including soft failures. A failed run
			// only forks earlier when the brain explicitly left a savepoint open: we
			// fork right after the outermost still-open host.try so its whole body
			// re-executes live under the bumped revision. Nothing is inferred from a
			// bare failing call — soft failures are simply replayed.
			forkOffset := j.Length()
			if run.status == RunFailed {
				if off, ok := j.outermostOpenTry(); ok {
					forkOffset = off
				}
			}
			if err := r.forkJournalLocked(run, forkOffset, RetryResume); err != nil {
				r.mu.Unlock()
				return RunSnapshot{}, err
			}
		}
	}
	run.status = RunQueued
	run.attempt++
	run.answer = ""
	run.err = ""
	run.failure = nil
	run.stopRequested = false
	run.startedAt = nil
	run.completedAt = nil
	run.updatedAt = r.now().UTC()
	thread.activeRunID = run.id
	thread.updatedAt = run.updatedAt
	if err := r.appendRun(run); err != nil {
		r.mu.Unlock()
		return RunSnapshot{}, err
	}
	snapshot := r.runSnapshotLocked(run)
	r.mu.Unlock()

	r.publish(run.threadID, Event{Type: "run.updated", Data: snapshot})
	r.wg.Add(1)
	go r.execute(runID)
	return snapshot, nil
}

// forkJournalLocked re-forks run's journal at forkOffset, records the retry mode
// for downstream cascade children, and sets cascade/preserveSession.
// Must be called with the runtime mutex held.
func (r *Runtime) forkJournalLocked(run *runState, forkOffset int, mode RetryMode) error {
	parentJournal, ok := run.journal.(*logJournal)
	if !ok {
		return fmt.Errorf("%w: run %s has no forkable journal", ErrConflict, run.id)
	}
	run.revision++
	run.forkOffset = forkOffset
	run.journal = newLogJournal(
		parentJournal.log, parentJournal.scope, parentJournal.run, run.revision,
		parentJournal.history, forkOffset,
		parentJournal.now, parentJournal.onAppend,
	)
	run.preserveSession = false
	// Reuse the existing child subtree in spawn order (deep cascade resume).
	// Children whose spawn call is replayed from the shared prefix are skipped;
	// the cursor starts at the first child re-executed past the fork offset.
	run.cascade = true
	run.cascadeMode = mode
	run.cascadeCursor = childrenBefore(run.childSpawnOffsets, forkOffset)
	return nil
}

// childrenBefore counts the children whose spawn call sits before offset in the
// parent journal. Those are replayed from the shared/recorded prefix, so the
// cascade cursor starts past them — at the first child re-executed at offset.
func childrenBefore(spawnOffsets []int, offset int) int {
	n := 0
	for _, off := range spawnOffsets {
		if off < offset {
			n++
		}
	}
	return n
}

func (r *Runtime) Subscribe(threadID string) (Event, <-chan Event, func(), error) {
	r.mu.Lock()
	thread := r.threads[threadID]
	if thread == nil {
		r.mu.Unlock()
		return Event{}, nil, nil, fmt.Errorf("%w: thread %s", ErrNotFound, threadID)
	}
	r.nextSubID++
	id := r.nextSubID
	ch := make(chan Event, r.eventSize)
	if r.subscribers[threadID] == nil {
		r.subscribers[threadID] = make(map[uint64]chan Event)
	}
	r.subscribers[threadID][id] = ch
	snapshot := Event{Type: "snapshot", Data: r.threadSnapshotLocked(thread)}
	r.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.subscribers[threadID], id)
			r.mu.Unlock()
		})
	}
	return snapshot, ch, unsubscribe, nil
}

func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	handles := make([]*capcompute.PlayHandle[RunKey], 0)
	for _, run := range r.runs {
		if run.handle != nil && (run.status == RunRunning || run.status == RunStopping) {
			handles = append(handles, run.handle)
		}
	}
	r.mu.Unlock()
	for _, handle := range handles {
		handle.Stop()
	}

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	r.mu.Lock()
	sessions := make([]*capcompute.Session[RunKey], 0, len(r.runs))
	for _, run := range r.runs {
		if run.session != nil {
			sessions = append(sessions, run.session)
		}
	}
	r.mu.Unlock()
	for _, session := range sessions {
		_ = session.Close(context.Background())
	}
	closeErrors := []error{}
	for _, compute := range r.computes {
		closeErrors = append(closeErrors, compute.CloseCompiled(context.Background()))
	}
	return errors.Join(closeErrors...)
}
