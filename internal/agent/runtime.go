package agent

import (
	"capcompute"
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay/tape/journaled"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	internalhost "aurora-capcompute/internal/host"
	"aurora-capcompute/internal/task"

	extism "github.com/extism/go-sdk"
)

func NewRuntime(ctx context.Context, config Config) (*Runtime, error) {
	if config.Dispatchers == nil {
		return nil, fmt.Errorf("%w: dispatcher provider is required", ErrInvalid)
	}
	if config.StateStore == nil {
		return nil, fmt.Errorf("%w: state store is required", ErrInvalid)
	}
	if config.TaskStore == nil {
		return nil, fmt.Errorf("%w: task store is required", ErrInvalid)
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
		stateStore:   config.StateStore,
		taskStore:    config.TaskStore,
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
				manifest = cloneManifest(run.effectiveManifest)
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
			if hasCapability(manifest, "aurora.log") {
				d = newProgressDispatcher(d, runtime.publish, key.ThreadID, key.RunID)
			}
			if len(manifest.Children) > 0 {
				d = newDelegationRouter(d, manifest.Children, runtime)
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
			return runtime.stateStore.OpenJournal(context.Background(), key)
		},
		Tasks:      runtime.taskStore,
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

func (r *Runtime) CreateThread(manifest Manifest) (ThreadSnapshot, error) {
	if strings.TrimSpace(manifest.Brain) == "" {
		manifest.Brain = r.brains.DefaultID()
	}
	manifest, err := ValidateManifest(manifest, r.dispatchers)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	if _, err := r.brains.Resolve(manifest.Brain); err != nil {
		return ThreadSnapshot{}, err
	}
	id, err := r.idSource("thr_")
	if err != nil {
		return ThreadSnapshot{}, err
	}
	now := r.now().UTC()
	thread := &threadState{id: id, title: "New thread", createdAt: now, updatedAt: now, manifest: manifest}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ThreadSnapshot{}, fmt.Errorf("%w: runtime is closed", ErrConflict)
	}
	if err := r.stateStore.SaveThread(context.Background(), r.storedThreadLocked(thread)); err != nil {
		return ThreadSnapshot{}, err
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

func (r *Runtime) CreateRun(threadID string, message string, overrides []CapabilityConfig) (RunSnapshot, error) {
	if message == "" {
		return RunSnapshot{}, fmt.Errorf("%w: message is required", ErrInvalid)
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
	effectiveManifest, err := EffectiveManifest(thread.manifest, overrides, r.dispatchers)
	if err != nil {
		r.mu.Unlock()
		return RunSnapshot{}, err
	}
	brain, err := r.brains.Resolve(effectiveManifest.Brain)
	if err != nil {
		r.mu.Unlock()
		return RunSnapshot{}, err
	}
	run := &runState{
		id:                runID,
		threadID:          threadID,
		message:           message,
		history:           append([]HistoryMessage(nil), thread.history...),
		status:            RunQueued,
		attempt:           1,
		createdAt:         now,
		updatedAt:         now,
		effectiveManifest: effectiveManifest,
		revision:          1,
		brainDigest:       brain.Digest,
	}
	run.journal, err = r.newJournal(run)
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
	if err := r.stateStore.SaveRun(context.Background(), r.storedRunLocked(run)); err != nil {
		delete(r.runs, runID)
		thread.runIDs = thread.runIDs[:len(thread.runIDs)-1]
		thread.activeRunID = ""
		r.mu.Unlock()
		return RunSnapshot{}, err
	}
	if err := r.stateStore.SaveThread(context.Background(), r.storedThreadLocked(thread)); err != nil {
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
	var journal journaled.Journal
	if run != nil {
		journal = run.journal
	}
	r.mu.Unlock()
	if run == nil {
		return nil, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	length := journal.Length()
	entries := make([]JournalEntry, 0, length)
	for i := 0; i < length; i++ {
		record, err := journal.Load(i)
		if err != nil {
			return nil, err
		}
		entries = append(entries, JournalEntry{
			Index: i,
			Call:  record.Call,
			Outcome: JournalOutcome{
				Status:  record.Outcome.Kind(),
				Result:  record.Outcome.Result(),
				Message: record.Outcome.Message(),
			},
		})
	}
	return entries, nil
}

func (r *Runtime) Tasks(runID string) ([]TaskSnapshot, error) {
	r.mu.Lock()
	run := r.runs[runID]
	r.mu.Unlock()
	if run == nil {
		return nil, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	records, err := r.taskStore.List(context.Background(), r.tenantID, runID)
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
	acquired, err := r.stateStore.AcquireLease(
		context.Background(), r.tenantID, "task", taskID,
		r.instanceID, r.now().UTC(), time.Minute,
	)
	if err != nil {
		return TaskSnapshot{}, err
	}
	if !acquired {
		return TaskSnapshot{}, fmt.Errorf("%w: task is being resolved", ErrConflict)
	}
	defer r.stateStore.ReleaseLease(context.Background(), r.tenantID, "task", taskID, r.instanceID)

	sum := sha256.Sum256([]byte(token))
	record, err := r.taskStore.Resolve(
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
		if _, retryErr := r.Retry(record.Scope.RunID, RetryResume, nil); retryErr != nil {
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
	_ = r.stateStore.SaveRun(context.Background(), r.storedRunLocked(run))
	if thread := r.threads[run.threadID]; thread != nil {
		_ = r.stateStore.SaveThread(context.Background(), r.storedThreadLocked(thread))
	}
	r.mu.Unlock()
	if closeSession != nil {
		_ = closeSession.Close(context.Background())
	}
	r.publish(run.threadID, Event{Type: "run.updated", Data: snapshot})
	return snapshot, nil
}

func (r *Runtime) Retry(runID string, mode RetryMode, overrides []CapabilityConfig) (RunSnapshot, error) {
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
	if len(thread.runIDs) == 0 || thread.runIDs[len(thread.runIDs)-1] != run.id {
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: only the latest thread run can be retried", ErrConflict)
	}
	if thread.activeRunID != "" && thread.activeRunID != run.id {
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: thread already has active run %s", ErrConflict, thread.activeRunID)
	}

	if mode != RetryRestart && len(overrides) > 0 {
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: capability overrides require restart mode", ErrInvalid)
	}
	var replacementManifest *Manifest
	if len(overrides) > 0 {
		effective, err := EffectiveManifest(thread.manifest, overrides, r.dispatchers)
		if err != nil {
			r.mu.Unlock()
			return RunSnapshot{}, err
		}
		replacementManifest = &effective
	}
	if mode == RetryRestart {
		// A hard retry of a failed run forks just before the failing step so the
		// completed prefix is shared copy-on-write and only the failure onward is
		// re-executed. Any other restart (e.g. redoing a completed run) shares no
		// prefix and re-runs from the beginning.
		forkOffset := 0
		if run.status == RunFailed && run.failureOffset > 0 {
			forkOffset = run.failureOffset - 1
		}
		parent := r.runContextLocked(run)
		run.revision++
		child := r.runContextLocked(run)
		if err := r.stateStore.ForkJournal(context.Background(), parent, child, forkOffset); err != nil {
			r.mu.Unlock()
			return RunSnapshot{}, err
		}
		journal, journalErr := r.newJournal(run)
		if journalErr != nil {
			r.mu.Unlock()
			return RunSnapshot{}, journalErr
		}
		run.journal = journal
		run.preserveSession = false
		// Reuse the existing child subtree in spawn order (deep cascade resume).
		// Children whose spawn call is replayed from the shared prefix are skipped;
		// the cursor starts at the first child re-executed past the fork offset.
		run.cascade = true
		run.cascadeCursor = 0
		for _, off := range run.childSpawnOffsets {
			if off < forkOffset {
				run.cascadeCursor++
			}
		}
	} else {
		run.preserveSession = run.status == RunYielded || run.status == RunWaitingTask
		run.cascade = false
	}
	if replacementManifest != nil {
		run.effectiveManifest = *replacementManifest
	}
	run.status = RunQueued
	run.attempt++
	run.answer = ""
	run.err = ""
	run.failure = nil
	run.failureOffset = 0
	run.stopRequested = false
	run.startedAt = nil
	run.completedAt = nil
	run.updatedAt = r.now().UTC()
	thread.activeRunID = run.id
	thread.updatedAt = run.updatedAt
	if err := r.stateStore.SaveRun(context.Background(), r.storedRunLocked(run)); err != nil {
		r.mu.Unlock()
		return RunSnapshot{}, err
	}
	if err := r.stateStore.SaveThread(context.Background(), r.storedThreadLocked(thread)); err != nil {
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

func (r *Runtime) execute(runID string) {
	defer r.wg.Done()

	r.mu.Lock()
	run := r.runs[runID]
	if run == nil {
		r.mu.Unlock()
		return
	}
	if run.stopRequested {
		r.finishLocked(run, RunStopped, "", context.Canceled)
		snapshot := r.runSnapshotLocked(run)
		threadID := run.threadID
		r.mu.Unlock()
		r.publish(threadID, Event{Type: "run.updated", Data: snapshot})
		return
	}
	leaseResource := fmt.Sprintf("%s/%d", run.id, run.revision)
	acquired, leaseErr := r.stateStore.AcquireLease(
		context.Background(), r.tenantID, "run", leaseResource,
		r.instanceID, r.now().UTC(), r.leaseTTL,
	)
	if leaseErr != nil || !acquired {
		err := leaseErr
		if err == nil {
			err = errors.New("run is leased by another Aurora instance")
		}
		r.finishLocked(run, RunInterrupted, "", err)
		_ = r.stateStore.SaveRun(context.Background(), r.storedRunLocked(run))
		if thread := r.threads[run.threadID]; thread != nil {
			_ = r.stateStore.SaveThread(context.Background(), r.storedThreadLocked(thread))
		}
		snapshot := r.runSnapshotLocked(run)
		threadID := run.threadID
		r.mu.Unlock()
		r.publish(threadID, Event{Type: "run.updated", Data: snapshot})
		return
	}
	defer r.stateStore.ReleaseLease(
		context.Background(), r.tenantID, "run", leaseResource, r.instanceID,
	)
	session := run.session
	preserve := run.preserveSession && session != nil
	compute := r.computes[run.effectiveManifest.Brain]
	run.preserveSession = false
	r.mu.Unlock()
	if compute == nil {
		r.finish(runID, RunFailed, "", fmt.Errorf("brain %q is unavailable", run.effectiveManifest.Brain))
		return
	}

	if !preserve {
		var err error
		if session != nil {
			_ = session.Close(context.Background())
		}
		runCtx := r.runContext(run)
		sessionDispatcher, err := r.dispatcherFactory.NewDispatcher(context.Background(), runCtx)
		if err != nil {
			r.finish(runID, RunFailed, "", err)
			return
		}
		session, err = compute.CreateSession(context.Background(), capcompute.PlayRequest[string, RunKey]{
			Entrypoint: "run",
			UserData:   runCtx,
			Dispatcher: sessionDispatcher,
		})
		// The guest fetches its input via the agent.input host call (served by the
		// lifecycle dispatcher), so no entrypoint input is supplied here.
		if err == nil {
			err = r.sessionStore.SaveSession(context.Background(), session.GuestData.SessionKey(), session)
		}
		if err != nil {
			r.finish(runID, RunFailed, "", err)
			return
		}
	}

	r.mu.Lock()
	run = r.runs[runID]
	if run.stopRequested {
		if !preserve {
			_ = session.Close(context.Background())
		}
		r.finishLocked(run, RunStopped, "", context.Canceled)
		snapshot := r.runSnapshotLocked(run)
		threadID := run.threadID
		r.mu.Unlock()
		r.publish(threadID, Event{Type: "run.updated", Data: snapshot})
		return
	}
	now := r.now().UTC()
	run.session = session
	run.status = RunRunning
	run.startedAt = &now
	run.updatedAt = now
	snapshot := r.runSnapshotLocked(run)
	threadID := run.threadID
	_ = r.stateStore.SaveRun(context.Background(), r.storedRunLocked(run))
	r.mu.Unlock()
	r.publish(threadID, Event{Type: "run.updated", Data: snapshot})

	handle, err := compute.Play(context.Background(), session)
	if err != nil {
		r.finish(runID, RunFailed, "", err)
		return
	}
	r.mu.Lock()
	run = r.runs[runID]
	run.handle = handle
	stopRequested := run.stopRequested
	r.mu.Unlock()
	if stopRequested {
		handle.Stop()
	}

	result := <-handle.Results()
	r.mu.Lock()
	forced := r.runs[runID].failure
	r.mu.Unlock()
	if forced != nil {
		r.finish(runID, RunFailed, "", forced)
		return
	}
	switch result.Status {
	case capcompute.PlayCompleted:
		answer, err := r.answerFromJournal(runID)
		if err != nil {
			r.finish(runID, RunFailed, "", err)
			return
		}
		r.finish(runID, RunCompleted, answer, nil)
	case capcompute.PlayYielded:
		tasks, taskErr := r.taskStore.List(context.Background(), r.tenantID, runID)
		if taskErr == nil && hasPendingTask(tasks) {
			r.finish(runID, RunWaitingTask, "", nil)
		} else {
			r.finish(runID, RunYielded, "", taskErr)
		}
	case capcompute.PlayStopped:
		r.mu.Lock()
		closing := r.closed
		r.mu.Unlock()
		if closing {
			r.finish(runID, RunInterrupted, "", result.Err)
		} else {
			r.finish(runID, RunStopped, "", result.Err)
		}
	default:
		r.finish(runID, RunFailed, "", result.Err)
	}
}

// requestRunFailure marks a run to finish as failed and stops its in-flight play.
// It is used to propagate a delegated child's failure up to its parent run when
// the child's failure-mode policy is OnFailurePropagate.
func (r *Runtime) requestRunFailure(runID string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	run := r.runs[runID]
	if run == nil {
		return
	}
	if run.failure == nil {
		run.failure = err
	}
	if run.handle != nil {
		run.handle.Stop()
	}
}

func (r *Runtime) finish(runID string, status RunStatus, answer string, err error) {
	r.mu.Lock()
	run := r.runs[runID]
	if run == nil {
		r.mu.Unlock()
		return
	}
	r.finishLocked(run, status, answer, err)
	_ = r.stateStore.SaveRun(context.Background(), r.storedRunLocked(run))
	if thread := r.threads[run.threadID]; thread != nil {
		// Conversation history is no longer persisted separately; it is derived
		// from the thread's completed runs (each run stores its message + answer)
		// and rebuilt on recovery.
		_ = r.stateStore.SaveThread(context.Background(), r.storedThreadLocked(thread))
	}
	snapshot := r.runSnapshotLocked(run)
	threadID := run.threadID
	r.mu.Unlock()
	r.publish(threadID, Event{Type: "run.updated", Data: snapshot})
}

func (r *Runtime) finishLocked(run *runState, status RunStatus, answer string, err error) {
	now := r.now().UTC()
	run.status = status
	run.answer = answer
	run.updatedAt = now
	run.completedAt = &now
	run.handle = nil
	if err != nil {
		run.err = err.Error()
	} else {
		run.err = ""
	}
	if status == RunFailed && run.journal != nil {
		// Record where the run stopped so a hard retry can fork just before the
		// failing step instead of re-running from the beginning.
		run.failureOffset = run.journal.Length()
	}
	thread := r.threads[run.threadID]
	if thread != nil {
		if status != RunYielded && status != RunWaitingTask && thread.activeRunID == run.id {
			thread.activeRunID = ""
		}
		thread.updatedAt = now
		if status == RunCompleted {
			thread.history = append(thread.history,
				HistoryMessage{Role: "user", Content: run.message},
				HistoryMessage{Role: "assistant", Content: answer},
			)
		}
	}
}

func (r *Runtime) newJournal(run *runState) (journaled.Journal, error) {
	journal, err := r.stateStore.OpenJournal(context.Background(), r.runContextLocked(run))
	if err != nil {
		return nil, err
	}
	return &observableJournal{
		Journal: journal,
		onStore: func(index int, call dispatcher.Call, outcome dispatcher.Outcome) {
			r.publish(run.threadID, Event{
				Type: "journal.appended",
				Data: JournalEvent{
					RunID:         run.id,
					Index:         index,
					Call:          call.Name,
					OutcomeStatus: outcome.Kind(),
					OutcomeSize:   len(outcome.Result()),
				},
			})
		},
	}, nil
}

func (r *Runtime) publish(threadID string, event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ch := range r.subscribers[threadID] {
		select {
		case ch <- event:
		default:
		}
	}
}

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

func (r *Runtime) restore(ctx context.Context) error {
	state, err := r.stateStore.Load(ctx, r.tenantID)
	if err != nil {
		return err
	}
	for _, stored := range state.Threads {
		if stored.Manifest.Brain == "" {
			stored.Manifest.Brain = r.brains.DefaultID()
		}
		stored.Manifest, err = ValidateManifest(stored.Manifest, r.dispatchers)
		if err != nil {
			return err
		}
		if _, err := r.brains.Resolve(stored.Manifest.Brain); err != nil {
			return err
		}
		thread := &threadState{
			id:          stored.ID,
			title:       stored.Title,
			createdAt:   stored.CreatedAt,
			updatedAt:   stored.UpdatedAt,
			activeRunID: stored.ActiveRunID,
			manifest:    cloneManifest(stored.Manifest),
		}
		r.threads[thread.id] = thread
	}
	// Conversation history is derived from completed runs (each stores its
	// message + answer), accumulated below in run order, rather than loaded from a
	// separate message store.
	sort.Slice(state.Runs, func(i, j int) bool {
		return state.Runs[i].CreatedAt.Before(state.Runs[j].CreatedAt)
	})
	for _, stored := range state.Runs {
		if stored.EffectiveManifest.Brain == "" {
			stored.EffectiveManifest.Brain = r.brains.DefaultID()
		}
		stored.EffectiveManifest, err = ValidateManifest(stored.EffectiveManifest, r.dispatchers)
		if err != nil {
			return err
		}
		if _, err := r.brains.Resolve(stored.EffectiveManifest.Brain); err != nil {
			return err
		}
		brain, err := r.brains.Resolve(stored.EffectiveManifest.Brain)
		if err != nil {
			return err
		}
		if stored.BrainDigest != "" && stored.BrainDigest != brain.Digest {
			slog.Info("skipping run with outdated brain digest",
				"run_id", stored.ID, "brain", brain.ID,
				"stored_digest", stored.BrainDigest, "current_digest", brain.Digest)
			continue
		}
		status := stored.Status
		if status == RunQueued || status == RunRunning || status == RunStopping {
			status = RunInterrupted
		}
		run := &runState{
			id:                stored.ID,
			threadID:          stored.ThreadID,
			message:           stored.Message,
			status:            status,
			attempt:           stored.Attempt,
			revision:          stored.Revision,
			createdAt:         stored.CreatedAt,
			updatedAt:         stored.UpdatedAt,
			startedAt:         copyTime(stored.StartedAt),
			completedAt:       copyTime(stored.CompletedAt),
			answer:            stored.Answer,
			err:               stored.Error,
			effectiveManifest: cloneManifest(stored.EffectiveManifest),
			brainDigest:       brain.Digest,
			parentRunID:       stored.ParentRunID,
			childRunIDs:       append([]string(nil), stored.ChildRunIDs...),
			childSpawnOffsets: append([]int(nil), stored.ChildSpawnOffsets...),
			failureOffset:     stored.FailureOffset,
		}
		if run.revision == 0 {
			run.revision = 1
		}
		run.journal, err = r.newJournal(run)
		if err != nil {
			return err
		}
		r.runs[run.id] = run
		if thread := r.threads[run.threadID]; thread != nil {
			run.history = append([]HistoryMessage(nil), thread.history...)
			thread.runIDs = append(thread.runIDs, run.id)
			if run.status == RunCompleted {
				thread.history = append(thread.history,
					HistoryMessage{Role: "user", Content: run.message},
					HistoryMessage{Role: "assistant", Content: run.answer},
				)
			}
		}
		if status != stored.Status {
			if err := r.stateStore.SaveRun(ctx, r.storedRunLocked(run)); err != nil {
				return err
			}
		}
	}
	for _, thread := range r.threads {
		if thread.activeRunID != "" && r.runs[thread.activeRunID] == nil {
			slog.Info("clearing active run from thread due to brain digest mismatch",
				"thread_id", thread.id, "run_id", thread.activeRunID)
			thread.activeRunID = ""
		}
	}
	return nil
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

type observableJournal struct {
	journaled.Journal
	onStore func(index int, call dispatcher.Call, outcome dispatcher.Outcome)
}

func (j *observableJournal) Store(index int, call dispatcher.Call, outcome dispatcher.Outcome) error {
	if err := j.Journal.Store(index, call, outcome); err != nil {
		return err
	}
	if j.onStore != nil {
		j.onStore(index, call, outcome)
	}
	return nil
}
