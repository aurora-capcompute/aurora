package agent

import (
	"capcompute/dispatcher"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type delegationRouter struct {
	next     dispatcher.Dispatcher[RunContext]
	children map[string]delegationChild
}

type delegationChild struct {
	manifest ChildManifest
	runtime  *Runtime
}

type delegateArgs struct {
	Message      string `json:"message"`
	SystemPrompt string `json:"system_prompt,omitempty"`
}

type delegateResult struct {
	Answer string `json:"answer"`
}

func newDelegationRouter(next dispatcher.Dispatcher[RunContext], children []ChildManifest, runtime *Runtime) *delegationRouter {
	m := make(map[string]delegationChild, len(children))
	for _, child := range children {
		m[child.Name] = delegationChild{
			manifest: child,
			runtime:  runtime,
		}
	}
	return &delegationRouter{next: next, children: m}
}

func (r *delegationRouter) Dispatch(ctx context.Context, key RunContext, call dispatcher.Call) (dispatcher.Outcome, error) {
	if strings.HasPrefix(call.Name, "call.") {
		childName := call.Name[len("call."):]
		child, ok := r.children[childName]
		if ok {
			return child.dispatch(ctx, key, call)
		}
	}
	return r.next.Dispatch(ctx, key, call)
}

func (r *delegationRouter) Capabilities() []dispatcher.Capability {
	caps := dispatcher.Capabilities(r.next)
	for name, child := range r.children {
		caps = append(caps, delegationCapability(name, child.manifest))
	}
	return caps
}

// onChildFailure applies the child's failure-mode policy. OnFailurePropagate
// forces the parent run to fail (a dispatcher error alone only surfaces a
// recoverable observation to the brain); otherwise the failure is reported to
// the parent brain as a recoverable failed observation.
func (c *delegationChild) onChildFailure(parentRunID string, err error) (dispatcher.Outcome, error) {
	if c.manifest.OnFailure == OnFailurePropagate {
		c.runtime.requestRunFailure(parentRunID, fmt.Errorf("child %q failed: %w", c.manifest.Name, err))
	}
	return dispatcher.Failed(err.Error()), nil
}

func (c *delegationChild) dispatch(ctx context.Context, parent RunContext, call dispatcher.Call) (dispatcher.Outcome, error) {
	slog.Info("delegation: dispatch started", "child", c.manifest.Name)

	var args delegateArgs
	if err := json.Unmarshal(call.Args, &args); err != nil {
		return dispatcher.Failed(fmt.Sprintf("decode delegation args: %v", err)), nil
	}

	// Deep cascade resume: when the parent run is being restarted, re-execution
	// re-issues the same deterministic sequence of delegation calls. Rather than
	// spawning a fresh child each time, reuse the existing child run recorded at
	// this position (in spawn order) and retry it, which recursively cascades the
	// restart down its own subtree.
	if childID, threadID, ok := c.runtime.nextCascadeChild(parent.RunID); ok {
		slog.Info("delegation: cascade retry", "child", c.manifest.Name, "run_id", childID)
		if _, err := c.runtime.Retry(childID, RetryRestart, nil); err != nil {
			return dispatcher.Failed(fmt.Sprintf("cascade retry child: %v", err)), nil
		}
		answer, err := c.runtime.waitForCompletion(ctx, childID, threadID)
		if err != nil {
			return c.onChildFailure(parent.RunID, err)
		}
		result, marshalErr := json.Marshal(delegateResult{Answer: answer})
		if marshalErr != nil {
			return dispatcher.Outcome{}, marshalErr
		}
		return dispatcher.Result(result), nil
	}
	slog.Info("delegation: creating child", "child", c.manifest.Name, "message_len", len(args.Message))

	childManifest := buildChildManifest(c.manifest, args.SystemPrompt)
	slog.Info("delegation: child manifest built", "brain", childManifest.Brain, "caps", len(childManifest.Capabilities))

	thread, err := c.runtime.CreateThread(childManifest)
	if err != nil {
		slog.Error("delegation: create thread failed", "child", c.manifest.Name, "error", err)
		return dispatcher.Failed(fmt.Sprintf("create child thread: %v", err)), nil
	}
	slog.Info("delegation: thread created", "child", c.manifest.Name, "thread_id", thread.ID)

	run, err := c.runtime.createChildRun(parent.RunID, thread.ID, args.Message)
	if err != nil {
		slog.Error("delegation: create run failed", "child", c.manifest.Name, "error", err)
		return dispatcher.Failed(fmt.Sprintf("create child run: %v", err)), nil
	}
	slog.Info("delegation: run created, waiting for completion", "child", c.manifest.Name, "run_id", run.ID)

	answer, err := c.runtime.waitForCompletion(ctx, run.ID, thread.ID)
	if err != nil {
		slog.Error("delegation: wait failed", "child", c.manifest.Name, "run_id", run.ID, "error", err)
		return c.onChildFailure(parent.RunID, err)
	}
	slog.Info("delegation: completed", "child", c.manifest.Name, "answer_len", len(answer))

	result, marshalErr := json.Marshal(delegateResult{Answer: answer})
	if marshalErr != nil {
		return dispatcher.Outcome{}, marshalErr
	}
	return dispatcher.Result(result), nil
}

func buildChildManifest(child ChildManifest, systemPromptOverride string) Manifest {
	prompt := child.SystemPrompt
	if systemPromptOverride != "" {
		prompt = systemPromptOverride
	}
	caps := make([]CapabilityConfig, len(child.Capabilities))
	for i, cap := range child.Capabilities {
		caps[i] = CapabilityConfig{
			Name:     cap.Name,
			Settings: append(json.RawMessage(nil), cap.Settings...),
		}
	}
	var children []ChildManifest
	if len(child.Children) > 0 {
		children = make([]ChildManifest, len(child.Children))
		copy(children, child.Children)
	}
	return Manifest{
		Version:      ManifestVersion,
		Brain:        child.Brain,
		SystemPrompt: prompt,
		Capabilities: caps,
		Children:     children,
	}
}

func settingsRequireApproval(settings json.RawMessage) bool {
	if len(settings) == 0 {
		return false
	}
	var parsed struct {
		RequireApproval *bool `json:"require_approval"`
	}
	if json.Unmarshal(settings, &parsed) != nil {
		return false
	}
	return parsed.RequireApproval != nil && *parsed.RequireApproval
}

func delegationCapability(name string, child ChildManifest) dispatcher.Capability {
	var desc strings.Builder
	desc.WriteString("Delegate work to the ")
	desc.WriteString(name)
	desc.WriteString(" brain.")
	if len(child.Capabilities) > 0 {
		desc.WriteString(" It can: ")
		for i, cap := range child.Capabilities {
			if i > 0 {
				desc.WriteString(", ")
			}
			desc.WriteString(cap.Name)
		}
		desc.WriteString(".")
	} else {
		desc.WriteString(" Pure computation brain, no external capabilities.")
	}
	return dispatcher.Capability{
		Name:        "call." + name,
		Description: desc.String(),
		InputSchema: json.RawMessage(`{"type":"object","properties":{"message":{"type":"string","description":"Task description for the child brain"},"system_prompt":{"type":"string","description":"Optional system prompt override"}},"required":["message"],"additionalProperties":false}`),
	}
}

// nextCascadeChild returns the next existing child run to reuse when a parent run
// is being restarted with cascade enabled, advancing through the parent's children
// in spawn order. It returns ok=false once cascade is off or the recorded children
// are exhausted, in which case the caller spawns a fresh child.
func (r *Runtime) nextCascadeChild(parentRunID string) (childID, threadID string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	parent := r.runs[parentRunID]
	if parent == nil || !parent.cascade || parent.cascadeCursor >= len(parent.childRunIDs) {
		return "", "", false
	}
	childID = parent.childRunIDs[parent.cascadeCursor]
	parent.cascadeCursor++
	child := r.runs[childID]
	if child == nil {
		// Recorded child is no longer resident; fall back to spawning fresh.
		return "", "", false
	}
	return childID, child.threadID, true
}

func (r *Runtime) createChildRun(parentRunID string, threadID string, message string) (RunSnapshot, error) {
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
	effectiveManifest, err := EffectiveManifest(thread.manifest, nil, r.dispatchers)
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
		parentRunID:       parentRunID,
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
	if parent := r.runs[parentRunID]; parent != nil {
		spawnOffset := 0
		if parent.journal != nil {
			// The position this call.<child> occupies in the parent journal; it is
			// recorded once the dispatch returns.
			spawnOffset = parent.journal.Length()
		}
		parent.childRunIDs = append(parent.childRunIDs, runID)
		parent.childSpawnOffsets = append(parent.childSpawnOffsets, spawnOffset)
		_ = r.stateStore.SaveRun(context.Background(), r.storedRunLocked(parent))
	}
	snapshot := r.runSnapshotLocked(run)
	r.mu.Unlock()

	r.publish(threadID, Event{Type: "run.updated", Data: snapshot})
	r.wg.Add(1)
	go r.execute(runID)
	return snapshot, nil
}

func (r *Runtime) waitForCompletion(ctx context.Context, runID, threadID string) (string, error) {
	slog.Info("waitForCompletion: subscribing", "run_id", runID, "thread_id", threadID)
	_, events, unsubscribe, err := r.Subscribe(threadID)
	if err != nil {
		return "", fmt.Errorf("subscribe to child thread: %w", err)
	}
	defer unsubscribe()

	if snapshot, err := r.GetRun(runID); err == nil {
		slog.Info("waitForCompletion: initial status", "run_id", runID, "status", snapshot.Status)
		switch snapshot.Status {
		case RunCompleted:
			return snapshot.Answer, nil
		case RunFailed:
			return "", fmt.Errorf("child run failed: %s", snapshot.Error)
		case RunStopped:
			return "", fmt.Errorf("child run stopped")
		case RunInterrupted:
			return "", fmt.Errorf("child run interrupted")
		}
	} else {
		slog.Error("waitForCompletion: GetRun failed", "run_id", runID, "error", err)
	}

	timeout := 5 * time.Minute
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	slog.Info("waitForCompletion: entering event loop", "run_id", runID, "timeout", timeout)
	for {
		select {
		case <-ctx.Done():
			slog.Warn("waitForCompletion: context cancelled", "run_id", runID)
			_, _ = r.Stop(runID)
			return "", ctx.Err()
		case <-timer.C:
			slog.Warn("waitForCompletion: timed out", "run_id", runID)
			_, _ = r.Stop(runID)
			return "", fmt.Errorf("child run timed out after %s", timeout)
		case event, ok := <-events:
			if !ok {
				slog.Warn("waitForCompletion: event stream closed", "run_id", runID)
				return "", fmt.Errorf("child event stream closed")
			}
			slog.Info("waitForCompletion: event received", "run_id", runID, "type", event.Type)
			if event.Type != "run.updated" {
				continue
			}
			snapshot, ok := event.Data.(RunSnapshot)
			if !ok {
				slog.Warn("waitForCompletion: event data not RunSnapshot", "run_id", runID, "data_type", fmt.Sprintf("%T", event.Data))
				continue
			}
			if snapshot.ID != runID {
				continue
			}
			slog.Info("waitForCompletion: run status update", "run_id", runID, "status", snapshot.Status, "error", snapshot.Error)
			switch snapshot.Status {
			case RunCompleted:
				return snapshot.Answer, nil
			case RunFailed:
				return "", fmt.Errorf("child run failed: %s", snapshot.Error)
			case RunStopped:
				return "", fmt.Errorf("child run stopped")
			case RunInterrupted:
				return "", fmt.Errorf("child run interrupted")
			}
		}
	}
}
