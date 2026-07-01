package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aurora-capcompute/capcompute/dispatcher"
)

// agentRouter is the dispatcher for `core.agent` tools. It runs each sub-agent
// as a tracked child run of the runtime; its Dispatch forwards a call to that
// run and returns the child's answer (or propagates a yield for HITL). It sits
// in the runtime dispatcher stack where the leaf tools cannot reach the runtime.
type agentRouter struct {
	next     dispatcher.Dispatcher[RunContext]
	children map[string]agentChild
}

type agentChild struct {
	tool     Tool
	settings AgentSettings
	runtime  *Runtime
}

type delegateArgs struct {
	Message      string `json:"message"`
	SystemPrompt string `json:"system_prompt,omitempty"`
}

type delegateResult struct {
	Answer string `json:"answer"`
}

func newAgentRouter(next dispatcher.Dispatcher[RunContext], agents []Tool, runtime *Runtime) (*agentRouter, error) {
	m := make(map[string]agentChild, len(agents))
	for _, tool := range agents {
		settings, err := decodeAgentSettings(tool)
		if err != nil {
			return nil, fmt.Errorf("agent tool %q settings: %w", tool.Name, err)
		}
		m[tool.Name] = agentChild{tool: tool, settings: settings, runtime: runtime}
	}
	return &agentRouter{next: next, children: m}, nil
}

func (r *agentRouter) Dispatch(ctx context.Context, key RunContext, call dispatcher.Call, auth dispatcher.Authorization) (dispatcher.Outcome, error) {
	if child, ok := r.children[call.Name]; ok {
		return child.dispatch(ctx, key, call)
	}
	return r.next.Dispatch(ctx, key, call, auth)
}

func (r *agentRouter) Capabilities() []dispatcher.Capability {
	caps := r.next.Capabilities()
	for name, child := range r.children {
		caps = append(caps, agentCapability(name, child))
	}
	return caps
}

// onChildFailure applies the child's failure-mode policy. OnFailurePropagate
// forces the parent run to fail (a dispatcher error alone only surfaces a
// recoverable observation to the brain); otherwise the failure is reported to
// the parent brain as a recoverable failed observation.
func (c *agentChild) onChildFailure(parentRunID string, err error) (dispatcher.Outcome, error) {
	if c.settings.OnFailure == OnFailurePropagate {
		c.runtime.requestRunFailure(parentRunID, fmt.Errorf("child %q failed: %w", c.tool.Name, err))
	}
	return dispatcher.Fail(err.Error()), nil
}

func (c *agentChild) dispatch(ctx context.Context, parent RunContext, call dispatcher.Call) (dispatcher.Outcome, error) {
	var args delegateArgs
	if err := json.Unmarshal(call.Args, &args); err != nil {
		return dispatcher.Fail(fmt.Sprintf("decode delegation args: %v", err)), nil
	}

	// Deep cascade resume: when the parent run is being restarted (or re-driven
	// after a child's HITL approval), re-execution re-issues the same deterministic
	// sequence of delegation calls. Rather than spawning a fresh child each time,
	// reuse the existing child run recorded at this position (in spawn order).
	if childID, threadID, cascadeMode, reuse, ok := c.runtime.nextCascadeChild(parent.RunID); ok {
		if reuse {
			// HITL reconnect: the child already finished (e.g. after its approval was
			// resolved while the parent was suspended). Reuse its terminal result
			// directly instead of re-running it, which would fork a new revision and
			// re-create the child's approval task.
			snap, err := c.runtime.GetRun(childID)
			if err != nil {
				return dispatcher.Fail(fmt.Sprintf("reconnect child: %v", err)), nil
			}
			answer, _, runErr := childTerminal(snap)
			if runErr != nil {
				return c.onChildFailure(parent.RunID, runErr)
			}
			return delegationResult(answer)
		}
		if _, err := c.runtime.Retry(childID, cascadeMode); err != nil {
			return dispatcher.Fail(fmt.Sprintf("cascade retry child: %v", err)), nil
		}
		answer, parked, err := c.runtime.waitForCompletion(ctx, childID, threadID)
		if err != nil {
			return c.onChildFailure(parent.RunID, err)
		}
		if parked {
			return dispatcher.Yield(fmt.Sprintf("waiting on child %s", c.tool.Name)), nil
		}
		return delegationResult(answer)
	}

	childManifest := buildChildManifest(c.tool, c.settings, args.SystemPrompt)
	slog.Info("spawning child run in parent thread", "parent_run", parent.RunID, "child", c.tool.Name)
	run, err := c.runtime.createChildRun(parent.RunID, parent.ThreadID, args.Message, childManifest)
	if err != nil {
		return dispatcher.Fail(fmt.Sprintf("create child run: %v", err)), nil
	}
	answer, parked, err := c.runtime.waitForCompletion(ctx, run.ID, parent.ThreadID)
	if err != nil {
		return c.onChildFailure(parent.RunID, err)
	}
	if parked {
		// The child parked for human approval. Yield so the parent run suspends
		// durably; the child→parent finish hook re-drives this call once the child
		// finishes, and the reconnect branch above returns its answer.
		return dispatcher.Yield(fmt.Sprintf("waiting on child %s", c.tool.Name)), nil
	}
	return delegationResult(answer)
}

// delegationResult marshals a child's answer into the delegate result envelope.
func delegationResult(answer string) (dispatcher.Outcome, error) {
	result, err := json.Marshal(delegateResult{Answer: answer})
	if err != nil {
		return dispatcher.Outcome{}, err
	}
	return dispatcher.Result(result), nil
}

// buildChildManifest lifts a `core.agent` tool node into a Manifest for the child
// run: brain/system_prompt come from the tool's AgentSettings, composition from
// its nested Tools.
func buildChildManifest(tool Tool, settings AgentSettings, systemPromptOverride string) Manifest {
	prompt := settings.SystemPrompt
	if systemPromptOverride != "" {
		prompt = systemPromptOverride
	}
	return Manifest{
		Version:      ManifestVersion,
		Name:         tool.Name,
		Brain:        settings.Code,
		BindingRef:   settings.BindingRef,
		SystemPrompt: prompt,
		OnFailure:    settings.OnFailure,
		Tools:        cloneTools(tool.Tools),
	}
}

func agentCapability(name string, child agentChild) dispatcher.Capability {
	var desc strings.Builder
	desc.WriteString("Delegate work to the ")
	desc.WriteString(name)
	desc.WriteString(" agent.")
	visible := make([]string, 0, len(child.tool.Tools))
	for _, t := range child.tool.Tools {
		if !t.Hidden {
			visible = append(visible, t.Name)
		}
	}
	if len(visible) > 0 {
		desc.WriteString(" It can: ")
		desc.WriteString(strings.Join(visible, ", "))
		desc.WriteString(".")
	} else {
		desc.WriteString(" Pure computation agent, no external tools.")
	}
	return dispatcher.Capability{
		Name:        name,
		Description: desc.String(),
		InputSchema: json.RawMessage(`{"type":"object","properties":{"message":{"type":"string","description":"Task description for the child agent"},"system_prompt":{"type":"string","description":"Optional system prompt override"}},"required":["message"],"additionalProperties":false}`),
		Hidden:      child.tool.Hidden,
	}
}

// nextCascadeChild returns the next existing child run to reuse when a parent run
// is being retried with cascade enabled, advancing through the parent's children
// in spawn order. It returns ok=false once cascade is off or the recorded children
// are exhausted, in which case the caller spawns a fresh child.
//
// The returned cascadeMode is the effective retry mode to use on the child:
// it mirrors the parent's cascadeMode except that completed children are always
// restarted (RetryResume is invalid for completed runs).
func (r *Runtime) nextCascadeChild(parentRunID string) (childID, threadID string, cascadeMode RetryMode, reuse, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	parent := r.runs[parentRunID]
	if parent == nil || !parent.cascade || parent.cascadeCursor >= len(parent.childRunIDs) {
		if parent != nil {
			slog.Debug("cascade skip: off or exhausted",
				"run", parentRunID,
				"cascade", parent.cascade,
				"cursor", parent.cascadeCursor,
				"children", len(parent.childRunIDs),
			)
		}
		return "", "", "", false, false
	}
	childID = parent.childRunIDs[parent.cascadeCursor]
	parent.cascadeCursor++
	child := r.runs[childID]
	if child == nil {
		slog.Warn("cascade child not resident; spawning fresh",
			"parent_run", parentRunID,
			"child_run", childID,
			"cursor", parent.cascadeCursor-1,
		)
		return "", "", "", false, false
	}
	// HITL reconnect: a parent re-driven after a child finished its approval reuses
	// the child's terminal result directly. Re-running it would fork a new revision
	// and, for a HITL child, re-create the now-resolved approval task.
	if parent.reconnectChildren && isTerminal(child.status) {
		return childID, child.threadID, parent.cascadeMode, true, true
	}
	// A resume-mode cascade should also resume the child so only the failed step
	// gets a new revision. Completed children cannot be resumed, so fall back to
	// restart in that case.
	mode := parent.cascadeMode
	if mode == RetryResume && child.status == RunCompleted {
		mode = RetryRestart
	}
	return childID, child.threadID, mode, false, true
}

func (r *Runtime) createChildRun(parentRunID string, threadID string, message string, manifest Manifest) (RunSnapshot, error) {
	if message == "" {
		return RunSnapshot{}, fmt.Errorf("%w: message is required", ErrInvalid)
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
	if thread.activeRunID != "" && thread.activeRunID != parentRunID {
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
		parentRunID: parentRunID,
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
	prevActiveRunID := thread.activeRunID
	thread.activeRunID = runID
	thread.updatedAt = now
	if err := r.appendRun(run); err != nil {
		delete(r.runs, runID)
		thread.runIDs = thread.runIDs[:len(thread.runIDs)-1]
		thread.activeRunID = prevActiveRunID
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
		_ = r.appendRun(parent)
	}
	snapshot := r.runSnapshotLocked(run)
	r.mu.Unlock()

	r.publish(threadID, Event{Type: "run.updated", Data: snapshot})
	r.wg.Add(1)
	go r.execute(runID)
	return snapshot, nil
}

// waitForCompletion blocks until the child reaches a terminal state or parks
// awaiting its own out-of-band approval. parked=true means the caller should
// yield (suspend the parent durably) rather than treat the result as final;
// there is deliberately no timeout, since a human approval may take arbitrarily
// long. ctx cancellation (shutdown/stop) still stops the child.
func (r *Runtime) waitForCompletion(ctx context.Context, runID, threadID string) (answer string, parked bool, err error) {
	_, events, unsubscribe, err := r.Subscribe(threadID)
	if err != nil {
		return "", false, fmt.Errorf("subscribe to child thread: %w", err)
	}
	defer unsubscribe()

	if snapshot, err := r.GetRun(runID); err == nil {
		if ans, done, runErr := childTerminal(snapshot); done {
			return ans, false, runErr
		}
		if childParked(snapshot) {
			return "", true, nil
		}
	}

	for {
		select {
		case <-ctx.Done():
			_, _ = r.Stop(runID)
			return "", false, ctx.Err()
		case event, ok := <-events:
			if !ok {
				return "", false, fmt.Errorf("child event stream closed")
			}
			if event.Type != "run.updated" {
				continue
			}
			snapshot, ok := event.Data.(RunSnapshot)
			if !ok || snapshot.ID != runID {
				continue
			}
			if ans, done, runErr := childTerminal(snapshot); done {
				return ans, false, runErr
			}
			if childParked(snapshot) {
				return "", true, nil
			}
		}
	}
}

// childTerminal reports whether a child run snapshot has reached a terminal state,
// returning its answer (on completion) or the corresponding error. done is false
// while the run is still in flight.
func childTerminal(snapshot RunSnapshot) (answer string, done bool, err error) {
	switch snapshot.Status {
	case RunCompleted:
		return snapshot.Answer, true, nil
	case RunFailed:
		return "", true, fmt.Errorf("child run failed: %s", snapshot.Error)
	case RunStopped:
		return "", true, fmt.Errorf("child run stopped")
	case RunInterrupted:
		return "", true, fmt.Errorf("child run interrupted")
	default:
		return "", false, nil
	}
}

// childParked reports whether a child run is durably suspended awaiting out-of-band
// resolution (its own HITL approval) rather than terminal or still in flight.
func childParked(s RunSnapshot) bool {
	return s.Status == RunWaitingTask || s.Status == RunYielded
}

// resumeParentIfWaiting re-drives a parent that suspended on a delegated child's
// HITL approval, once that child has reached a terminal state. It is a no-op when
// the parent is not parked — e.g. a parent still actively blocked in a synchronous
// delegation call observes the child's completion through its own subscription.
// The parent is re-driven via fork+replay (reconnectChildren) so the un-recorded
// delegation call re-executes and reconnects to the finished child.
func (r *Runtime) resumeParentIfWaiting(parentRunID string) {
	r.mu.Lock()
	parent := r.runs[parentRunID]
	resumable := parent != nil && (parent.status == RunYielded || parent.status == RunWaitingTask)
	if resumable {
		parent.reconnectChildren = true
	}
	r.mu.Unlock()
	if !resumable {
		return
	}
	if _, err := r.Retry(parentRunID, RetryResume); err != nil {
		slog.Warn("resume parent on child completion failed", "parent", parentRunID, "err", err)
	}
}

// isTerminal reports whether a run status is a final state (no further execution).
func isTerminal(status RunStatus) bool {
	switch status {
	case RunCompleted, RunFailed, RunStopped, RunInterrupted:
		return true
	default:
		return false
	}
}
