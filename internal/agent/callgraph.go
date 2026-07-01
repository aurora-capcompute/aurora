package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aurora-capcompute/aurora-capcompute/internal/eventlog"
)

// ThreadGraphRun is a run within a thread graph: its metadata and the flat
// journal of every (position, revision) entry ever written. The fork structure
// is derivable from duplicate positions with different revision numbers.
type ThreadGraphRun struct {
	RunID           string         `json:"run_id"`
	Message         string         `json:"message"`
	ParentRunID     string         `json:"parent_run_id,omitempty"`
	Status          RunStatus      `json:"status"`
	Answer          string         `json:"answer,omitempty"`
	Error           string         `json:"error,omitempty"`
	Attempt         int            `json:"attempt"`
	CurrentRevision uint64         `json:"current_revision"`
	ChildRunIDs     []string       `json:"child_run_ids,omitempty"`
	Entries         []JournalEntry `json:"entries"`
}

// ThreadGraph projects a whole thread for exploration: its runs in order, each
// with its complete flat entry history across all revisions.
type ThreadGraph struct {
	ThreadID string           `json:"thread_id"`
	Title    string           `json:"title"`
	Runs     []ThreadGraphRun `json:"runs"`
}

// ThreadGraph builds the execution graph of a thread by reading each run's
// capability.recorded events directly from the log. The flat entry list carries
// per-entry revision numbers so the caller can reconstruct the fork graph.
func (r *Runtime) ThreadGraph(threadID string) (ThreadGraph, error) {
	r.mu.Lock()
	thread := r.threads[threadID]
	if thread == nil {
		r.mu.Unlock()
		return ThreadGraph{}, fmt.Errorf("%w: thread %s", ErrNotFound, threadID)
	}
	graph := ThreadGraph{ThreadID: thread.id, Title: thread.title}
	type runMeta struct {
		id, message, parentRunID string
		status                   RunStatus
		answer, err              string
		attempt                  int
		currentRevision          uint64
		childRunIDs              []string
	}
	metas := make([]runMeta, 0, len(thread.runIDs))
	for _, runID := range thread.runIDs {
		run := r.runs[runID]
		if run == nil {
			continue
		}
		metas = append(metas, runMeta{
			id: run.id, message: run.message, parentRunID: run.parentRunID,
			status: run.status, answer: run.answer, err: run.err,
			attempt: run.attempt, currentRevision: run.revision,
			childRunIDs: append([]string(nil), run.childRunIDs...),
		})
	}
	tenantID := r.tenantID
	r.mu.Unlock()

	ctx := context.Background()
	events, err := r.log.Read(ctx, eventlog.Scope{TenantID: tenantID, ThreadID: threadID}, 0)
	if err != nil {
		return ThreadGraph{}, err
	}

	// Build a flat entry list per run. Duplicates (same position + revision)
	// are last-write-wins, which should never happen in a correct log.
	type entryKey struct {
		position int
		revision uint64
	}
	allEntries := map[string]map[entryKey]JournalEntry{} // run → key → entry

	for _, ev := range events {
		if ev.Kind != evCapability {
			continue
		}
		var cd capabilityData
		if err := json.Unmarshal(ev.Data, &cd); err != nil {
			return ThreadGraph{}, fmt.Errorf("decode capability.recorded: %w", err)
		}
		if allEntries[ev.Run] == nil {
			allEntries[ev.Run] = make(map[entryKey]JournalEntry)
		}
		allEntries[ev.Run][entryKey{cd.Position, ev.Rev}] = JournalEntry{
			Index:    cd.Position,
			Revision: ev.Rev,
			Call:     cd.Call,
			Outcome:  cd.Outcome,
		}
	}

	for _, meta := range metas {
		entries := make([]JournalEntry, 0, len(allEntries[meta.id]))
		for _, e := range allEntries[meta.id] {
			entries = append(entries, e)
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Index != entries[j].Index {
				return entries[i].Index < entries[j].Index
			}
			return entries[i].Revision < entries[j].Revision
		})
		graph.Runs = append(graph.Runs, ThreadGraphRun{
			RunID:           meta.id,
			Message:         meta.message,
			ParentRunID:     meta.parentRunID,
			Status:          meta.status,
			Answer:          meta.answer,
			Error:           meta.err,
			Attempt:         meta.attempt,
			CurrentRevision: meta.currentRevision,
			ChildRunIDs:     meta.childRunIDs,
			Entries:         entries,
		})
	}
	return graph, nil
}

// RunGraphNode is a node in the projected call graph: a run together with the
// delegated child runs it spawned, in spawn order.
type RunGraphNode struct {
	RunID    string         `json:"run_id"`
	Name     string         `json:"name,omitempty"`
	ThreadID string         `json:"thread_id"`
	ParentID string         `json:"parent_id,omitempty"`
	Status   RunStatus      `json:"status"`
	Attempt  int            `json:"attempt"`
	Revision uint64         `json:"revision"`
	Answer   string         `json:"answer,omitempty"`
	Error    string         `json:"error,omitempty"`
	Children []RunGraphNode `json:"children,omitempty"`
}

// CallGraph projects a run and its delegated child runs (recursively) into a
// tree, using the recorded parent/child links.
func (r *Runtime) CallGraph(runID string) (RunGraphNode, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.runs[runID]; !ok {
		return RunGraphNode{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	return r.callGraphLocked(runID, make(map[string]bool)), nil
}

func (r *Runtime) callGraphLocked(runID string, visited map[string]bool) RunGraphNode {
	run := r.runs[runID]
	if run == nil || visited[runID] {
		return RunGraphNode{RunID: runID}
	}
	visited[runID] = true
	node := RunGraphNode{
		RunID:    run.id,
		Name:     run.manifest.Name,
		ThreadID: run.threadID,
		ParentID: run.parentRunID,
		Status:   run.status,
		Attempt:  run.attempt,
		Revision: run.revision,
		Answer:   run.answer,
		Error:    run.err,
	}
	// Build a brain→name index from the parent's agent tools as a backfill: a
	// child run with an empty Name can infer it from the parent's `core.agent`
	// tool whose brain (settings.code) matches.
	childNameByBrain := make(map[string]string)
	for _, tool := range run.manifest.agentTools() {
		if s, err := decodeAgentSettings(tool); err == nil && s.Code != "" && tool.Name != "" {
			childNameByBrain[s.Code] = tool.Name
		}
	}
	for _, childID := range run.childRunIDs {
		childNode := r.callGraphLocked(childID, visited)
		if childNode.Name == "" {
			if cr := r.runs[childID]; cr != nil {
				childNode.Name = childNameByBrain[cr.manifest.Brain]
			}
		}
		node.Children = append(node.Children, childNode)
	}
	return node
}
