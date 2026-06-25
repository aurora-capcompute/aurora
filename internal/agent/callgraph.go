package agent

import (
	"context"
	"fmt"
)

// RevisionView is one copy-on-write revision of a run: its fork metadata and the
// full (parent-resolved) journal of calls and outcomes recorded for it.
type RevisionView struct {
	Revision   uint64         `json:"revision"`
	Forked     bool           `json:"forked"`
	ForkParent uint64         `json:"fork_parent,omitempty"`
	ForkOffset int            `json:"fork_offset"`
	Entries    []JournalEntry `json:"entries"`
}

// ThreadGraphRun is a run within a thread graph: its metadata, the child runs it
// delegated to, and every revision it has been through.
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
	Revisions       []RevisionView `json:"revisions"`
}

// ThreadGraph projects a whole thread for exploration: its runs in order, each
// with its delegation links and full revision history.
type ThreadGraph struct {
	ThreadID string           `json:"thread_id"`
	Title    string           `json:"title"`
	Runs     []ThreadGraphRun `json:"runs"`
}

// runRevisionMeta is a lock-free snapshot of a run used to read its journals
// without holding the runtime lock during store I/O.
type runRevisionMeta struct {
	id          string
	message     string
	parentRunID string
	status      RunStatus
	answer      string
	err         string
	attempt     int
	revision    uint64
	childRunIDs []string
}

// ThreadGraph builds the execution graph of a thread across all revisions of all
// its runs, reading each revision's journal from the state store.
func (r *Runtime) ThreadGraph(threadID string) (ThreadGraph, error) {
	r.mu.Lock()
	thread := r.threads[threadID]
	if thread == nil {
		r.mu.Unlock()
		return ThreadGraph{}, fmt.Errorf("%w: thread %s", ErrNotFound, threadID)
	}
	graph := ThreadGraph{ThreadID: thread.id, Title: thread.title}
	metas := make([]runRevisionMeta, 0, len(thread.runIDs))
	for _, runID := range thread.runIDs {
		run := r.runs[runID]
		if run == nil {
			continue
		}
		metas = append(metas, runRevisionMeta{
			id:          run.id,
			message:     run.message,
			parentRunID: run.parentRunID,
			status:      run.status,
			answer:      run.answer,
			err:         run.err,
			attempt:     run.attempt,
			revision:    run.revision,
			childRunIDs: append([]string(nil), run.childRunIDs...),
		})
	}
	tenantID := r.tenantID
	r.mu.Unlock()

	for _, meta := range metas {
		runView := ThreadGraphRun{
			RunID:           meta.id,
			Message:         meta.message,
			ParentRunID:     meta.parentRunID,
			Status:          meta.status,
			Answer:          meta.answer,
			Error:           meta.err,
			Attempt:         meta.attempt,
			CurrentRevision: meta.revision,
			ChildRunIDs:     meta.childRunIDs,
		}
		for rev := uint64(1); rev <= meta.revision; rev++ {
			scope := RunContext{TenantID: tenantID, ThreadID: threadID, RunID: meta.id, Revision: rev}
			view, err := r.revisionView(scope)
			if err != nil {
				return ThreadGraph{}, err
			}
			runView.Revisions = append(runView.Revisions, view)
		}
		graph.Runs = append(graph.Runs, runView)
	}
	return graph, nil
}

func (r *Runtime) revisionView(scope RunContext) (RevisionView, error) {
	ctx := context.Background()
	offset, forked, err := r.stateStore.ForkInfo(ctx, scope)
	if err != nil {
		return RevisionView{}, err
	}
	view := RevisionView{Revision: scope.Revision, Forked: forked, ForkOffset: offset}
	if forked {
		view.ForkParent = scope.Revision - 1
	}
	journal, err := r.stateStore.OpenJournal(ctx, scope)
	if err != nil {
		return RevisionView{}, err
	}
	length := journal.Length()
	for i := 0; i < length; i++ {
		record, err := journal.Load(i)
		if err != nil {
			return RevisionView{}, err
		}
		view.Entries = append(view.Entries, JournalEntry{
			Index: i,
			Call:  record.Call,
			Outcome: JournalOutcome{
				Status:  record.Outcome.Kind(),
				Result:  record.Outcome.Result(),
				Message: record.Outcome.Message(),
			},
		})
	}
	return view, nil
}

// RunGraphNode is a node in the projected call graph: a run together with the
// delegated child runs it spawned, in spawn order.
type RunGraphNode struct {
	RunID    string         `json:"run_id"`
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
// tree, using the recorded parent/child links. It reflects the current state of
// runs resident in the runtime.
func (r *Runtime) CallGraph(runID string) (RunGraphNode, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.runs[runID]; !ok {
		return RunGraphNode{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	return r.callGraphLocked(runID, make(map[string]bool)), nil
}

// callGraphLocked builds the subtree rooted at runID. The visited set guards
// against revisiting a run, so a malformed cycle cannot cause infinite recursion.
func (r *Runtime) callGraphLocked(runID string, visited map[string]bool) RunGraphNode {
	run := r.runs[runID]
	if run == nil || visited[runID] {
		return RunGraphNode{RunID: runID}
	}
	visited[runID] = true
	node := RunGraphNode{
		RunID:    run.id,
		ThreadID: run.threadID,
		ParentID: run.parentRunID,
		Status:   run.status,
		Attempt:  run.attempt,
		Revision: run.revision,
		Answer:   run.answer,
		Error:    run.err,
	}
	for _, childID := range run.childRunIDs {
		node.Children = append(node.Children, r.callGraphLocked(childID, visited))
	}
	return node
}
