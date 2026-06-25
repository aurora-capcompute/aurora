package agent

import "fmt"

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
