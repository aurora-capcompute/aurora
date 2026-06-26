package agent

import (
	"context"
	"testing"
	"time"

	"aurora-capcompute/internal/eventlog"
	"aurora-capcompute/internal/task"
)

// appendAll encodes and appends events to a stream, failing the test on error.
func mustAppend(t *testing.T, log *eventlog.Memory, scope eventlog.Scope, events ...eventlog.Event) {
	t.Helper()
	if _, err := log.Append(context.Background(), scope, events...); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func TestFoldReconstructsLatestRunAndThreadState(t *testing.T) {
	log := eventlog.NewMemory()
	scope := eventlog.Scope{TenantID: "t", ThreadID: "th1"}
	now := time.Unix(0, 0).UTC()

	th, _ := threadStateEvent(now, StoredThread{TenantID: "t", ID: "th1", Title: "first", ActiveRunID: "run1"})
	r1, _ := runStateEvent(now, StoredRun{TenantID: "t", ID: "run1", ThreadID: "th1", Revision: 1, Status: RunRunning})
	r2, _ := runStateEvent(now.Add(time.Second), StoredRun{TenantID: "t", ID: "run1", ThreadID: "th1", Revision: 1, Status: RunCompleted, Answer: "done"})
	thDone, _ := threadStateEvent(now.Add(time.Second), StoredThread{TenantID: "t", ID: "th1", Title: "first", ActiveRunID: ""})
	mustAppend(t, log, scope, th, r1, r2, thDone)

	events, _ := log.Read(context.Background(), scope, 0)
	proj, err := Fold(events)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if proj.Thread.ActiveRunID != "" {
		t.Fatalf("thread active run = %q, want cleared (last writer wins)", proj.Thread.ActiveRunID)
	}
	run := proj.Runs["run1"]
	if run.Status != RunCompleted || run.Answer != "done" {
		t.Fatalf("run folded to %+v, want completed/done", run)
	}
}

func TestFoldTaskLifecycle(t *testing.T) {
	log := eventlog.NewMemory()
	scope := eventlog.Scope{TenantID: "t", ThreadID: "th1"}
	now := time.Unix(0, 0).UTC()
	rec := task.Record{
		Scope:     task.Scope{TenantID: "t", ThreadID: "th1", RunID: "run1", Revision: 1},
		ID:        "task1",
		State:     task.StatePending,
		TokenHash: []byte{1, 2, 3},
		CreatedAt: now,
	}
	created, _ := taskCreatedEvent(now, rec)

	resolved := rec
	resolved.State = task.StateApproved
	resolvedEv, _ := taskResolvedEvent(now.Add(time.Second), resolved)

	executed, _ := taskExecutedEvent(now.Add(2*time.Second), "run1", 1, "task1")
	mustAppend(t, log, scope, created, resolvedEv, executed)

	events, _ := log.Read(context.Background(), scope, 0)
	proj, err := Fold(events)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	got := proj.Tasks["task1"]
	if got.State != task.StateExecuted {
		t.Fatalf("task state = %q, want executed", got.State)
	}
	// TokenHash must survive the round-trip even though task.Record omits it from JSON.
	if len(got.TokenHash) != 3 || got.TokenHash[0] != 1 {
		t.Fatalf("token hash not preserved: %v", got.TokenHash)
	}
	list := proj.TaskList()
	if len(list) != 1 || list[0].ID != "task1" {
		t.Fatalf("task list = %+v", list)
	}
}
