package agent

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"aurora-capcompute/internal/eventlog"
	"aurora-capcompute/internal/task"
)

// Domain event kinds appended to a thread's eventlog stream. Lifecycle payloads
// are state-carried: a thread/run event holds the entity's full durable state at
// that point, so folding is last-writer-wins per id and provably reproduces the
// state the runtime previously upserted. Task events are semantic (created /
// resolved / executed). Capability-journal and fork events are defined alongside
// the journal view.
const (
	evThreadState  = "thread.state"
	evRunState     = "run.state"
	evTaskCreated  = "task.created"
	evTaskResolved = "task.resolved"
	evTaskExecuted = "task.executed"
)

// taskEventData carries a task record plus its token hash, which task.Record
// deliberately omits from JSON (json:"-") since it is a secret-derived value the
// store must persist out of band.
type taskEventData struct {
	Record    task.Record `json:"record"`
	TokenHash []byte      `json:"token_hash,omitempty"`
}

type taskExecutedData struct {
	TaskID string `json:"task_id"`
}

func threadStateEvent(now time.Time, t StoredThread) (eventlog.Event, error) {
	return encodeEvent(evThreadState, "", 0, now, t)
}

func runStateEvent(now time.Time, r StoredRun) (eventlog.Event, error) {
	return encodeEvent(evRunState, r.ID, r.Revision, now, r)
}

func taskCreatedEvent(now time.Time, record task.Record) (eventlog.Event, error) {
	return encodeEvent(evTaskCreated, record.Scope.RunID, record.Scope.Revision, now,
		taskEventData{Record: record, TokenHash: record.TokenHash})
}

func taskResolvedEvent(now time.Time, record task.Record) (eventlog.Event, error) {
	return encodeEvent(evTaskResolved, record.Scope.RunID, record.Scope.Revision, now,
		taskEventData{Record: record, TokenHash: record.TokenHash})
}

func taskExecutedEvent(now time.Time, runID string, rev uint64, taskID string) (eventlog.Event, error) {
	return encodeEvent(evTaskExecuted, runID, rev, now, taskExecutedData{TaskID: taskID})
}

func encodeEvent(kind, run string, rev uint64, now time.Time, payload any) (eventlog.Event, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return eventlog.Event{}, fmt.Errorf("encode %s event: %w", kind, err)
	}
	return eventlog.Event{Kind: kind, Time: now.UTC(), Run: run, Rev: rev, Data: data}, nil
}

// Projection is the durable state folded from a thread's event stream: the same
// StoredState + task records the runtime previously loaded from the CRUD stores,
// now derived from the append-only log.
type Projection struct {
	Thread StoredThread
	Runs   map[string]StoredRun
	Tasks  map[string]task.Record
}

// Fold reconstructs a thread's durable projection from its event stream. Events
// must be in append order (ascending Seq).
func Fold(events []eventlog.Event) (Projection, error) {
	proj := Projection{
		Runs:  make(map[string]StoredRun),
		Tasks: make(map[string]task.Record),
	}
	for _, ev := range events {
		switch ev.Kind {
		case evThreadState:
			var t StoredThread
			if err := json.Unmarshal(ev.Data, &t); err != nil {
				return Projection{}, fmt.Errorf("decode thread.state: %w", err)
			}
			proj.Thread = t
		case evRunState:
			var r StoredRun
			if err := json.Unmarshal(ev.Data, &r); err != nil {
				return Projection{}, fmt.Errorf("decode run.state: %w", err)
			}
			proj.Runs[r.ID] = r
		case evTaskCreated, evTaskResolved:
			var td taskEventData
			if err := json.Unmarshal(ev.Data, &td); err != nil {
				return Projection{}, fmt.Errorf("decode %s: %w", ev.Kind, err)
			}
			td.Record.TokenHash = td.TokenHash
			proj.Tasks[td.Record.ID] = td.Record
		case evTaskExecuted:
			var x taskExecutedData
			if err := json.Unmarshal(ev.Data, &x); err != nil {
				return Projection{}, fmt.Errorf("decode task.executed: %w", err)
			}
			if rec, ok := proj.Tasks[x.TaskID]; ok {
				rec.State = task.StateExecuted
				proj.Tasks[x.TaskID] = rec
			}
		}
		// capability.recorded / run.forked are folded by the journal view, not here.
	}
	return proj, nil
}

// TaskList returns the projection's task records sorted by creation time, the
// order callers expect from the old TaskStore.List.
func (p Projection) TaskList() []task.Record {
	out := make([]task.Record, 0, len(p.Tasks))
	for _, rec := range p.Tasks {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}
