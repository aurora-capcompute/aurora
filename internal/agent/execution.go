package agent

// Run execution: the play goroutine that drives a brain to a terminal state, the
// finishing path, and the event appends plus subscriber publishing that surface
// each state change to the durable log and live watchers.

import (
	"capcompute"
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay/tape/journaled"
	"context"
	"errors"
	"fmt"
	"time"

	"aurora-capcompute/internal/eventlog"
)

// Run execution: the play goroutine, terminal-state finishing, and the event
// appends + subscriber publishing that surface state changes.
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
	acquired, leaseErr := r.leases.Acquire(
		context.Background(), r.tenantID, "run", leaseResource,
		r.instanceID, r.now().UTC(), r.leaseTTL,
	)
	if leaseErr != nil || !acquired {
		err := leaseErr
		if err == nil {
			err = errors.New("run is leased by another Aurora instance")
		}
		r.finishLocked(run, RunInterrupted, "", err)
		_ = r.appendRun(run)
		if thread := r.threads[run.threadID]; thread != nil {
			_ = r.appendThread(thread)
		}
		snapshot := r.runSnapshotLocked(run)
		threadID := run.threadID
		r.mu.Unlock()
		r.publish(threadID, Event{Type: "run.updated", Data: snapshot})
		return
	}
	defer r.leases.Release(
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
	_ = r.appendRun(run)
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
		tasks, taskErr := r.tasks.List(context.Background(), r.tenantID, runID)
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
	_ = r.appendRun(run)
	if thread := r.threads[run.threadID]; thread != nil {
		// Conversation history is no longer persisted separately; it is derived
		// from the thread's completed runs (each run stores its message + answer)
		// and rebuilt on recovery.
		_ = r.appendThread(thread)
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

// scope returns the event stream key for a thread.
func (r *Runtime) scope(threadID string) eventlog.Scope {
	return eventlog.Scope{TenantID: r.tenantID, ThreadID: threadID}
}

func (r *Runtime) journalNow() time.Time { return r.now().UTC() }

// journalAppendPublisher publishes a journal.appended event for a thread when a
// capability record is appended to one of its runs' journals.
func (r *Runtime) journalAppendPublisher(threadID string) func(string, int, dispatcher.Call, dispatcher.Outcome) {
	return func(runID string, index int, call dispatcher.Call, outcome dispatcher.Outcome) {
		r.publish(threadID, Event{
			Type: "journal.appended",
			Data: JournalEvent{
				RunID:         runID,
				Index:         index,
				Call:          call.Name,
				OutcomeStatus: outcome.Kind(),
				OutcomeSize:   len(outcome.Result()),
			},
		})
	}
}

// appendThread records a thread's current state to its event stream.
func (r *Runtime) appendThread(thread *threadState) error {
	ev, err := threadStateEvent(r.now().UTC(), r.storedThreadLocked(thread))
	if err != nil {
		return err
	}
	_, err = r.log.Append(context.Background(), r.scope(thread.id), ev)
	return err
}

// appendRun records a run's current state to its thread's event stream.
func (r *Runtime) appendRun(run *runState) error {
	ev, err := runStateEvent(r.now().UTC(), r.storedRunLocked(run))
	if err != nil {
		return err
	}
	_, err = r.log.Append(context.Background(), r.scope(run.threadID), ev)
	return err
}

func (r *Runtime) newJournal(run *runState) (journaled.Journal, error) {
	return newLogJournal(r.log, r.scope(run.threadID), run.id, run.revision,
		r.journalNow, r.journalAppendPublisher(run.threadID)), nil
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
