package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"aurora-capcompute/internal/eventlog"

	"capcompute/dispatcher"
	"capcompute/dispatcher/replay/tape/journaled"
)

// Capability-journal events. The journal is a projection of the same append-only
// stream as lifecycle/task events: each recorded call is a capability.recorded
// event, and a copy-on-write revision fork is a run.forked event. A revision's
// journal is its forked prefix (from the parent revision) plus its own appended
// records.
const (
	evCapability = "capability.recorded"
	evForked     = "run.forked"
)

type capabilityData struct {
	Position int             `json:"position"`
	Call     dispatcher.Call `json:"call"`
	Outcome  JournalOutcome  `json:"outcome"`
}

type forkedData struct {
	FromRev uint64 `json:"from_rev"`
	Offset  int    `json:"offset"`
}

func encodeOutcome(o dispatcher.Outcome) JournalOutcome {
	return JournalOutcome{Status: o.Kind(), Result: o.Result(), Message: o.Message()}
}

func decodeOutcome(jo JournalOutcome) dispatcher.Outcome {
	switch jo.Status {
	case dispatcher.OutcomeResult:
		return dispatcher.Result(jo.Result)
	case dispatcher.OutcomeYield:
		return dispatcher.Yield(jo.Message)
	default:
		return dispatcher.Failed(jo.Message)
	}
}

// logJournal implements journaled.Journal over an event stream. It mirrors the
// copy-on-write structure of the old store journal — reads below offset fall
// through to the parent revision, new records are appended to this revision's
// tail — but each append is a capability.recorded event on the log rather than a
// row in a separate journal table.
type logJournal struct {
	log      eventlog.Log
	scope    eventlog.Scope
	run      string
	rev      uint64
	now      func() time.Time
	onAppend func(run string, position int, call dispatcher.Call, outcome dispatcher.Outcome)

	mu      sync.Mutex
	parent  *logJournal
	offset  int
	records []journaled.Record
}

func newLogJournal(log eventlog.Log, scope eventlog.Scope, run string, rev uint64, now func() time.Time,
	onAppend func(string, int, dispatcher.Call, dispatcher.Outcome)) *logJournal {
	return &logJournal{log: log, scope: scope, run: run, rev: rev, now: now, onAppend: onAppend}
}

func (j *logJournal) Length() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.offset + len(j.records)
}

func (j *logJournal) Load(index int) (journaled.Record, error) {
	j.mu.Lock()
	if j.parent != nil && index < j.offset {
		j.mu.Unlock()
		return j.parent.Load(index)
	}
	defer j.mu.Unlock()
	local := index - j.offset
	if local < 0 || local >= len(j.records) {
		return journaled.Record{}, errors.New("journal record not found")
	}
	record := j.records[local]
	return journaled.Record{Call: record.Call.Copy(), Outcome: record.Outcome.Copy()}, nil
}

func (j *logJournal) Store(index int, call dispatcher.Call, outcome dispatcher.Outcome) error {
	j.mu.Lock()
	if index != j.offset+len(j.records) {
		j.mu.Unlock()
		return errors.New("invalid journal index")
	}
	ev, err := encodeEvent(evCapability, j.run, j.rev, j.now(), capabilityData{
		Position: index, Call: call.Copy(), Outcome: encodeOutcome(outcome),
	})
	if err != nil {
		j.mu.Unlock()
		return err
	}
	if _, err := j.log.Append(context.Background(), j.scope, ev); err != nil {
		j.mu.Unlock()
		return err
	}
	j.records = append(j.records, journaled.Record{Call: call.Copy(), Outcome: outcome.Copy()})
	j.mu.Unlock()
	if j.onAppend != nil {
		j.onAppend(j.run, index, call, outcome)
	}
	return nil
}

// fork mints a child revision sharing this revision's prefix [0, offset)
// copy-on-write, recording the relationship as a run.forked event. The parent
// revision is left untouched and remains addressable.
func (j *logJournal) fork(childRev uint64, offset int) (*logJournal, error) {
	if offset < 0 {
		return nil, errors.New("fork journal: negative offset")
	}
	ev, err := encodeEvent(evForked, j.run, childRev, j.now(), forkedData{FromRev: j.rev, Offset: offset})
	if err != nil {
		return nil, err
	}
	if _, err := j.log.Append(context.Background(), j.scope, ev); err != nil {
		return nil, err
	}
	return &logJournal{
		log: j.log, scope: j.scope, run: j.run, rev: childRev, now: j.now,
		onAppend: j.onAppend, parent: j, offset: offset,
	}, nil
}

// foldJournals rebuilds every revision's journal for a thread stream from its
// capability.recorded and run.forked events, linking forked revisions to their
// parents. The result is keyed by run id then revision; the runtime selects each
// run's current revision and lets new records append onto it.
func foldJournals(events []eventlog.Event, log eventlog.Log, scope eventlog.Scope, now func() time.Time,
	onAppend func(string, int, dispatcher.Call, dispatcher.Outcome)) (map[string]map[uint64]*logJournal, error) {
	own := map[string]map[uint64][]journaled.Record{}
	forks := map[string]map[uint64]forkedData{}
	revs := map[string]map[uint64]struct{}{}
	note := func(run string, rev uint64) {
		if revs[run] == nil {
			revs[run] = map[uint64]struct{}{}
		}
		revs[run][rev] = struct{}{}
	}

	for _, ev := range events {
		switch ev.Kind {
		case evCapability:
			var cd capabilityData
			if err := json.Unmarshal(ev.Data, &cd); err != nil {
				return nil, fmt.Errorf("decode capability.recorded: %w", err)
			}
			if own[ev.Run] == nil {
				own[ev.Run] = map[uint64][]journaled.Record{}
			}
			own[ev.Run][ev.Rev] = append(own[ev.Run][ev.Rev],
				journaled.Record{Call: cd.Call, Outcome: decodeOutcome(cd.Outcome)})
			note(ev.Run, ev.Rev)
		case evForked:
			var fd forkedData
			if err := json.Unmarshal(ev.Data, &fd); err != nil {
				return nil, fmt.Errorf("decode run.forked: %w", err)
			}
			if forks[ev.Run] == nil {
				forks[ev.Run] = map[uint64]forkedData{}
			}
			forks[ev.Run][ev.Rev] = fd
			note(ev.Run, ev.Rev)
			note(ev.Run, fd.FromRev)
		}
	}

	result := map[string]map[uint64]*logJournal{}
	for run, set := range revs {
		ordered := make([]uint64, 0, len(set))
		for rev := range set {
			ordered = append(ordered, rev)
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
		result[run] = map[uint64]*logJournal{}
		for _, rev := range ordered {
			j := newLogJournal(log, scope, run, rev, now, onAppend)
			if fd, ok := forks[run][rev]; ok {
				j.parent = result[run][fd.FromRev]
				j.offset = fd.Offset
			}
			j.records = own[run][rev]
			result[run][rev] = j
		}
	}
	return result, nil
}
