package agent

import (
	"capcompute"
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay"
	"capcompute/dispatcher/replay/tape/journaled"
	"capcompute/dispatcher/replay/tape/journaled/journal/memory"
	"capcompute/session_store_memory"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	internalhost "aurora-capcompute/internal/host"
	"aurora-capcompute/internal/llm"
	extism "github.com/extism/go-sdk"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("lifecycle conflict")
	ErrInvalid  = errors.New("invalid request")
)

type RunKey struct {
	ID string
}

func (r RunKey) SessionKey() string {
	return r.ID
}

type RunStatus string

const (
	RunQueued    RunStatus = "queued"
	RunRunning   RunStatus = "running"
	RunStopping  RunStatus = "stopping"
	RunYielded   RunStatus = "yielded"
	RunCompleted RunStatus = "completed"
	RunStopped   RunStatus = "stopped"
	RunFailed    RunStatus = "failed"
)

type RetryMode string

const (
	RetryResume  RetryMode = "resume"
	RetryRestart RetryMode = "restart"
)

type HistoryMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Config struct {
	WasmPath  string
	LLM       llm.Client
	Internet  internalhost.InternetReader
	IDSource  func(prefix string) (string, error)
	Now       func() time.Time
	EventSize int
}

type Runtime struct {
	mu          sync.Mutex
	compute     *capcompute.ComputeCompiledPlugin[string, RunKey]
	store       *session_store_memory.Store[string, RunKey]
	threads     map[string]*threadState
	runs        map[string]*runState
	subscribers map[string]map[uint64]chan Event
	nextSubID   uint64
	idSource    func(string) (string, error)
	now         func() time.Time
	eventSize   int
	wg          sync.WaitGroup
	closed      bool
}

type threadState struct {
	id          string
	createdAt   time.Time
	updatedAt   time.Time
	history     []HistoryMessage
	runIDs      []string
	activeRunID string
}

type runState struct {
	id              string
	threadID        string
	message         string
	history         []HistoryMessage
	status          RunStatus
	attempt         int
	createdAt       time.Time
	updatedAt       time.Time
	startedAt       *time.Time
	completedAt     *time.Time
	answer          string
	err             string
	journal         *observableJournal
	session         *capcompute.Session[RunKey]
	handle          *capcompute.PlayHandle[RunKey]
	stopRequested   bool
	preserveSession bool
}

type agentInput struct {
	Message string           `json:"message"`
	History []HistoryMessage `json:"history,omitempty"`
}

type agentOutput struct {
	Status string `json:"status"`
	Answer string `json:"answer"`
}

type ThreadSummary struct {
	ID          string    `json:"id"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	RunCount    int       `json:"run_count"`
	ActiveRunID string    `json:"active_run_id,omitempty"`
}

type ThreadSnapshot struct {
	ThreadSummary
	History []HistoryMessage `json:"history"`
	Runs    []RunSnapshot    `json:"runs"`
}

type RunSnapshot struct {
	ID            string     `json:"id"`
	ThreadID      string     `json:"thread_id"`
	Message       string     `json:"message"`
	Status        RunStatus  `json:"status"`
	Attempt       int        `json:"attempt"`
	Answer        string     `json:"answer,omitempty"`
	Error         string     `json:"error,omitempty"`
	JournalLength int        `json:"journal_length"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
}

type JournalEntry struct {
	Index   int             `json:"index"`
	Call    dispatcher.Call `json:"call"`
	Outcome JournalOutcome  `json:"outcome"`
}

type JournalOutcome struct {
	Status  dispatcher.OutcomeKind `json:"status"`
	Result  json.RawMessage        `json:"result,omitempty"`
	Message string                 `json:"message,omitempty"`
}

type Event struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type JournalEvent struct {
	RunID string `json:"run_id"`
	Index int    `json:"index"`
	Call  string `json:"call"`
}

func NewRuntime(ctx context.Context, config Config) (*Runtime, error) {
	if config.WasmPath == "" {
		return nil, fmt.Errorf("%w: wasm path is required", ErrInvalid)
	}
	if config.LLM == nil {
		return nil, fmt.Errorf("%w: llm client is required", ErrInvalid)
	}
	if config.Internet == nil {
		return nil, fmt.Errorf("%w: internet reader is required", ErrInvalid)
	}
	runtime := &Runtime{
		store:       session_store_memory.New[string, RunKey](),
		threads:     make(map[string]*threadState),
		runs:        make(map[string]*runState),
		subscribers: make(map[string]map[uint64]chan Event),
		idSource:    config.IDSource,
		now:         config.Now,
		eventSize:   config.EventSize,
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

	compute, err := capcompute.NewComputeCompiledPlugin[string, RunKey](ctx, capcompute.Config[string, RunKey]{
		Manifest: extism.Manifest{
			Wasm: []extism.Wasm{extism.WasmFile{Path: config.WasmPath}},
		},
		PluginConfig: extism.PluginConfig{EnableWasi: true},
		Dispatchers: internalhost.Factory[RunKey]{
			LLM:      config.LLM,
			Internet: config.Internet,
			NewTape: func(_ context.Context, key RunKey) (replay.Tape, error) {
				runtime.mu.Lock()
				run := runtime.runs[key.ID]
				runtime.mu.Unlock()
				if run == nil || run.journal == nil {
					return nil, fmt.Errorf("%w: journal for run %s", ErrNotFound, key.ID)
				}
				return journaled.NewTape(run.journal), nil
			},
		},
		SessionStore: runtime.store,
	})
	if err != nil {
		return nil, err
	}
	runtime.compute = compute
	return runtime, nil
}

func (r *Runtime) CreateThread() (ThreadSnapshot, error) {
	id, err := r.idSource("thr_")
	if err != nil {
		return ThreadSnapshot{}, err
	}
	now := r.now().UTC()
	thread := &threadState{id: id, createdAt: now, updatedAt: now}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ThreadSnapshot{}, fmt.Errorf("%w: runtime is closed", ErrConflict)
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

func (r *Runtime) GetThread(threadID string) (ThreadSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	thread := r.threads[threadID]
	if thread == nil {
		return ThreadSnapshot{}, fmt.Errorf("%w: thread %s", ErrNotFound, threadID)
	}
	return r.threadSnapshotLocked(thread), nil
}

func (r *Runtime) CreateRun(threadID string, message string) (RunSnapshot, error) {
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
	run := &runState{
		id:        runID,
		threadID:  threadID,
		message:   message,
		history:   append([]HistoryMessage(nil), thread.history...),
		status:    RunQueued,
		attempt:   1,
		createdAt: now,
		updatedAt: now,
	}
	run.journal = r.newJournal(run)
	r.runs[runID] = run
	thread.runIDs = append(thread.runIDs, runID)
	thread.activeRunID = runID
	thread.updatedAt = now
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
	var journal *observableJournal
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
	case RunYielded:
		closeSession = run.session
		r.finishLocked(run, RunStopped, "", context.Canceled)
	case RunStopping, RunStopped:
	default:
		r.mu.Unlock()
		return RunSnapshot{}, fmt.Errorf("%w: run %s cannot be stopped from %s", ErrConflict, runID, run.status)
	}
	snapshot := r.runSnapshotLocked(run)
	r.mu.Unlock()
	if closeSession != nil {
		_ = closeSession.Close(context.Background())
	}
	r.publish(run.threadID, Event{Type: "run.updated", Data: snapshot})
	return snapshot, nil
}

func (r *Runtime) Retry(runID string, mode RetryMode) (RunSnapshot, error) {
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
	case RunYielded, RunStopped, RunFailed:
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

	if mode == RetryRestart {
		run.journal = r.newJournal(run)
		run.preserveSession = false
	} else {
		run.preserveSession = run.status == RunYielded
	}
	run.status = RunQueued
	run.attempt++
	run.answer = ""
	run.err = ""
	run.stopRequested = false
	run.startedAt = nil
	run.completedAt = nil
	run.updatedAt = r.now().UTC()
	thread.activeRunID = run.id
	thread.updatedAt = run.updatedAt
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
	return r.compute.CloseCompiled(context.Background())
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
	input, err := json.Marshal(agentInput{Message: run.message, History: run.history})
	if err != nil {
		r.finishLocked(run, RunFailed, "", err)
		snapshot := r.runSnapshotLocked(run)
		threadID := run.threadID
		r.mu.Unlock()
		r.publish(threadID, Event{Type: "run.updated", Data: snapshot})
		return
	}
	session := run.session
	preserve := run.preserveSession && session != nil
	run.preserveSession = false
	r.mu.Unlock()

	if !preserve {
		if session != nil {
			_ = session.Close(context.Background())
		}
		session, err = r.compute.CreateSession(context.Background(), capcompute.PlayRequest[string, RunKey]{
			Input:      input,
			Entrypoint: "run",
			UserData:   RunKey{ID: runID},
		})
		if err == nil {
			err = r.store.SaveSession(context.Background(), runID, session)
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
	r.mu.Unlock()
	r.publish(threadID, Event{Type: "run.updated", Data: snapshot})

	handle, err := r.compute.Play(context.Background(), session)
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
	switch result.Status {
	case capcompute.PlayCompleted:
		var output agentOutput
		if err := json.Unmarshal(result.Output, &output); err != nil {
			r.finish(runID, RunFailed, "", fmt.Errorf("decode agent output: %w", err))
			return
		}
		if output.Answer == "" {
			r.finish(runID, RunFailed, "", errors.New("agent output missing answer"))
			return
		}
		r.finish(runID, RunCompleted, output.Answer, nil)
	case capcompute.PlayYielded:
		r.finish(runID, RunYielded, "", nil)
	case capcompute.PlayStopped:
		r.finish(runID, RunStopped, "", result.Err)
	default:
		r.finish(runID, RunFailed, "", result.Err)
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
	thread := r.threads[run.threadID]
	if thread != nil {
		if status != RunYielded && thread.activeRunID == run.id {
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

func (r *Runtime) newJournal(run *runState) *observableJournal {
	return &observableJournal{
		Journal: memory.NewJournal(),
		onStore: func(index int, call dispatcher.Call) {
			r.publish(run.threadID, Event{
				Type: "journal.appended",
				Data: JournalEvent{RunID: run.id, Index: index, Call: call.Name},
			})
		},
	}
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
		CreatedAt:   thread.createdAt,
		UpdatedAt:   thread.updatedAt,
		RunCount:    len(thread.runIDs),
		ActiveRunID: thread.activeRunID,
	}
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
		ID:            run.id,
		ThreadID:      run.threadID,
		Message:       run.message,
		Status:        run.status,
		Attempt:       run.attempt,
		Answer:        run.answer,
		Error:         run.err,
		JournalLength: journalLength,
		CreatedAt:     run.createdAt,
		UpdatedAt:     run.updatedAt,
		StartedAt:     copyTime(run.startedAt),
		CompletedAt:   copyTime(run.completedAt),
	}
}

func copyTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func randomID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(raw[:]), nil
}

type observableJournal struct {
	*memory.Journal
	onStore func(index int, call dispatcher.Call)
}

func (j *observableJournal) Store(index int, call dispatcher.Call, outcome dispatcher.Outcome) error {
	if err := j.Journal.Store(index, call, outcome); err != nil {
		return err
	}
	if j.onStore != nil {
		j.onStore(index, call)
	}
	return nil
}
