package agent

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aurora-capcompute/internal/task"
	"capcompute"
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay/tape/journaled"
)

type runtimeDispatchers struct {
	mu        sync.Mutex
	manifests []Manifest
}

func (*runtimeDispatchers) Normalize(_ string, settings json.RawMessage) (json.RawMessage, error) {
	if len(settings) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return append(json.RawMessage(nil), settings...), nil
}

func (p *runtimeDispatchers) NewDispatcher(_ context.Context, _ RunContext, manifest Manifest) (dispatcher.Dispatcher[RunContext], error) {
	p.mu.Lock()
	p.manifests = append(p.manifests, cloneManifest(manifest))
	p.mu.Unlock()
	return finalDispatcher{}, nil
}

func (*runtimeDispatchers) IsSubset(_ string, _, _ json.RawMessage) error {
	return nil
}

type finalDispatcher struct{}

func (finalDispatcher) Dispatch(_ context.Context, _ RunContext, call dispatcher.Call) (dispatcher.Outcome, error) {
	if call.Name != "openai.chat" {
		return dispatcher.Failed("unsupported call: " + call.Name), nil
	}
	return dispatcher.Result(json.RawMessage(
		`{"choices":[{"message":{"content":"{\"actions\":[{\"action\":\"final\",\"content\":{\"answer\":\"done\"}}]}"}}]}`,
	)), nil
}

type runtimeStore struct {
	mu       sync.Mutex
	state    StoredState
	journals map[string]*testJournal
	tasks    map[string]task.Record
	leases   map[string]string
}

func newRuntimeStore() *runtimeStore {
	return &runtimeStore{
		journals: make(map[string]*testJournal),
		tasks:    make(map[string]task.Record),
		leases:   make(map[string]string),
	}
}

func (s *runtimeStore) Load(context.Context, string) (StoredState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, nil
}

func (s *runtimeStore) SaveThread(_ context.Context, thread StoredThread) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.state.Threads {
		if s.state.Threads[i].ID == thread.ID {
			s.state.Threads[i] = thread
			return nil
		}
	}
	s.state.Threads = append(s.state.Threads, thread)
	return nil
}

func (s *runtimeStore) SaveRun(_ context.Context, run StoredRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.state.Runs {
		if s.state.Runs[i].ID == run.ID {
			s.state.Runs[i] = run
			return nil
		}
	}
	s.state.Runs = append(s.state.Runs, run)
	return nil
}

func (s *runtimeStore) AppendMessages(_ context.Context, tenantID, threadID string, messages []HistoryMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	position := len(s.state.Messages)
	for _, message := range messages {
		s.state.Messages = append(s.state.Messages, StoredMessage{
			TenantID: tenantID, ThreadID: threadID, Position: position,
			Role: message.Role, Content: message.Content,
		})
		position++
	}
	return nil
}

func (s *runtimeStore) OpenJournal(_ context.Context, key RunContext) (journaled.Journal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	journal := s.journals[key.SessionKey()]
	if journal == nil {
		journal = &testJournal{}
		s.journals[key.SessionKey()] = journal
	}
	return journal, nil
}

func (s *runtimeStore) ForkJournal(_ context.Context, parent, child RunContext, offset int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.journals[child.SessionKey()] = &testJournal{
		parent: s.journals[parent.SessionKey()],
		offset: offset,
	}
	return nil
}

func (s *runtimeStore) AcquireLease(_ context.Context, tenant, kind, resource, holder string, _ time.Time, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenant + "/" + kind + "/" + resource
	if current := s.leases[key]; current != "" && current != holder {
		return false, nil
	}
	s.leases[key] = holder
	return true, nil
}

func (s *runtimeStore) ReleaseLease(_ context.Context, tenant, kind, resource, holder string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenant + "/" + kind + "/" + resource
	if s.leases[key] == holder {
		delete(s.leases, key)
	}
	return nil
}

func (s *runtimeStore) Find(_ context.Context, scope task.Scope, position int, hash string) (task.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range s.tasks {
		if record.Scope == scope && record.JournalPosition == position && record.CallHash == hash {
			return record, true, nil
		}
	}
	return task.Record{}, false, nil
}

func (s *runtimeStore) Create(_ context.Context, record task.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tasks[record.ID]; exists {
		return task.ErrConflict
	}
	s.tasks[record.ID] = record
	return nil
}

func (s *runtimeStore) Get(_ context.Context, _ string, taskID string) (task.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.tasks[taskID]
	if !ok {
		return task.Record{}, task.ErrNotFound
	}
	return record, nil
}

func (s *runtimeStore) List(_ context.Context, _ string, runID string) ([]task.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var records []task.Record
	for _, record := range s.tasks {
		if runID == "" || record.Scope.RunID == runID {
			records = append(records, record)
		}
	}
	return records, nil
}

func (s *runtimeStore) Resolve(_ context.Context, _ string, taskID string, tokenHash []byte, resolution task.Resolution, now time.Time) (task.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.tasks[taskID]
	if !ok {
		return task.Record{}, task.ErrNotFound
	}
	if !hmac.Equal(record.TokenHash, tokenHash) {
		return task.Record{}, task.ErrUnauthorized
	}
	record.State = resolution.Decision
	record.Resolution = resolution
	record.ResolvedAt = &now
	s.tasks[taskID] = record
	return record, nil
}

func (s *runtimeStore) MarkExecuted(_ context.Context, _ string, taskID string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.tasks[taskID]
	record.State = task.StateExecuted
	s.tasks[taskID] = record
	return nil
}

type testJournal struct {
	mu      sync.Mutex
	records []journaled.Record
	parent  *testJournal
	offset  int
}

func (j *testJournal) Load(index int) (journaled.Record, error) {
	j.mu.Lock()
	parent := j.parent
	offset := j.offset
	if parent != nil && index < offset {
		j.mu.Unlock()
		return parent.Load(index)
	}
	defer j.mu.Unlock()
	local := index - offset
	if local < 0 || local >= len(j.records) {
		return journaled.Record{}, errors.New("record not found")
	}
	return j.records[local], nil
}

func (j *testJournal) Store(index int, call dispatcher.Call, outcome dispatcher.Outcome) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if index != j.offset+len(j.records) {
		return errors.New("invalid index")
	}
	j.records = append(j.records, journaled.Record{Call: call.Copy(), Outcome: outcome.Copy()})
	return nil
}

func (j *testJournal) Length() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.offset + len(j.records)
}

type runtimeSessions struct {
	mu       sync.Mutex
	sessions map[string]*capcompute.Session[RunContext]
}

func newRuntimeSessions() *runtimeSessions {
	return &runtimeSessions{sessions: make(map[string]*capcompute.Session[RunContext])}
}

func (s *runtimeSessions) LoadSession(_ context.Context, id string) (*capcompute.Session[RunContext], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.sessions[id]
	if session == nil {
		return nil, capcompute.ErrSessionRequired
	}
	return session, nil
}

func (s *runtimeSessions) SaveSession(_ context.Context, id string, session *capcompute.Session[RunContext]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[id] = session
	return nil
}

func TestNewRuntimeRequiresImplementationDependencies(t *testing.T) {
	store := newRuntimeStore()
	dispatchers := &runtimeDispatchers{}
	sessions := newRuntimeSessions()
	brains := staticBrains{defaultID: "brain@1", sources: []BrainSource{{ID: "brain@1", Wasm: []byte("wasm")}}}
	base := Config{
		Brains: brains, Dispatchers: dispatchers, StateStore: store,
		TaskStore: store, SessionStore: sessions, TaskSecret: []byte("secret"),
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "brain provider", mutate: func(config *Config) { config.Brains = nil }},
		{name: "dispatcher provider", mutate: func(config *Config) { config.Dispatchers = nil }},
		{name: "state store", mutate: func(config *Config) { config.StateStore = nil }},
		{name: "task store", mutate: func(config *Config) { config.TaskStore = nil }},
		{name: "session store", mutate: func(config *Config) { config.SessionStore = nil }},
		{name: "task secret", mutate: func(config *Config) { config.TaskSecret = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := base
			test.mutate(&config)
			if _, err := NewRuntime(context.Background(), config); err == nil {
				t.Fatal("expected missing dependency error")
			}
		})
	}
}

func TestRuntimePassesEffectiveManifestToDispatcherProvider(t *testing.T) {
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}
	dispatchers := &runtimeDispatchers{}
	store := newRuntimeStore()
	runtime, err := NewRuntime(context.Background(), Config{
		Brains: staticBrains{
			defaultID: "brain@1",
			sources:   []BrainSource{{ID: "brain@1", Wasm: buildBrain(t)}},
		},
		Dispatchers:  dispatchers,
		StateStore:   store,
		TaskStore:    store,
		SessionStore: newRuntimeSessions(),
		TaskSecret:   []byte("stable-secret"),
		IDSource:     sequentialIDs(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	thread, err := runtime.CreateThread(Manifest{
		Version: ManifestVersion,
		Capabilities: []CapabilityConfig{{
			Name: "custom.call", Settings: json.RawMessage(`{"value":1}`),
		}},
	})
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	run, err := runtime.CreateRun(thread.ID, "finish", []CapabilityConfig{{
		Name: "custom.call", Settings: json.RawMessage(`{"value":2}`),
	}})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	waitForStatus(t, runtime, run.ID, RunCompleted)
	journal, err := runtime.Journal(run.ID)
	if err != nil {
		t.Fatalf("load journal: %v", err)
	}
	if len(journal) != 3 ||
		journal[0].Call.Name != callAgentInput ||
		journal[1].Call.Name != "openai.chat" ||
		journal[2].Call.Name != callAgentFinish {
		t.Fatalf("journal = %+v", journal)
	}

	dispatchers.mu.Lock()
	defer dispatchers.mu.Unlock()
	if len(dispatchers.manifests) != 1 ||
		string(dispatchers.manifests[0].Capabilities[0].Settings) != `{"value":2}` {
		t.Fatalf("dispatcher manifests = %+v", dispatchers.manifests)
	}
}

func TestRuntimeRejectsPersistedBrainDigestMismatch(t *testing.T) {
	store := newRuntimeStore()
	now := time.Now().UTC()
	store.state = StoredState{
		Threads: []StoredThread{{
			TenantID: "local", ID: "thread", CreatedAt: now, UpdatedAt: now,
			Manifest: Manifest{Version: ManifestVersion, Brain: "brain@1"},
		}},
		Runs: []StoredRun{{
			TenantID: "local", ID: "run", ThreadID: "thread", Revision: 1,
			Status: RunCompleted, CreatedAt: now, UpdatedAt: now,
			EffectiveManifest: Manifest{Version: ManifestVersion, Brain: "brain@1"},
			BrainDigest:       "different",
		}},
	}
	_, err := NewRuntime(context.Background(), Config{
		Brains: staticBrains{
			defaultID: "brain@1",
			sources:   []BrainSource{{ID: "brain@1", Wasm: []byte("wasm")}},
		},
		Dispatchers:  &runtimeDispatchers{},
		StateStore:   store,
		TaskStore:    store,
		SessionStore: newRuntimeSessions(),
		TaskSecret:   []byte("stable-secret"),
	})
	if err == nil {
		t.Fatal("expected persisted brain digest mismatch")
	}
}

func waitForStatus(t *testing.T, runtime *Runtime, runID string, want RunStatus) RunSnapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run, err := runtime.GetRun(runID)
		if err != nil {
			t.Fatalf("get run: %v", err)
		}
		if run.Status == want {
			return run
		}
		if run.Status == RunFailed {
			t.Fatalf("run failed: %s", run.Error)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run did not reach %s", want)
	return RunSnapshot{}
}

func sequentialIDs() func(string) (string, error) {
	var next atomic.Int32
	return func(prefix string) (string, error) {
		return fmt.Sprintf("%s%d", prefix, next.Add(1)), nil
	}
}

func buildBrain(t *testing.T) []byte {
	t.Helper()
	wasmPath := filepath.Join(t.TempDir(), "agent.wasm")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "tinygo", "build",
		"-target", "wasip1",
		"-buildmode=c-shared",
		"-tags", "tinygo",
		"-o", wasmPath,
		"./agent",
	)
	cmd.Dir = "../../../aurora-brains"
	cmd.Env = append(os.Environ(),
		"XDG_CACHE_HOME="+t.TempDir(),
		"GOCACHE="+filepath.Join(t.TempDir(), "go-build"),
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build brain: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Fatalf("read brain: %v", err)
	}
	return raw
}

// cascadeDispatchers drives a parent brain to delegate to a "child" once and
// then finish. The openai.chat fake decides what to emit by inspecting the
// conversation: the child's own turn (whose user message is the delegated task)
// finishes immediately; the parent's first turn delegates; its second turn (which
// now carries a tool observation) finishes.
type cascadeDispatchers struct{}

func (cascadeDispatchers) Normalize(_ string, settings json.RawMessage) (json.RawMessage, error) {
	if len(settings) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return append(json.RawMessage(nil), settings...), nil
}

func (cascadeDispatchers) NewDispatcher(_ context.Context, _ RunContext, _ Manifest) (dispatcher.Dispatcher[RunContext], error) {
	return cascadeDispatcher{}, nil
}

func (cascadeDispatchers) IsSubset(_ string, _, _ json.RawMessage) error { return nil }

type cascadeDispatcher struct{}

func chatActions(actions string) dispatcher.Outcome {
	payload, _ := json.Marshal(map[string]any{
		"choices": []any{map[string]any{"message": map[string]any{"content": actions}}},
	})
	return dispatcher.Result(payload)
}

func (cascadeDispatcher) Dispatch(_ context.Context, _ RunContext, call dispatcher.Call) (dispatcher.Outcome, error) {
	if call.Name != "openai.chat" {
		return dispatcher.Failed("unsupported call: " + call.Name), nil
	}
	var req struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	_ = json.Unmarshal(call.Args, &req)
	isChild, hasTool := false, false
	for _, m := range req.Messages {
		if m.Role == "user" && strings.Contains(m.Content, "do subtask") {
			isChild = true
		}
		if m.Role == "tool" {
			hasTool = true
		}
	}
	switch {
	case isChild:
		return chatActions(`{"actions":[{"action":"final","content":{"answer":"child-done"}}]}`), nil
	case hasTool:
		return chatActions(`{"actions":[{"action":"final","content":{"answer":"parent-done"}}]}`), nil
	default:
		return chatActions(`{"actions":[{"action":"call.child","content":{"message":"do subtask"}}]}`), nil
	}
}

func onlyChildRun(t *testing.T, r *Runtime, parentID string) string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	parent := r.runs[parentID]
	if parent == nil || len(parent.childRunIDs) != 1 {
		t.Fatalf("parent %q childRunIDs = %v, want exactly one", parentID, parentChildIDs(parent))
	}
	return parent.childRunIDs[0]
}

func parentChildIDs(run *runState) []string {
	if run == nil {
		return nil
	}
	return run.childRunIDs
}

func runField(t *testing.T, r *Runtime, id string) (parentRunID string, attempt int) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	run := r.runs[id]
	if run == nil {
		t.Fatalf("run %q not found", id)
	}
	return run.parentRunID, run.attempt
}

func TestRuntimeCascadeResumeReusesChildRun(t *testing.T) {
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}
	store := newRuntimeStore()
	runtime, err := NewRuntime(context.Background(), Config{
		Brains: staticBrains{
			defaultID: "brain@1",
			sources:   []BrainSource{{ID: "brain@1", Wasm: buildBrain(t)}},
		},
		Dispatchers:  cascadeDispatchers{},
		StateStore:   store,
		TaskStore:    store,
		SessionStore: newRuntimeSessions(),
		TaskSecret:   []byte("stable-secret"),
		IDSource:     sequentialIDs(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	thread, err := runtime.CreateThread(Manifest{
		Version:  ManifestVersion,
		Brain:    "brain@1",
		Children: []ChildManifest{{Name: "child", Brain: "brain@1", Capabilities: []CapabilityConfig{}}},
	})
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	run, err := runtime.CreateRun(thread.ID, "parent task", nil)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	first := waitForStatus(t, runtime, run.ID, RunCompleted)
	if first.Answer != "parent-done" {
		t.Fatalf("parent answer = %q, want parent-done", first.Answer)
	}

	// Addressability: the parent recorded exactly one child, and that child links
	// back to the parent.
	childID := onlyChildRun(t, runtime, run.ID)
	childParent, childAttempt := runField(t, runtime, childID)
	if childParent != run.ID {
		t.Fatalf("child.parentRunID = %q, want %q", childParent, run.ID)
	}

	// Call-graph projection: the parent run projects to a tree with the child
	// beneath it, linked back to the parent.
	graph, err := runtime.CallGraph(run.ID)
	if err != nil {
		t.Fatalf("call graph: %v", err)
	}
	if graph.RunID != run.ID || len(graph.Children) != 1 || graph.Children[0].RunID != childID {
		t.Fatalf("call graph = %+v, want root %s with single child %s", graph, run.ID, childID)
	}
	if graph.Children[0].ParentID != run.ID {
		t.Fatalf("child node ParentID = %q, want %q", graph.Children[0].ParentID, run.ID)
	}

	// Deep cascade resume: restarting the parent must reuse and retry the same
	// child run rather than spawning a fresh one.
	if _, err := runtime.Retry(run.ID, RetryRestart, nil); err != nil {
		t.Fatalf("retry parent: %v", err)
	}
	waitForStatus(t, runtime, run.ID, RunCompleted)

	reusedChildID := onlyChildRun(t, runtime, run.ID)
	if reusedChildID != childID {
		t.Fatalf("cascade spawned a new child %q, want reuse of %q", reusedChildID, childID)
	}
	if _, attempt := runField(t, runtime, childID); attempt <= childAttempt {
		t.Fatalf("child attempt = %d, want > %d (child should have been retried)", attempt, childAttempt)
	}
}

// failingChildDispatchers makes a parent delegate once to a child whose brain
// then requests an unavailable capability, failing the child run.
type failingChildDispatchers struct{}

func (failingChildDispatchers) Normalize(_ string, settings json.RawMessage) (json.RawMessage, error) {
	if len(settings) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return append(json.RawMessage(nil), settings...), nil
}

func (failingChildDispatchers) NewDispatcher(_ context.Context, _ RunContext, _ Manifest) (dispatcher.Dispatcher[RunContext], error) {
	return failingChildDispatcher{}, nil
}

func (failingChildDispatchers) IsSubset(_ string, _, _ json.RawMessage) error { return nil }

type failingChildDispatcher struct{}

func (failingChildDispatcher) Dispatch(_ context.Context, _ RunContext, call dispatcher.Call) (dispatcher.Outcome, error) {
	if call.Name != "openai.chat" {
		return dispatcher.Failed("unsupported call: " + call.Name), nil
	}
	var req struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	_ = json.Unmarshal(call.Args, &req)
	for _, m := range req.Messages {
		if m.Role == "user" && strings.Contains(m.Content, "do subtask") {
			// The child requests a capability it was not granted; the brain
			// rejects it and the child run fails.
			return chatActions(`{"actions":[{"action":"missing.tool","content":{}}]}`), nil
		}
	}
	return chatActions(`{"actions":[{"action":"call.child","content":{"message":"do subtask"}}]}`), nil
}

func TestRuntimeChildFailurePropagatesToParent(t *testing.T) {
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}
	store := newRuntimeStore()
	runtime, err := NewRuntime(context.Background(), Config{
		Brains: staticBrains{
			defaultID: "brain@1",
			sources:   []BrainSource{{ID: "brain@1", Wasm: buildBrain(t)}},
		},
		Dispatchers:  failingChildDispatchers{},
		StateStore:   store,
		TaskStore:    store,
		SessionStore: newRuntimeSessions(),
		TaskSecret:   []byte("stable-secret"),
		IDSource:     sequentialIDs(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	thread, err := runtime.CreateThread(Manifest{
		Version: ManifestVersion,
		Brain:   "brain@1",
		Children: []ChildManifest{{
			Name: "child", Brain: "brain@1", Capabilities: []CapabilityConfig{},
			OnFailure: OnFailurePropagate,
		}},
	})
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	run, err := runtime.CreateRun(thread.ID, "parent task", nil)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	// With OnFailurePropagate, the failed child fails the parent run rather than
	// surfacing as a recoverable observation.
	failed := waitForStatus(t, runtime, run.ID, RunFailed)
	if !strings.Contains(failed.Error, "child") {
		t.Fatalf("parent error = %q, want it to mention the failed child", failed.Error)
	}
}
