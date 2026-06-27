package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aurora-capcompute/internal/eventlog"
	"capcompute"
	"capcompute/dispatcher"
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
	log    *eventlog.Memory
	mu     sync.Mutex
	leases map[string]string
}

func newRuntimeStore() *runtimeStore {
	return &runtimeStore{log: eventlog.NewMemory(), leases: make(map[string]string)}
}

func (s *runtimeStore) Acquire(_ context.Context, tenant, kind, resource, holder string, _ time.Time, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenant + "/" + kind + "/" + resource
	if current := s.leases[key]; current != "" && current != holder {
		return false, nil
	}
	s.leases[key] = holder
	return true, nil
}

func (s *runtimeStore) Release(_ context.Context, tenant, kind, resource, holder string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenant + "/" + kind + "/" + resource
	if s.leases[key] == holder {
		delete(s.leases, key)
	}
	return nil
}

// seed appends thread.state/run.state events so the runtime folds them on restore.
func (s *runtimeStore) seed(t StoredThread, runs ...StoredRun) {
	now := time.Now().UTC()
	ev, _ := threadStateEvent(now, t)
	_, _ = s.log.Append(context.Background(), eventlog.Scope{TenantID: t.TenantID, ThreadID: t.ID}, ev)
	for _, r := range runs {
		rev, _ := runStateEvent(now, r)
		_, _ = s.log.Append(context.Background(), eventlog.Scope{TenantID: r.TenantID, ThreadID: r.ThreadID}, rev)
	}
}

// lastForkOffset returns the offset of the most recent run.forked event, or 0.
func (s *runtimeStore) lastForkOffset() int {
	streams, _ := s.log.Streams(context.Background(), "local")
	offset := 0
	for _, scope := range streams {
		events, _ := s.log.Read(context.Background(), scope, 0)
		for _, ev := range events {
			if ev.Kind == evForked {
				var fd forkedData
				if json.Unmarshal(ev.Data, &fd) == nil {
					offset = fd.Offset
				}
			}
		}
	}
	return offset
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
		Brains: brains, Dispatchers: dispatchers, Log: store.log,
		Leases: store, SessionStore: sessions, TaskSecret: []byte("secret"),
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "dispatcher provider", mutate: func(config *Config) { config.Dispatchers = nil }},
		{name: "event log", mutate: func(config *Config) { config.Log = nil }},
		{name: "leases", mutate: func(config *Config) { config.Leases = nil }},
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
		Log:          store.log,
		Leases:       store,
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

// TestRuntimeSetBrainsLifecycle boots with no brain (so brain runs fail), then
// hot-registers a brain via SetBrains and drives a run through it, then removes
// it — covering the control plane's live Brain CRD add/remove path.
func TestRuntimeSetBrainsLifecycle(t *testing.T) {
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}
	wasm := buildBrain(t)
	dispatchers := &runtimeDispatchers{}
	store := newRuntimeStore()
	runtime, err := NewRuntime(context.Background(), Config{
		Brains:       nil, // boot with zero brains
		Dispatchers:  dispatchers,
		Log:          store.log,
		Leases:       store,
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

	if len(runtime.Brains()) != 0 {
		t.Fatalf("expected no brains at boot, got %v", runtime.Brains())
	}
	if _, err := runtime.CreateThread(Manifest{Version: ManifestVersion}); err == nil {
		t.Fatal("creating a thread with no registered brain should fail")
	}

	// Hot-register the brain.
	if err := runtime.SetBrains(context.Background(), []BrainSource{{ID: "brain@1", Wasm: wasm}}); err != nil {
		t.Fatalf("set brains: %v", err)
	}
	if got := runtime.Brains(); len(got) != 1 || got[0].ID != "brain@1" {
		t.Fatalf("brains after register = %v", got)
	}

	// A run now dispatches through the freshly registered brain.
	thread, err := runtime.CreateThread(Manifest{
		Version:      ManifestVersion,
		Capabilities: []CapabilityConfig{{Name: "custom.call", Settings: json.RawMessage(`{"value":1}`)}},
	})
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	run, err := runtime.CreateRun(thread.ID, "finish", nil)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	waitForStatus(t, runtime, run.ID, RunCompleted)

	// Re-applying the same set is a no-op; removing it leaves an empty registry.
	if err := runtime.SetBrains(context.Background(), []BrainSource{{ID: "brain@1", Wasm: wasm}}); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if len(runtime.Brains()) != 1 {
		t.Fatalf("re-apply changed registry: %v", runtime.Brains())
	}
	if err := runtime.SetBrains(context.Background(), nil); err != nil {
		t.Fatalf("clear brains: %v", err)
	}
	if len(runtime.Brains()) != 0 {
		t.Fatalf("expected empty registry after removal, got %v", runtime.Brains())
	}
}

func TestRuntimeRejectsPersistedBrainDigestMismatch(t *testing.T) {
	store := newRuntimeStore()
	now := time.Now().UTC()
	store.seed(
		StoredThread{
			TenantID: "local", ID: "thread", CreatedAt: now, UpdatedAt: now,
			Manifest: Manifest{Version: ManifestVersion, Brain: "brain@1"},
		},
		StoredRun{
			TenantID: "local", ID: "run", ThreadID: "thread", Revision: 1,
			Status: RunCompleted, CreatedAt: now, UpdatedAt: now,
			EffectiveManifest: Manifest{Version: ManifestVersion, Brain: "brain@1"},
			BrainDigest:       "different",
		},
	)
	_, err := NewRuntime(context.Background(), Config{
		Brains: staticBrains{
			defaultID: "brain@1",
			sources:   []BrainSource{{ID: "brain@1", Wasm: []byte("wasm")}},
		},
		Dispatchers:  &runtimeDispatchers{},
		Log:          store.log,
		Leases:       store,
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
		Log:          store.log,
		Leases:       store,
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
		Log:          store.log,
		Leases:       store,
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

// failThenSucceedDispatchers drives a run that does a tool call, then on its
// second turn requests an unavailable capability (failing the run) on the first
// attempt and finishes on the second. The shared counter persists across the
// run's attempts.
type failThenSucceedDispatchers struct {
	mu    sync.Mutex
	turn2 int
}

func (*failThenSucceedDispatchers) Normalize(_ string, settings json.RawMessage) (json.RawMessage, error) {
	if len(settings) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return append(json.RawMessage(nil), settings...), nil
}

func (d *failThenSucceedDispatchers) NewDispatcher(_ context.Context, _ RunContext, _ Manifest) (dispatcher.Dispatcher[RunContext], error) {
	return &failThenSucceedDispatcher{parent: d}, nil
}

func (*failThenSucceedDispatchers) IsSubset(_ string, _, _ json.RawMessage) error { return nil }

type failThenSucceedDispatcher struct{ parent *failThenSucceedDispatchers }

func (d *failThenSucceedDispatcher) Capabilities() []dispatcher.Capability {
	return []dispatcher.Capability{{
		Name:        "tool.x",
		Description: "test tool",
		InputSchema: json.RawMessage(`{"type":"object"}`),
	}}
}

func (d *failThenSucceedDispatcher) Dispatch(_ context.Context, _ RunContext, call dispatcher.Call) (dispatcher.Outcome, error) {
	switch call.Name {
	case "tool.x":
		return dispatcher.Result(json.RawMessage(`{"ok":true}`)), nil
	case "openai.chat":
		var req struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		_ = json.Unmarshal(call.Args, &req)
		hasTool := false
		for _, m := range req.Messages {
			if m.Role == "tool" {
				hasTool = true
			}
		}
		if !hasTool {
			return chatActions(`{"actions":[{"action":"tool.x","content":{}}]}`), nil
		}
		d.parent.mu.Lock()
		d.parent.turn2++
		n := d.parent.turn2
		d.parent.mu.Unlock()
		if n == 1 {
			// First attempt: request a capability that was not granted; the brain
			// rejects it and the run fails after several recorded steps.
			return chatActions(`{"actions":[{"action":"missing.tool","content":{}}]}`), nil
		}
		return chatActions(`{"actions":[{"action":"final","content":{"answer":"recovered"}}]}`), nil
	default:
		return dispatcher.Failed("unsupported call: " + call.Name), nil
	}
}

func TestRuntimeHardRetryForksFromFailurePoint(t *testing.T) {
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}
	store := newRuntimeStore()
	runtime, err := NewRuntime(context.Background(), Config{
		Brains: staticBrains{
			defaultID: "brain@1",
			sources:   []BrainSource{{ID: "brain@1", Wasm: buildBrain(t)}},
		},
		Dispatchers:  &failThenSucceedDispatchers{},
		Log:          store.log,
		Leases:       store,
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
		Version:      ManifestVersion,
		Brain:        "brain@1",
		Capabilities: []CapabilityConfig{{Name: "tool.x"}},
	})
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	run, err := runtime.CreateRun(thread.ID, "task", nil)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	failed := waitForStatus(t, runtime, run.ID, RunFailed)
	if failed.Error == "" {
		t.Fatal("expected a failure error")
	}

	// Hard retry must fork from the failure point over a shared copy-on-write
	// prefix, not re-run from scratch.
	if _, err := runtime.Retry(run.ID, RetryRestart, nil); err != nil {
		t.Fatalf("retry: %v", err)
	}
	recovered := waitForStatus(t, runtime, run.ID, RunCompleted)
	if recovered.Answer != "recovered" {
		t.Fatalf("answer = %q, want recovered", recovered.Answer)
	}
	off := store.lastForkOffset()
	if off <= 0 {
		t.Fatalf("fork offset = %d, want > 0 (hard retry should share the pre-failure prefix)", off)
	}

	// The thread graph exposes both revisions of the run, with revision 2 forked
	// from revision 1 at the failure point.
	graph, err := runtime.ThreadGraph(thread.ID)
	if err != nil {
		t.Fatalf("thread graph: %v", err)
	}
	if len(graph.Runs) != 1 {
		t.Fatalf("graph runs = %d, want 1", len(graph.Runs))
	}
	gr := graph.Runs[0]
	if gr.CurrentRevision != 2 || len(gr.Revisions) != 2 {
		t.Fatalf("revisions = %d, current = %d, want 2/2", len(gr.Revisions), gr.CurrentRevision)
	}
	if gr.Revisions[0].Forked {
		t.Fatal("revision 1 must not be a fork")
	}
	rev2 := gr.Revisions[1]
	if !rev2.Forked || rev2.ForkParent != 1 || rev2.ForkOffset != off {
		t.Fatalf("revision 2 fork meta = %+v, want forked from rev 1 at offset %d", rev2, off)
	}
	if len(gr.Revisions[0].Entries) == 0 || len(rev2.Entries) == 0 {
		t.Fatal("both revisions should expose journal entries")
	}
}
