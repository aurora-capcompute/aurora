package aurora_capcompute_test

import (
	"capcompute"
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay"
	"capcompute/dispatcher/replay/tape/journaled"
	"capcompute/dispatcher/replay/tape/journaled/journal/memory"
	"capcompute/session_store_memory"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalhost "aurora-capcompute/internal/host"
	"aurora-capcompute/internal/internet"
	"aurora-capcompute/internal/llm"
	extism "github.com/extism/go-sdk"
)

type integrationRun struct {
	id string
}

var integrationInternetCapability = dispatcher.Capability{
	Name:        "internet.read",
	Description: "Read textual content with HTTP GET.",
	InputSchema: json.RawMessage(`{"type":"object"}`),
}

func (r integrationRun) SessionKey() string {
	return r.id
}

type batchLLM struct {
	urls  []string
	calls atomic.Int32
}

func (c *batchLLM) Chat(_ context.Context, request llm.ChatRequest) (llm.ChatResponse, error) {
	switch c.calls.Add(1) {
	case 1:
		actions := make([]map[string]any, 0, len(c.urls))
		for _, target := range c.urls {
			actions = append(actions, map[string]any{
				"action": "internet.read",
				"content": map[string]string{
					"method": "GET",
					"url":    target,
					"reason": "batch integration test",
				},
			})
		}
		first, err := json.Marshal(actions[0])
		if err != nil {
			return llm.ChatResponse{}, err
		}
		second, err := json.Marshal(actions[1])
		if err != nil {
			return llm.ChatResponse{}, err
		}
		stream := string(first) + "\n" + string(second)
		stream += "\n" + `{"action":"final","content":{"answer":"premature answer before tool observations"}}`
		stream += "\n" + `{"error":"invalid_format","message":"Expected a JSON array of actions"}`
		encoded, err := json.Marshal(stream)
		return llm.ChatResponse{Content: string(encoded)}, err
	case 2:
		var toolContent string
		for _, message := range request.Messages {
			if message.Role == "tool" {
				toolContent = message.Content
			}
		}
		var observations []struct {
			Action  string                `json:"action"`
			Content internet.ReadResponse `json:"content"`
		}
		if err := json.Unmarshal([]byte(toolContent), &observations); err != nil {
			return llm.ChatResponse{}, fmt.Errorf("decode aggregate observations: %w", err)
		}
		if len(observations) != 2 ||
			observations[0].Action != "internet.read" ||
			observations[1].Action != "internet.read" ||
			observations[0].Content.Body != "first source" ||
			observations[1].Content.Body != "second source" {
			return llm.ChatResponse{}, fmt.Errorf("unexpected aggregate observations: %+v", observations)
		}
		return llm.ChatResponse{
			Content: `{"actions":[{"action":"final","content":{"answer":"combined both sources"}}]}`,
		}, nil
	default:
		return llm.ChatResponse{}, fmt.Errorf("unexpected llm call")
	}
}

type failureAwareLLM struct {
	calls atomic.Int32
}

func (c *failureAwareLLM) Chat(_ context.Context, request llm.ChatRequest) (llm.ChatResponse, error) {
	switch c.calls.Add(1) {
	case 1:
		return llm.ChatResponse{Content: `{"actions":[
			{"action":"internet.read","content":{"method":"GET","url":"https://working.test"}},
			{"action":"internet.read","content":{"method":"GET","url":"https://timeout.test"}}
		]}`}, nil
	case 2:
		var toolContent string
		for _, message := range request.Messages {
			if message.Role == "tool" {
				toolContent = message.Content
			}
		}
		var observations []struct {
			Action  string                `json:"action"`
			Status  string                `json:"status"`
			Content internet.ReadResponse `json:"content"`
			Error   string                `json:"error"`
		}
		if err := json.Unmarshal([]byte(toolContent), &observations); err != nil {
			return llm.ChatResponse{}, fmt.Errorf("decode failure observations: %w", err)
		}
		if len(observations) != 2 ||
			observations[0].Status != "result" ||
			observations[0].Content.Body != "usable source" ||
			observations[1].Status != "failed" ||
			!strings.Contains(observations[1].Error, "context deadline exceeded") {
			return llm.ChatResponse{}, fmt.Errorf("unexpected failure observations: %+v", observations)
		}
		return llm.ChatResponse{
			Content: `{"actions":[{"action":"final","content":{"answer":"answered from the usable source"}}]}`,
		}, nil
	default:
		return llm.ChatResponse{}, fmt.Errorf("unexpected llm call")
	}
}

type partiallyFailingReader struct{}

func (partiallyFailingReader) Read(_ context.Context, request internet.ReadRequest) (internet.ReadResponse, error) {
	if request.URL == "https://timeout.test" {
		return internet.ReadResponse{}, fmt.Errorf(
			`Get "https://timeout.test": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`,
		)
	}
	return internet.ReadResponse{
		URL:         request.URL,
		Status:      http.StatusOK,
		ContentType: "text/plain",
		Body:        "usable source",
	}, nil
}

func TestTinyGoGuestThroughCapcomputeWithFakeLLM(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TinyGo integration test in short mode")
	}
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("tinygo integration content"))
	}))
	defer server.Close()

	policy, err := internet.ParseAllowlist("GET:" + server.URL)
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}

	ctx := context.Background()
	wasmPath := buildGuest(t)
	journal := memory.NewJournal()
	store := session_store_memory.New[string, integrationRun]()
	compute, err := capcompute.NewComputeCompiledPlugin[string, integrationRun](ctx, capcompute.Config[string, integrationRun]{
		Manifest: extism.Manifest{
			Wasm: []extism.Wasm{extism.WasmFile{Path: wasmPath}},
		},
		PluginConfig: extism.PluginConfig{
			EnableWasi: true,
		},
		Dispatchers: internalhost.Factory[integrationRun]{
			LLM:          llm.NewFakeClient(server.URL),
			Internet:     internet.NewClient(policy),
			Capabilities: []dispatcher.Capability{integrationInternetCapability},
			NewTape: func(context.Context, integrationRun) (replay.Tape, error) {
				return journaled.NewTape(journal), nil
			},
		},
		SessionStore: store,
	})
	if err != nil {
		t.Fatalf("new compute plugin: %v", err)
	}
	t.Cleanup(func() {
		if err := compute.CloseCompiled(context.Background()); err != nil {
			t.Errorf("close compiled: %v", err)
		}
	})

	input, err := json.Marshal(struct {
		Message      string                  `json:"message"`
		Capabilities []dispatcher.Capability `json:"capabilities"`
	}{
		Message:      "read the configured page",
		Capabilities: []dispatcher.Capability{integrationInternetCapability},
	})
	if err != nil {
		t.Fatalf("encode input: %v", err)
	}
	run := integrationRun{id: "integration"}
	session, err := compute.CreateSession(ctx, capcompute.PlayRequest[string, integrationRun]{
		Input:      input,
		Entrypoint: "run",
		UserData:   run,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close session: %v", err)
		}
	})
	if err := store.SaveSession(ctx, run.SessionKey(), session); err != nil {
		t.Fatalf("save session: %v", err)
	}

	handle, err := compute.Play(ctx, session)
	if err != nil {
		t.Fatalf("play: %v", err)
	}
	result := <-handle.Results()
	if result.Status != capcompute.PlayCompleted || result.Err != nil {
		t.Fatalf("result = %+v output=%s", result, result.Output)
	}
	if !strings.Contains(string(result.Output), "tinygo integration content") {
		t.Fatalf("output does not include read content: %s", result.Output)
	}
	assertRecordedCalls(t, journal, []string{"llm.chat", "internet.read", "llm.chat"})
}

func TestTinyGoGuestExecutesAndAggregatesActionBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TinyGo integration test in short mode")
	}
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		switch r.URL.Path {
		case "/first":
			_, _ = w.Write([]byte("first source"))
		case "/second":
			_, _ = w.Write([]byte("second source"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	policy, err := internet.ParseAllowlist("GET:" + server.URL)
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}

	ctx := context.Background()
	journal := memory.NewJournal()
	model := &batchLLM{urls: []string{server.URL + "/first", server.URL + "/second"}}
	store := session_store_memory.New[string, integrationRun]()
	compute, err := capcompute.NewComputeCompiledPlugin[string, integrationRun](ctx, capcompute.Config[string, integrationRun]{
		Manifest: extism.Manifest{
			Wasm: []extism.Wasm{extism.WasmFile{Path: buildGuest(t)}},
		},
		PluginConfig: extism.PluginConfig{
			EnableWasi: true,
		},
		Dispatchers: internalhost.Factory[integrationRun]{
			LLM:          model,
			Internet:     internet.NewClient(policy),
			Capabilities: []dispatcher.Capability{integrationInternetCapability},
			NewTape: func(context.Context, integrationRun) (replay.Tape, error) {
				return journaled.NewTape(journal), nil
			},
		},
		SessionStore: store,
	})
	if err != nil {
		t.Fatalf("new compute plugin: %v", err)
	}
	t.Cleanup(func() {
		if err := compute.CloseCompiled(context.Background()); err != nil {
			t.Errorf("close compiled: %v", err)
		}
	})

	run := integrationRun{id: "batch"}
	session, err := compute.CreateSession(ctx, capcompute.PlayRequest[string, integrationRun]{
		Input:      mustAgentInput(t, "combine both sources"),
		Entrypoint: "run",
		UserData:   run,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close session: %v", err)
		}
	})
	if err := store.SaveSession(ctx, run.SessionKey(), session); err != nil {
		t.Fatalf("save session: %v", err)
	}

	handle, err := compute.Play(ctx, session)
	if err != nil {
		t.Fatalf("play: %v", err)
	}
	result := <-handle.Results()
	if result.Status != capcompute.PlayCompleted || result.Err != nil {
		t.Fatalf("result = %+v output=%s", result, result.Output)
	}
	if !strings.Contains(string(result.Output), "combined both sources") {
		t.Fatalf("output does not contain final answer: %s", result.Output)
	}
	assertRecordedCalls(t, journal, []string{"llm.chat", "internet.read", "internet.read", "llm.chat"})
}

func TestTinyGoGuestReturnsCapabilityFailureToModel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TinyGo integration test in short mode")
	}
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}

	ctx := context.Background()
	journal := memory.NewJournal()
	model := &failureAwareLLM{}
	store := session_store_memory.New[string, integrationRun]()
	compute, err := capcompute.NewComputeCompiledPlugin[string, integrationRun](ctx, capcompute.Config[string, integrationRun]{
		Manifest: extism.Manifest{
			Wasm: []extism.Wasm{extism.WasmFile{Path: buildGuest(t)}},
		},
		PluginConfig: extism.PluginConfig{EnableWasi: true},
		Dispatchers: internalhost.Factory[integrationRun]{
			LLM:          model,
			Internet:     partiallyFailingReader{},
			Capabilities: []dispatcher.Capability{integrationInternetCapability},
			NewTape: func(context.Context, integrationRun) (replay.Tape, error) {
				return journaled.NewTape(journal), nil
			},
		},
		SessionStore: store,
	})
	if err != nil {
		t.Fatalf("new compute plugin: %v", err)
	}
	t.Cleanup(func() {
		if err := compute.CloseCompiled(context.Background()); err != nil {
			t.Errorf("close compiled: %v", err)
		}
	})

	run := integrationRun{id: "capability-failure"}
	session, err := compute.CreateSession(ctx, capcompute.PlayRequest[string, integrationRun]{
		Input:      mustAgentInput(t, "research with both sources"),
		Entrypoint: "run",
		UserData:   run,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close session: %v", err)
		}
	})
	if err := store.SaveSession(ctx, run.SessionKey(), session); err != nil {
		t.Fatalf("save session: %v", err)
	}

	handle, err := compute.Play(ctx, session)
	if err != nil {
		t.Fatalf("play: %v", err)
	}
	result := <-handle.Results()
	if result.Status != capcompute.PlayCompleted || result.Err != nil {
		t.Fatalf("result = %+v output=%s", result, result.Output)
	}
	if !strings.Contains(string(result.Output), "answered from the usable source") {
		t.Fatalf("output does not contain final answer: %s", result.Output)
	}
	// The replay tape records successful results only. The failed read is
	// deliberately not cached, but the guest still returns it to the model.
	assertRecordedCalls(t, journal, []string{"llm.chat", "internet.read", "llm.chat"})
}

func assertRecordedCalls(t *testing.T, journal *memory.Journal, want []string) {
	t.Helper()

	if got := journal.Length(); got != len(want) {
		t.Fatalf("recorded calls = %d, want %d", got, len(want))
	}
	for i, wantName := range want {
		record, err := journal.Load(i)
		if err != nil {
			t.Fatalf("load record %d: %v", i, err)
		}
		if record.Call.Name != wantName {
			t.Fatalf("record %d call = %q, want %q", i, record.Call.Name, wantName)
		}
	}
}

type countingLLM struct {
	calls atomic.Int32
	next  llm.Client
}

func (c *countingLLM) Chat(ctx context.Context, request llm.ChatRequest) (llm.ChatResponse, error) {
	c.calls.Add(1)
	return c.next.Chat(ctx, request)
}

type interruptibleReader struct {
	calls   atomic.Int32
	started chan struct{}
	once    sync.Once
}

func (r *interruptibleReader) Read(ctx context.Context, request internet.ReadRequest) (internet.ReadResponse, error) {
	if r.calls.Add(1) == 1 {
		r.once.Do(func() { close(r.started) })
		<-ctx.Done()
		return internet.ReadResponse{}, ctx.Err()
	}
	return internet.ReadResponse{
		URL:         request.URL,
		Status:      http.StatusOK,
		ContentType: "text/plain",
		Body:        "recovered content",
	}, nil
}

func TestForceStopCanRecoverThroughRecreatedSessionAndJournal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TinyGo integration test in short mode")
	}
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}

	ctx := context.Background()
	wasmPath := buildGuest(t)
	journal := memory.NewJournal()
	reader := &interruptibleReader{started: make(chan struct{})}
	model := &countingLLM{next: llm.NewFakeClient("https://example.com")}
	store := session_store_memory.New[string, integrationRun]()
	compute, err := capcompute.NewComputeCompiledPlugin[string, integrationRun](ctx, capcompute.Config[string, integrationRun]{
		Manifest: extism.Manifest{
			Wasm: []extism.Wasm{extism.WasmFile{Path: wasmPath}},
		},
		PluginConfig: extism.PluginConfig{
			EnableWasi: true,
		},
		Dispatchers: internalhost.Factory[integrationRun]{
			LLM:          model,
			Internet:     reader,
			Capabilities: []dispatcher.Capability{integrationInternetCapability},
			NewTape: func(context.Context, integrationRun) (replay.Tape, error) {
				return journaled.NewTape(journal), nil
			},
		},
		SessionStore: store,
	})
	if err != nil {
		t.Fatalf("new compute plugin: %v", err)
	}
	t.Cleanup(func() {
		if err := compute.CloseCompiled(context.Background()); err != nil {
			t.Errorf("close compiled: %v", err)
		}
	})

	run := integrationRun{id: "recover"}
	request := capcompute.PlayRequest[string, integrationRun]{
		Input:      mustAgentInput(t, "read the configured page"),
		Entrypoint: "run",
		UserData:   run,
	}
	stoppedSession, err := compute.CreateSession(ctx, request)
	if err != nil {
		t.Fatalf("create stopped session: %v", err)
	}
	t.Cleanup(func() {
		if err := stoppedSession.Close(context.Background()); err != nil {
			t.Errorf("close stopped session: %v", err)
		}
	})
	if err := store.SaveSession(ctx, run.SessionKey(), stoppedSession); err != nil {
		t.Fatalf("save stopped session: %v", err)
	}

	handle, err := compute.Play(ctx, stoppedSession)
	if err != nil {
		t.Fatalf("play stopped session: %v", err)
	}
	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("internet call did not start")
	}
	handle.Stop()
	result := <-handle.Results()
	if result.Status != capcompute.PlayStopped || !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("stopped result = %+v", result)
	}

	recreated, err := compute.CreateSession(ctx, request)
	if err != nil {
		t.Fatalf("recreate session: %v", err)
	}
	t.Cleanup(func() {
		if err := recreated.Close(context.Background()); err != nil {
			t.Errorf("close recreated session: %v", err)
		}
	})
	if err := store.SaveSession(ctx, run.SessionKey(), recreated); err != nil {
		t.Fatalf("replace session: %v", err)
	}

	replayHandle, err := compute.Play(ctx, recreated)
	if err != nil {
		t.Fatalf("play recreated session: %v", err)
	}
	replayed := <-replayHandle.Results()
	if replayed.Status != capcompute.PlayCompleted || replayed.Err != nil {
		t.Fatalf("replayed result = %+v output=%s", replayed, replayed.Output)
	}
	if !strings.Contains(string(replayed.Output), "recovered content") {
		t.Fatalf("output does not contain recovered content: %s", replayed.Output)
	}
	if got := model.calls.Load(); got != 2 {
		t.Fatalf("llm calls = %d, want 2; first call should have replayed", got)
	}
	if got := reader.calls.Load(); got != 2 {
		t.Fatalf("internet calls = %d, want 2; interrupted call should retry", got)
	}
	assertRecordedCalls(t, journal, []string{"llm.chat", "internet.read", "llm.chat"})
}

type yieldOnceFactory struct {
	journal  *memory.Journal
	yielded  atomic.Bool
	llm      llm.Client
	internet internalhost.InternetReader
}

func (f *yieldOnceFactory) NewDispatcher(context.Context, integrationRun) (dispatcher.Dispatcher[integrationRun], error) {
	next := &yieldOnceDispatcher{
		yielded: &f.yielded,
		next: &internalhost.Dispatcher[integrationRun]{
			LLM:      f.llm,
			Internet: f.internet,
		},
	}
	return replay.NewDispatcher[integrationRun](journaled.NewTape(f.journal), next), nil
}

type yieldOnceDispatcher struct {
	yielded *atomic.Bool
	next    dispatcher.Dispatcher[integrationRun]
}

func (d *yieldOnceDispatcher) Dispatch(ctx context.Context, run integrationRun, call dispatcher.Call) (dispatcher.Outcome, error) {
	if d.yielded.CompareAndSwap(false, true) {
		return dispatcher.Yield("requested by host"), nil
	}
	return d.next.Dispatch(ctx, run, call)
}

func TestHostYieldKeepsAuroraSessionReplayable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TinyGo integration test in short mode")
	}
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("yielded then resumed"))
	}))
	defer server.Close()
	policy, err := internet.ParseAllowlist("GET:" + server.URL)
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}

	ctx := context.Background()
	journal := memory.NewJournal()
	factory := &yieldOnceFactory{
		journal:  journal,
		llm:      llm.NewFakeClient(server.URL),
		internet: internet.NewClient(policy),
	}
	store := session_store_memory.New[string, integrationRun]()
	compute, err := capcompute.NewComputeCompiledPlugin[string, integrationRun](ctx, capcompute.Config[string, integrationRun]{
		Manifest: extism.Manifest{
			Wasm: []extism.Wasm{extism.WasmFile{Path: buildGuest(t)}},
		},
		PluginConfig: extism.PluginConfig{
			EnableWasi: true,
		},
		Dispatchers:  factory,
		SessionStore: store,
	})
	if err != nil {
		t.Fatalf("new compute plugin: %v", err)
	}
	t.Cleanup(func() {
		if err := compute.CloseCompiled(context.Background()); err != nil {
			t.Errorf("close compiled: %v", err)
		}
	})

	run := integrationRun{id: "yield"}
	session, err := compute.CreateSession(ctx, capcompute.PlayRequest[string, integrationRun]{
		Input:      mustAgentInput(t, "read the configured page"),
		Entrypoint: "run",
		UserData:   run,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close session: %v", err)
		}
	})
	if err := store.SaveSession(ctx, run.SessionKey(), session); err != nil {
		t.Fatalf("save session: %v", err)
	}

	first, err := compute.Play(ctx, session)
	if err != nil {
		t.Fatalf("first play: %v", err)
	}
	if result := <-first.Results(); result.Status != capcompute.PlayYielded {
		t.Fatalf("first status = %s, want yielded; err=%v output=%s", result.Status, result.Err, result.Output)
	}

	second, err := compute.Play(ctx, session)
	if err != nil {
		t.Fatalf("second play: %v", err)
	}
	result := <-second.Results()
	if result.Status != capcompute.PlayCompleted || result.Err != nil {
		t.Fatalf("second result = %+v output=%s", result, result.Output)
	}
	if !strings.Contains(string(result.Output), "yielded then resumed") {
		t.Fatalf("output does not contain resumed content: %s", result.Output)
	}
	assertRecordedCalls(t, journal, []string{"llm.chat", "internet.read", "llm.chat"})
}

func mustAgentInput(t *testing.T, message string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"message":      message,
		"capabilities": []dispatcher.Capability{integrationInternetCapability},
	})
	if err != nil {
		t.Fatalf("marshal agent input: %v", err)
	}
	return raw
}

func buildGuest(t *testing.T) string {
	t.Helper()

	wasmPath := filepath.Join(t.TempDir(), "agent.wasm")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(
		ctx,
		"tinygo",
		"build",
		"-target", "wasip1",
		"-buildmode=c-shared",
		"-tags", "tinygo",
		"-o", wasmPath,
		"./guest",
	)
	cmd.Env = append(os.Environ(),
		"XDG_CACHE_HOME="+t.TempDir(),
		"GOCACHE="+filepath.Join(t.TempDir(), "go-build"),
	)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("build guest timed out: %v", ctx.Err())
	}
	if err != nil {
		t.Fatalf("build guest: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	return wasmPath
}
