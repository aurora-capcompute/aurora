package agent

import (
	"context"
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

	"aurora-capcompute/internal/internet"
	"aurora-capcompute/internal/llm"
)

type finalLLM struct {
	mu       sync.Mutex
	requests []llm.ChatRequest
}

func (c *finalLLM) Chat(_ context.Context, request llm.ChatRequest) (llm.ChatResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, request)
	c.mu.Unlock()
	return llm.ChatResponse{Content: `[{"action":"final","content":{"answer":"done"}}]`}, nil
}

func (c *finalLLM) Requests() []llm.ChatRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]llm.ChatRequest(nil), c.requests...)
}

type unusedInternet struct{}

func (unusedInternet) Read(context.Context, internet.ReadRequest) (internet.ReadResponse, error) {
	return internet.ReadResponse{}, fmt.Errorf("internet should not be called")
}

func TestRuntimeCarriesOnlyCompactCompletedHistory(t *testing.T) {
	model := &finalLLM{}
	runtime := newTestRuntime(t, model)
	thread, err := runtime.CreateThread()
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}

	first, err := runtime.CreateRun(thread.ID, "first question")
	if err != nil {
		t.Fatalf("create first run: %v", err)
	}
	waitForStatus(t, runtime, first.ID, RunCompleted)

	second, err := runtime.CreateRun(thread.ID, "second question")
	if err != nil {
		t.Fatalf("create second run: %v", err)
	}
	waitForStatus(t, runtime, second.ID, RunCompleted)

	requests := model.Requests()
	if len(requests) != 2 {
		t.Fatalf("llm requests = %d, want 2", len(requests))
	}
	messages := requests[1].Messages
	if len(messages) != 4 {
		t.Fatalf("second request messages = %d, want system + prior pair + current user", len(messages))
	}
	want := []struct {
		role    string
		content string
	}{
		{role: "system"},
		{role: "user", content: "first question"},
		{role: "assistant", content: "done"},
		{role: "user", content: "second question"},
	}
	for i, expected := range want {
		if messages[i].Role != expected.role {
			t.Fatalf("message %d role = %q, want %q", i, messages[i].Role, expected.role)
		}
		if expected.content != "" && messages[i].Content != expected.content {
			t.Fatalf("message %d content = %q, want %q", i, messages[i].Content, expected.content)
		}
		if messages[i].Role == "tool" {
			t.Fatalf("message %d unexpectedly contains tool history", i)
		}
	}

	snapshot, err := runtime.GetThread(thread.ID)
	if err != nil {
		t.Fatalf("get thread: %v", err)
	}
	if len(snapshot.History) != 4 {
		t.Fatalf("history entries = %d, want 4", len(snapshot.History))
	}
}

type stopThenFinishLLM struct {
	calls   atomic.Int32
	started chan struct{}
}

func (c *stopThenFinishLLM) Chat(ctx context.Context, _ llm.ChatRequest) (llm.ChatResponse, error) {
	if c.calls.Add(1) == 1 {
		close(c.started)
		<-ctx.Done()
		return llm.ChatResponse{}, ctx.Err()
	}
	return llm.ChatResponse{Content: `[{"action":"final","content":{"answer":"recovered"}}]`}, nil
}

func TestRuntimeStopsAndResumesRun(t *testing.T) {
	model := &stopThenFinishLLM{started: make(chan struct{})}
	runtime := newTestRuntime(t, model)
	thread, err := runtime.CreateThread()
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	run, err := runtime.CreateRun(thread.ID, "long request")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	select {
	case <-model.started:
	case <-time.After(5 * time.Second):
		t.Fatal("llm call did not start")
	}
	if _, err := runtime.CreateRun(thread.ID, "conflicting request"); !errors.Is(err, ErrConflict) {
		t.Fatalf("concurrent run error = %v, want conflict", err)
	}
	if _, err := runtime.Stop(run.ID); err != nil {
		t.Fatalf("stop run: %v", err)
	}
	waitForStatus(t, runtime, run.ID, RunStopped)

	retried, err := runtime.Retry(run.ID, RetryResume)
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if retried.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", retried.Attempt)
	}
	completed := waitForStatus(t, runtime, run.ID, RunCompleted)
	if completed.Answer != "recovered" {
		t.Fatalf("answer = %q, want recovered", completed.Answer)
	}
}

func newTestRuntime(t *testing.T, model llm.Client) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(context.Background(), Config{
		WasmPath: buildGuest(t),
		LLM:      model,
		Internet: unusedInternet{},
		IDSource: sequentialIDs(),
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
	return runtime
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
		if run.Status == RunFailed && want != RunFailed {
			t.Fatalf("run failed: %s", run.Error)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach %s", runID, want)
	return RunSnapshot{}
}

func sequentialIDs() func(string) (string, error) {
	var next atomic.Int32
	return func(prefix string) (string, error) {
		return fmt.Sprintf("%s%d", prefix, next.Add(1)), nil
	}
}

func buildGuest(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}
	wasmPath := filepath.Join(t.TempDir(), "agent.wasm")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx,
		"tinygo", "build",
		"-target", "wasip1",
		"-buildmode=c-shared",
		"-tags", "tinygo",
		"-o", wasmPath,
		"../../guest",
	)
	cmd.Env = append(os.Environ(),
		"XDG_CACHE_HOME="+t.TempDir(),
		"GOCACHE="+filepath.Join(t.TempDir(), "go-build"),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build guest: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	return wasmPath
}
