package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"aurora-capcompute/internal/agent"
	"aurora-capcompute/internal/internet"
	"aurora-capcompute/internal/llm"
)

type finalLLM struct{}

func (finalLLM) Chat(context.Context, llm.ChatRequest) (llm.ChatResponse, error) {
	return llm.ChatResponse{Content: `[{"action":"final","content":{"answer":"server answer"}}]`}, nil
}

type unusedInternet struct{}

func (unusedInternet) Read(context.Context, internet.ReadRequest) (internet.ReadResponse, error) {
	return internet.ReadResponse{}, fmt.Errorf("internet should not be called")
}

func TestRESTAndSSELifecycle(t *testing.T) {
	runtime, err := agent.NewRuntime(context.Background(), agent.Config{
		WasmPath: buildGuest(t),
		LLM:      finalLLM{},
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
	httpServer := httptest.NewServer(New(runtime).Handler())
	defer httpServer.Close()

	thread := requestJSON[agent.ThreadSnapshot](t, http.MethodPost, httpServer.URL+"/v1/threads", nil, http.StatusCreated)
	if thread.ID == "" {
		t.Fatal("thread id is empty")
	}

	response, err := http.Get(httpServer.URL + "/v1/threads/" + thread.ID + "/events")
	if err != nil {
		t.Fatalf("open events: %v", err)
	}
	reader := bufio.NewReader(response.Body)
	eventLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read event line: %v", err)
	}
	if strings.TrimSpace(eventLine) != "event: snapshot" {
		t.Fatalf("event line = %q, want snapshot", eventLine)
	}
	_ = response.Body.Close()

	run := requestJSON[agent.RunSnapshot](t, http.MethodPost,
		httpServer.URL+"/v1/threads/"+thread.ID+"/messages",
		map[string]string{"content": "hello"},
		http.StatusAccepted,
	)
	completed := waitForRun(t, httpServer.URL, run.ID)
	if completed.Answer != "server answer" {
		t.Fatalf("answer = %q", completed.Answer)
	}

	journal := requestJSON[struct {
		Entries []agent.JournalEntry `json:"entries"`
	}](t, http.MethodGet, httpServer.URL+"/v1/runs/"+run.ID+"/journal", nil, http.StatusOK)
	if len(journal.Entries) != 1 || journal.Entries[0].Call.Name != "llm.chat" {
		t.Fatalf("journal = %+v", journal.Entries)
	}

	gotThread := requestJSON[agent.ThreadSnapshot](t, http.MethodGet,
		httpServer.URL+"/v1/threads/"+thread.ID, nil, http.StatusOK)
	if len(gotThread.History) != 2 {
		t.Fatalf("history length = %d, want 2", len(gotThread.History))
	}
}

func requestJSON[T any](t *testing.T, method string, target string, body any, wantStatus int) T {
	t.Helper()
	var requestBody bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&requestBody).Encode(body); err != nil {
			t.Fatalf("encode request: %v", err)
		}
	}
	request, err := http.NewRequest(method, target, &requestBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		var failure any
		_ = json.NewDecoder(response.Body).Decode(&failure)
		t.Fatalf("status = %d, want %d; body=%v", response.StatusCode, wantStatus, failure)
	}
	var value T
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return value
}

func waitForRun(t *testing.T, baseURL string, runID string) agent.RunSnapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run := requestJSON[agent.RunSnapshot](t, http.MethodGet, baseURL+"/v1/runs/"+runID, nil, http.StatusOK)
		if run.Status == agent.RunCompleted {
			return run
		}
		if run.Status == agent.RunFailed {
			t.Fatalf("run failed: %s", run.Error)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("run did not complete")
	return agent.RunSnapshot{}
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
