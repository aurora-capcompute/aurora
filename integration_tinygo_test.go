package aurora_capcompute_test

import (
	"capcompute"
	"capcompute/session_store_memory"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

func (r integrationRun) SessionKey() string {
	return r.id
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
	store := session_store_memory.New[string, integrationRun]()
	compute, err := capcompute.NewComputeCompiledPlugin[string, integrationRun](ctx, capcompute.Config[string, integrationRun]{
		Manifest: extism.Manifest{
			Wasm: []extism.Wasm{extism.WasmFile{Path: wasmPath}},
		},
		PluginConfig: extism.PluginConfig{
			EnableWasi: true,
		},
		Dispatchers: internalhost.Factory[integrationRun]{
			LLM:      llm.NewFakeClient(server.URL),
			Internet: internet.NewClient(policy),
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
		Message  string `json:"message"`
		MaxSteps int    `json:"max_steps"`
	}{
		Message:  "read the configured page",
		MaxSteps: 4,
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

	results, err := compute.Play(ctx, session)
	if err != nil {
		t.Fatalf("play: %v", err)
	}
	result := <-results
	if result.Status != capcompute.PlayCompleted || result.Err != nil {
		t.Fatalf("result = %+v output=%s", result, result.Output)
	}
	if !strings.Contains(string(result.Output), "tinygo integration content") {
		t.Fatalf("output does not include read content: %s", result.Output)
	}
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
