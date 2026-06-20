package host_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	internalhost "aurora-capcompute/internal/host"
	"aurora-capcompute/internal/internet"
	"aurora-capcompute/internal/llm"
	"capcompute/dispatcher"
)

type run struct{}

func TestDispatcherLLMChatReturnsFakeContent(t *testing.T) {
	dispatch := &internalhost.Dispatcher[run]{
		LLM: llm.NewFakeClient("https://example.com"),
	}
	args := mustJSON(t, llm.ChatRequest{
		Messages: []llm.Message{{Role: "user", Content: "read"}},
		JSON:     true,
	})

	outcome, err := dispatch.Dispatch(context.Background(), run{}, dispatcher.Call{Name: "llm.chat", Args: args})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if outcome.Kind() != dispatcher.OutcomeResult {
		t.Fatalf("kind = %s, want result", outcome.Kind())
	}
	var response llm.ChatResponse
	if err := json.Unmarshal(outcome.Result(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	var batch struct {
		Actions []struct {
			Action  string `json:"action"`
			Content struct {
				Method string `json:"method"`
				URL    string `json:"url"`
			} `json:"content"`
		} `json:"actions"`
	}
	if err := json.Unmarshal([]byte(response.Content), &batch); err != nil {
		t.Fatalf("decode action: %v", err)
	}
	actions := batch.Actions
	if len(actions) != 1 || actions[0].Action != "internet.read" ||
		actions[0].Content.Method != http.MethodGet ||
		actions[0].Content.URL != "https://example.com" {
		t.Fatalf("actions = %+v", actions)
	}
}

func TestDispatcherInternetReadReturnsAllowedResource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("allowed"))
	}))
	defer server.Close()

	policy, err := internet.ParseAllowlist("GET:" + server.URL)
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}
	dispatch := &internalhost.Dispatcher[run]{
		Internet: internet.NewClient(policy),
	}
	args := mustJSON(t, internet.ReadRequest{Method: "GET", URL: server.URL})

	outcome, err := dispatch.Dispatch(context.Background(), run{}, dispatcher.Call{Name: "internet.read", Args: args})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if outcome.Kind() != dispatcher.OutcomeResult {
		t.Fatalf("kind = %s, want result", outcome.Kind())
	}
	var response internet.ReadResponse
	if err := json.Unmarshal(outcome.Result(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Body != "allowed" {
		t.Fatalf("body = %q", response.Body)
	}
}

func TestDispatcherUnknownCallReturnsFailedOutcome(t *testing.T) {
	dispatch := &internalhost.Dispatcher[run]{}

	outcome, err := dispatch.Dispatch(context.Background(), run{}, dispatcher.Call{Name: "missing.call"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if outcome.Kind() != dispatcher.OutcomeFailed {
		t.Fatalf("kind = %s, want failed", outcome.Kind())
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}
