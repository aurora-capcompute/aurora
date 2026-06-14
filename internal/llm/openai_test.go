package llm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"aurora-capcompute/internal/llm"
)

func TestOpenAIClientBuildsRequestAndParsesResponse(t *testing.T) {
	temperature := 0.2
	var got struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		ResponseFormat struct {
			Type string `json:"type"`
		} `json:"response_format"`
		MaxTokens   int      `json:"max_completion_tokens"`
		Temperature *float64 `json:"temperature"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("path = %s, want /chat/completions", r.URL.Path)
		}
		if gotAuth := r.Header.Get("Authorization"); gotAuth != "Bearer test-key" {
			t.Fatalf("authorization = %q", gotAuth)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"action\":\"final\",\"content\":{\"answer\":\"ok\"}}"}}]}`))
	}))
	defer server.Close()

	client, err := llm.NewOpenAIClient(llm.OpenAIConfig{
		APIKey:      "test-key",
		BaseURL:     server.URL,
		Model:       "test-model",
		MaxTokens:   123,
		Temperature: &temperature,
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	response, err := client.Chat(context.Background(), llm.ChatRequest{
		Messages: []llm.Message{
			{Role: "system", Content: "system"},
			{Role: "tool", Content: "observed"},
		},
		JSON: true,
	})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if response.Content != `{"action":"final","content":{"answer":"ok"}}` {
		t.Fatalf("content = %q", response.Content)
	}
	if got.Model != "test-model" {
		t.Fatalf("model = %q", got.Model)
	}
	if got.ResponseFormat.Type != "json_object" {
		t.Fatalf("response format = %q", got.ResponseFormat.Type)
	}
	if got.MaxTokens != 123 {
		t.Fatalf("max tokens = %d", got.MaxTokens)
	}
	if got.Temperature == nil || *got.Temperature != temperature {
		t.Fatalf("temperature = %v", got.Temperature)
	}
	if len(got.Messages) != 2 {
		t.Fatalf("messages = %d, want 2", len(got.Messages))
	}
	if got.Messages[1].Role != "user" || !strings.Contains(got.Messages[1].Content, "Tool observation") {
		t.Fatalf("tool message was not normalized: %+v", got.Messages[1])
	}
}

func TestOpenAIClientReportsProviderError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"message":"bad request"}}`, http.StatusBadRequest)
	}))
	defer server.Close()

	client, err := llm.NewOpenAIClient(llm.OpenAIConfig{
		APIKey:     "test-key",
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.Chat(context.Background(), llm.ChatRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "bad request") {
		t.Fatalf("error = %v", err)
	}
}

func TestOpenAIClientRetriesTransientFailures(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch attempts.Add(1) {
		case 1:
			http.Error(w, `{"error":{"message":"rate limited"}}`, http.StatusTooManyRequests)
		case 2:
			http.Error(w, `{"error":{"message":"unavailable"}}`, http.StatusServiceUnavailable)
		default:
			_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"action\":\"final\",\"content\":{\"answer\":\"ok\"}}"}}]}`))
		}
	}))
	defer server.Close()

	client, err := llm.NewOpenAIClient(llm.OpenAIConfig{
		APIKey:     "test-key",
		BaseURL:    server.URL,
		MaxRetries: 2,
		RetryWait:  0,
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	response, err := client.Chat(context.Background(), llm.ChatRequest{})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if response.Content == "" {
		t.Fatal("empty content")
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}
