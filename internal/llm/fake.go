package llm

import (
	"context"
	"encoding/json"
	"strings"
)

const defaultFakeReadURL = "https://example.com"

type actionEnvelope[T any] struct {
	Action  string `json:"action"`
	Content T      `json:"content"`
}

type fakeReadAction struct {
	Method string `json:"method"`
	URL    string `json:"url"`
	Reason string `json:"reason"`
}

type fakeFinalAction struct {
	Answer string `json:"answer"`
	Reason string `json:"reason"`
}

// FakeClient returns deterministic actions for tests and local demos.
type FakeClient struct {
	ReadURL string
}

func NewFakeClient(readURL string) *FakeClient {
	if strings.TrimSpace(readURL) == "" {
		readURL = defaultFakeReadURL
	}
	return &FakeClient{ReadURL: readURL}
}

func (c *FakeClient) Chat(_ context.Context, request ChatRequest) (ChatResponse, error) {
	return c.ChatWithMessages(request.Messages)
}

func (c *FakeClient) ChatWithMessages(messages []Message) (ChatResponse, error) {
	var observation string
	for _, message := range messages {
		if message.Role == "tool" {
			observation = message.Content
		}
	}
	if observation == "" {
		return marshalActions([]actionEnvelope[fakeReadAction]{{
			Action: "internet.read",
			Content: fakeReadAction{
				Method: "GET",
				URL:    c.readURL(),
				Reason: "fake client reads the configured URL",
			},
		}})
	}

	return marshalActions([]actionEnvelope[fakeFinalAction]{{
		Action: "final",
		Content: fakeFinalAction{
			Answer: "Read result: " + compactObservation(observation),
			Reason: "fake client observed a read result",
		},
	}})
}

func (c *FakeClient) readURL() string {
	if c == nil || strings.TrimSpace(c.ReadURL) == "" {
		return defaultFakeReadURL
	}
	return c.ReadURL
}

func marshalActions(v any) (ChatResponse, error) {
	data, err := json.Marshal(struct {
		Actions any `json:"actions"`
	}{Actions: v})
	if err != nil {
		return ChatResponse{}, err
	}
	return ChatResponse{Content: string(data)}, nil
}

func compactObservation(observation string) string {
	compact := strings.Join(strings.Fields(observation), " ")
	if len(compact) > 500 {
		return compact[:500]
	}
	return compact
}
