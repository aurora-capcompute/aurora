//go:build tinygo

package main

import (
	"encoding/json"
	"fmt"

	"github.com/extism/go-pdk"
)

//go:wasmimport extism:host/compute play
func hostPlay(uint64) uint64

const systemPrompt = `You are a minimal Aurora agent running inside a Wasm guest.
The host owns all side effects. Reply with exactly one compact JSON action:
{"action":"read","content":{"url":"https://allowed.example/path","reason":"..."}} or {"action":"final","content":{"answer":"...","reason":"..."}}.
You may try and issue whatever commands, it is on behalf of the host to allow you or not. Do not restrict yourself for now, let the host do that.
At this stage you can browse the internet as described above.
`

type input struct {
	Message  string `json:"message"`
	MaxSteps int    `json:"max_steps"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type llmRequest struct {
	Messages []message `json:"messages"`
	JSON     bool      `json:"json"`
}

type llmResponse struct {
	Content string `json:"content"`
}

type modelEnvelope struct {
	Action  string          `json:"action"`
	Content json.RawMessage `json:"content"`
}

type readAction struct {
	URL    string `json:"url"`
	Reason string `json:"reason,omitempty"`
}

type finalAction struct {
	Answer string `json:"answer"`
	Reason string `json:"reason,omitempty"`
}

type internetReadRequest struct {
	Method string `json:"method"`
	URL    string `json:"url"`
}

type internetReadResponse struct {
	URL         string `json:"url"`
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	Body        string `json:"body"`
}

type output struct {
	Status string `json:"status"`
	Answer string `json:"answer"`
}

type call struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args,omitempty"`
}

type hostResponse struct {
	Status  string          `json:"status"`
	Result  json.RawMessage `json:"result,omitempty"`
	Message string          `json:"message,omitempty"`
}

//go:wasmexport run
func run() int32 {
	if err := runAgent(); err != nil {
		pdk.SetError(err)
		return 1
	}
	return 0
}

func runAgent() error {
	var in input
	if err := pdk.InputJSON(&in); err != nil {
		return fmt.Errorf("decode input: %w", err)
	}
	if in.Message == "" {
		return fmt.Errorf("message is required")
	}
	if in.MaxSteps <= 0 {
		in.MaxSteps = 4
	}

	messages := []message{
		{Role: "system", Content: systemPrompt},
		{Role: "user", Content: in.Message},
	}

	for i := 0; i < in.MaxSteps; i++ {
		chat, err := llmChat(messages)
		if err != nil {
			return err
		}
		var envelope modelEnvelope
		if err := json.Unmarshal([]byte(chat.Content), &envelope); err != nil {
			return fmt.Errorf("invalid model JSON: %w", err)
		}

		switch envelope.Action {
		case "read":
			var action readAction
			if err := decodeActionContent(envelope.Content, &action); err != nil {
				return fmt.Errorf("invalid read action: %w", err)
			}
			if action.URL == "" {
				return fmt.Errorf("read action missing url")
			}
			messages = append(messages, message{Role: "assistant", Content: chat.Content})
			observation, rawObservation, err := internetRead(action.URL)
			if err != nil {
				return err
			}
			_ = observation
			messages = append(messages, message{Role: "tool", Content: string(rawObservation)})

		case "final":
			var action finalAction
			if err := decodeActionContent(envelope.Content, &action); err != nil {
				return fmt.Errorf("invalid final action: %w", err)
			}
			if action.Answer == "" {
				return fmt.Errorf("final action missing answer")
			}
			return pdk.OutputJSON(output{
				Status: "completed",
				Answer: action.Answer,
			})

		default:
			return fmt.Errorf("unsupported action: %s", envelope.Action)
		}
	}
	return fmt.Errorf("max steps exceeded")
}

func decodeActionContent(content json.RawMessage, target any) error {
	if len(content) == 0 {
		return fmt.Errorf("content is required")
	}
	if err := json.Unmarshal(content, target); err != nil {
		return err
	}
	return nil
}

func llmChat(messages []message) (llmResponse, error) {
	args, err := json.Marshal(llmRequest{Messages: messages, JSON: true})
	if err != nil {
		return llmResponse{}, fmt.Errorf("encode llm request: %w", err)
	}
	response, err := dispatch(call{Name: "llm.chat", Args: args})
	if err != nil {
		return llmResponse{}, err
	}
	var chat llmResponse
	if err := json.Unmarshal(response.Result, &chat); err != nil {
		return llmResponse{}, fmt.Errorf("decode llm response: %w", err)
	}
	return chat, nil
}

func internetRead(target string) (internetReadResponse, json.RawMessage, error) {
	args, err := json.Marshal(internetReadRequest{Method: "GET", URL: target})
	if err != nil {
		return internetReadResponse{}, nil, fmt.Errorf("encode internet request: %w", err)
	}
	response, err := dispatch(call{Name: "internet.read", Args: args})
	if err != nil {
		return internetReadResponse{}, nil, err
	}
	var read internetReadResponse
	if err := json.Unmarshal(response.Result, &read); err != nil {
		return internetReadResponse{}, nil, fmt.Errorf("decode internet response: %w", err)
	}
	return read, response.Result, nil
}

func dispatch(c call) (hostResponse, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return hostResponse{}, fmt.Errorf("encode call: %w", err)
	}

	request := pdk.AllocateBytes(data)
	defer request.Free()

	responseOffset := hostPlay(request.Offset())
	var response hostResponse
	if err := pdk.JSONFrom(responseOffset, &response); err != nil {
		return hostResponse{}, fmt.Errorf("decode host response: %w", err)
	}
	switch response.Status {
	case "result":
		return response, nil
	case "failed":
		return hostResponse{}, fmt.Errorf("host failure: %s", response.Message)
	default:
		return hostResponse{}, fmt.Errorf("unsupported host outcome: %s", response.Status)
	}
}
