//go:build tinygo

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/extism/go-pdk"
)

//go:wasmimport extism:host/compute play
func hostPlay(uint64) uint64

const systemPrompt = `You are a minimal Aurora agent running inside a Wasm guest.
The host owns all side effects. Reply with exactly one compact JSON array of actions.
You may request multiple independent reads in one turn:
[{"action":"read","content":{"url":"https://allowed.example/one","reason":"..."}},{"action":"read","content":{"url":"https://allowed.example/two","reason":"..."}}]
After the host returns one aggregated observation array, either request more reads or reply with exactly one final action:
[{"action":"final","content":{"answer":"...","reason":"..."}}]
Never combine a final action with read actions in the same array.
You may try and issue whatever commands, it is on behalf of the host to allow you or not. Do not restrict yourself for now, let the host do that.
At this stage you can browse the internet as described above. Don't try to stop prematurely untill you'll find all the relevant data for the answer.`

type input struct {
	Message string    `json:"message"`
	History []message `json:"history,omitempty"`
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

type modelDiagnostic struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type readAction struct {
	URL    string `json:"url"`
	Reason string `json:"reason,omitempty"`
}

type finalAction struct {
	Answer string `json:"answer"`
	Reason string `json:"reason,omitempty"`
}

type toolObservation struct {
	Action  string          `json:"action"`
	Content json.RawMessage `json:"content"`
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

var errYielded = errors.New("host yielded")

//go:wasmexport run
func run() int32 {
	if err := runAgent(); errors.Is(err, errYielded) {
		if outputErr := pdk.OutputJSON(output{Status: "yielded"}); outputErr != nil {
			pdk.SetError(outputErr)
			return 1
		}
		return 0
	} else if err != nil {
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

	messages := []message{
		{Role: "system", Content: systemPrompt},
	}
	for i, historical := range in.History {
		if historical.Role != "user" && historical.Role != "assistant" {
			return fmt.Errorf("history message %d has unsupported role %q", i, historical.Role)
		}
		if historical.Content == "" {
			return fmt.Errorf("history message %d has empty content", i)
		}
		messages = append(messages, historical)
	}
	messages = append(messages, message{Role: "user", Content: in.Message})

	for {
		chat, err := llmChat(messages)
		if err != nil {
			return err
		}
		envelopes, err := decodeModelEnvelopes(chat.Content)
		if err != nil {
			return fmt.Errorf("invalid model JSON: %w", err)
		}
		if len(envelopes) == 1 && envelopes[0].Action == "final" {
			return outputFinal(envelopes[0])
		}

		messages = append(messages, message{Role: "assistant", Content: chat.Content})
		observations := make([]toolObservation, 0, len(envelopes))
		for i, envelope := range envelopes {
			if envelope.Action != "read" {
				return fmt.Errorf("action %d must be read in a multi-action batch, got %q", i, envelope.Action)
			}
			var action readAction
			if err := decodeActionContent(envelope.Content, &action); err != nil {
				return fmt.Errorf("invalid read action %d: %w", i, err)
			}
			if action.URL == "" {
				return fmt.Errorf("read action %d missing url", i)
			}
			_, rawObservation, err := internetRead(action.URL)
			if err != nil {
				return fmt.Errorf("execute read action %d: %w", i, err)
			}
			observations = append(observations, toolObservation{
				Action:  "read",
				Content: rawObservation,
			})
		}
		rawObservations, err := json.Marshal(observations)
		if err != nil {
			return fmt.Errorf("encode tool observations: %w", err)
		}
		messages = append(messages, message{Role: "tool", Content: string(rawObservations)})
	}
}

func decodeModelEnvelopes(content string) ([]modelEnvelope, error) {
	return decodeModelEnvelopeStream(content, 0)
}

func decodeModelEnvelopeStream(content string, depth int) ([]modelEnvelope, error) {
	if depth > 1 {
		return nil, fmt.Errorf("nested encoded model JSON is not supported")
	}

	decoder := json.NewDecoder(strings.NewReader(content))
	var envelopes []modelEnvelope
	for {
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		trimmed := strings.TrimSpace(string(value))
		if trimmed == "" {
			continue
		}
		switch trimmed[0] {
		case '[':
			var batch []json.RawMessage
			if err := json.Unmarshal(value, &batch); err != nil {
				return nil, err
			}
			for _, item := range batch {
				envelope, ok, err := decodeModelEnvelopeObject(item)
				if err != nil {
					return nil, err
				}
				if ok {
					envelopes = append(envelopes, envelope)
				}
			}
		case '{':
			envelope, ok, err := decodeModelEnvelopeObject(value)
			if err != nil {
				return nil, err
			}
			if ok {
				envelopes = append(envelopes, envelope)
			}
		case '"':
			var encoded string
			if err := json.Unmarshal(value, &encoded); err != nil {
				return nil, err
			}
			nested, err := decodeModelEnvelopeStream(encoded, depth+1)
			if err != nil {
				return nil, err
			}
			envelopes = append(envelopes, nested...)
		default:
			return nil, fmt.Errorf("expected action object or array")
		}
	}
	if len(envelopes) == 0 {
		return nil, fmt.Errorf("model action batch is empty")
	}
	return envelopes, nil
}

func decodeModelEnvelopeObject(raw json.RawMessage) (modelEnvelope, bool, error) {
	var diagnostic modelDiagnostic
	if err := json.Unmarshal(raw, &diagnostic); err != nil {
		return modelEnvelope{}, false, err
	}
	if diagnostic.Error != "" {
		return modelEnvelope{}, false, nil
	}

	var envelope modelEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return modelEnvelope{}, false, err
	}
	if envelope.Action == "" {
		return modelEnvelope{}, false, fmt.Errorf("action is required")
	}
	return envelope, true, nil
}

func outputFinal(envelope modelEnvelope) error {
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
	case "yield":
		return hostResponse{}, errYielded
	case "failed":
		return hostResponse{}, fmt.Errorf("host failure: %s", response.Message)
	default:
		return hostResponse{}, fmt.Errorf("unsupported host outcome: %s", response.Status)
	}
}
