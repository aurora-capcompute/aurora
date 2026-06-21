package main

import (
	"aurora-stores/memory"
	"bytes"
	"capcompute"
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay"
	"capcompute/dispatcher/replay/tape/journaled"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"aurora-capcompute/internal/agent"
	internalhost "aurora-capcompute/internal/host"
	"aurora-dispatchers/llm"

	extism "github.com/extism/go-sdk"
)

type Run struct {
	ID string
}

func (r Run) SessionKey() string {
	return r.ID
}

type agentInput struct {
	Message      string                  `json:"message"`
	SystemPrompt string                  `json:"system_prompt,omitempty"`
	Capabilities []dispatcher.Capability `json:"capabilities,omitempty"`
}

type executeResult struct {
	Result  json.RawMessage `json:"result"`
	Journal []journalEntry  `json:"journal"`
}

type journalEntry struct {
	Call    journalCall    `json:"call"`
	Outcome journalOutcome `json:"outcome"`
}

type journalCall struct {
	Name string `json:"name"`
	Args any    `json:"args,omitempty"`
}

type journalOutcome struct {
	Status  dispatcher.OutcomeKind `json:"status"`
	Result  any                    `json:"result,omitempty"`
	Message string                 `json:"message,omitempty"`
}

const journalStringLimit = 500

func main() {
	result, err := execute(context.Background(), os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := writeJSON(os.Stdout, result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string) (executeResult, error) {
	llmClient, err := llmFromEnv()
	if err != nil {
		return executeResult{}, err
	}
	manifest, err := agent.DefaultManifest(os.Getenv("AURORA_HTTP_ALLOW"))
	if err != nil {
		return executeResult{}, fmt.Errorf("build manifest: %w", err)
	}
	hostConfig, err := agent.DispatcherConfig(manifest, llmClient)
	if err != nil {
		return executeResult{}, fmt.Errorf("build dispatcher: %w", err)
	}

	wasmPath := envDefault("AURORA_GUEST_WASM", "../aurora-brains/agent/agent.wasm")
	journal := memory.NewJournal()
	store := memory.NewSessionStore[string, Run]()
	dispatcherFactory := internalhost.Factory[Run]{
		LLM:                     hostConfig.LLM,
		Internet:                hostConfig.Internet,
		InternetRequireApproval: hostConfig.InternetRequireApproval,
		Capabilities:            hostConfig.Capabilities,
		NewTape: func(context.Context, Run) (replay.Tape, error) {
			return journaled.NewTape(journal), nil
		},
	}
	compute, err := capcompute.NewComputeCompiledPlugin[string, Run](ctx, capcompute.Config[string, Run]{
		Manifest: extism.Manifest{
			Wasm: []extism.Wasm{extism.WasmFile{Path: wasmPath}},
		},
		PluginConfig: extism.PluginConfig{
			EnableWasi: true,
		},
		SessionStore: store,
	})
	if err != nil {
		return executeResult{}, fmt.Errorf("compile guest %q: %w", wasmPath, err)
	}
	defer compute.CloseCompiled(context.Background())

	run := Run{ID: envDefault("AURORA_RUN_ID", fmt.Sprintf("run-%d", time.Now().UnixNano()))}
	sessionDispatcher, err := dispatcherFactory.NewDispatcher(ctx, run)
	if err != nil {
		return executeResult{}, fmt.Errorf("create dispatcher: %w", err)
	}
	session, err := compute.CreateSession(ctx, capcompute.PlayRequest[string, Run]{
		Entrypoint: "run",
		UserData:   run,
		Dispatcher: sessionDispatcher,
	})
	if err != nil {
		return executeResult{}, fmt.Errorf("create session: %w", err)
	}
	input, err := json.Marshal(agentInput{
		Message:      messageFromArgs(args),
		SystemPrompt: manifest.SystemPrompt,
		Capabilities: session.Capabilities(),
	})
	if err != nil {
		_ = session.Close(ctx)
		return executeResult{}, fmt.Errorf("encode input: %w", err)
	}
	session.Input = input
	defer session.Close(context.Background())

	if err := store.SaveSession(ctx, run.SessionKey(), session); err != nil {
		return executeResult{}, fmt.Errorf("save session: %w", err)
	}

	handle, err := compute.Play(ctx, session)
	if err != nil {
		return executeResult{}, fmt.Errorf("play: %w", err)
	}
	result := <-handle.Results()
	if result.Status != capcompute.PlayCompleted {
		if result.Err != nil {
			return executeResult{}, fmt.Errorf("play %s: %w", result.Status, result.Err)
		}
		return executeResult{}, fmt.Errorf("play %s: exit %d output %s", result.Status, result.Exit, result.Output)
	}

	entries, err := collectJournalEntries(journal)
	if err != nil {
		return executeResult{}, err
	}
	return executeResult{
		Result:  result.Output,
		Journal: entries,
	}, nil
}

func collectJournalEntries(journal *memory.Journal) ([]journalEntry, error) {
	entries := make([]journalEntry, 0, journal.Length())
	for i := 0; i < journal.Length(); i++ {
		record, err := journal.Load(i)
		if err != nil {
			return nil, fmt.Errorf("load journal record %d: %w", i, err)
		}
		args, err := journalJSON(record.Call.Args)
		if err != nil {
			return nil, fmt.Errorf("format journal record %d args: %w", i, err)
		}
		result, err := journalResult(record.Outcome.Result())
		if err != nil {
			return nil, fmt.Errorf("format journal record %d result: %w", i, err)
		}
		entries = append(entries, journalEntry{
			Call: journalCall{
				Name: record.Call.Name,
				Args: args,
			},
			Outcome: journalOutcome{
				Status:  record.Outcome.Kind(),
				Result:  result,
				Message: record.Outcome.Message(),
			},
		})
	}
	return entries, nil
}

func journalResult(raw json.RawMessage) (any, error) {
	return journalJSON(raw)
}

func journalJSON(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return truncateJournalStrings(value), nil
}

func truncateJournalStrings(value any) any {
	switch value := value.(type) {
	case string:
		runes := []rune(value)
		if len(runes) <= journalStringLimit {
			return value
		}
		return string(runes[:journalStringLimit]) + "[...]"
	case []any:
		for i := range value {
			value[i] = truncateJournalStrings(value[i])
		}
		return value
	case map[string]any:
		for key, item := range value {
			value[key] = truncateJournalStrings(item)
		}
		return value
	default:
		return value
	}
}

func llmFromEnv() (llm.Client, error) {
	switch strings.ToLower(envDefault("AURORA_LLM", "fake")) {
	case "fake":
		return llm.NewFakeClient(os.Getenv("AURORA_FAKE_READ_URL")), nil
	case "openai":
		return llm.NewOpenAIClient(llm.OpenAIConfigFromEnv())
	default:
		return nil, fmt.Errorf("unsupported AURORA_LLM: %s", os.Getenv("AURORA_LLM"))
	}
}

func messageFromArgs(args []string) string {
	if len(args) > 0 {
		return strings.Join(args, " ")
	}
	return envDefault("AURORA_MESSAGE", "Read https://example.com and summarize it.")
}

func writeJSON(file *os.File, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		_, err = file.Write(append(raw, '\n'))
		return err
	}
	pretty.WriteByte('\n')
	_, err = file.Write(pretty.Bytes())
	return err
}

func envDefault(name string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
