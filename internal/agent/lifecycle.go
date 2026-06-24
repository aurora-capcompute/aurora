package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"capcompute/dispatcher"
	"capcompute/dispatcher/replay/tape/journaled"
)

// Agent lifecycle host calls. The guest fetches its input and reports its answer
// through these calls so both are recorded on the replay journal — making the
// per-run tape the full narrative: agent.input -> capability calls -> agent.finish.
const (
	callAgentInput  = "agent.input"
	callAgentFinish = "agent.finish"
)

// AgentABIVersion is the guest<->host lifecycle protocol version. Bump it when the
// lifecycle calls or their payloads change.
const AgentABIVersion = 1

type finishArgs struct {
	Answer string `json:"answer"`
}

// lifecycleDispatcher intercepts the agent.input/agent.finish lifecycle calls
// below the replay tape (so they are journaled) and forwards everything else to
// the capability dispatcher.
type lifecycleDispatcher struct {
	next         dispatcher.Dispatcher[RunKey]
	message      string
	history      []HistoryMessage
	systemPrompt string
	manifest     Manifest
}

func newLifecycleDispatcher(
	next dispatcher.Dispatcher[RunKey],
	message string,
	history []HistoryMessage,
	manifest Manifest,
) *lifecycleDispatcher {
	return &lifecycleDispatcher{
		next:         next,
		message:      message,
		history:      history,
		systemPrompt: manifest.SystemPrompt,
		manifest:     manifest,
	}
}

func (l *lifecycleDispatcher) Dispatch(ctx context.Context, key RunKey, call dispatcher.Call) (dispatcher.Outcome, error) {
	switch call.Name {
	case callAgentInput:
		payload, err := json.Marshal(agentInput{
			Message:      l.message,
			History:      l.history,
			SystemPrompt: l.systemPrompt,
			Capabilities: visibleCapabilities(dispatcher.Capabilities(l.next), l.manifest),
		})
		if err != nil {
			return dispatcher.Failed(err.Error()), nil
		}
		return dispatcher.Result(payload), nil
	case callAgentFinish:
		// The answer travels in call.Args and is recorded on the journal; the host
		// reads it back from there. Acknowledge so the guest can return.
		return dispatcher.Result(json.RawMessage(`{"ok":true}`)), nil
	default:
		return l.next.Dispatch(ctx, key, call)
	}
}

func (l *lifecycleDispatcher) Capabilities() []dispatcher.Capability {
	return dispatcher.Capabilities(l.next)
}

// answerFromJournal reads a completed run's answer from the final journal record,
// which must be the agent.finish call. The answer is therefore sourced from the
// tape (the single source of truth) rather than the guest's return value.
func (r *Runtime) answerFromJournal(runID string) (string, error) {
	r.mu.Lock()
	run := r.runs[runID]
	var journal journaled.Journal
	if run != nil {
		journal = run.journal
	}
	r.mu.Unlock()
	if journal == nil {
		return "", errors.New("agent run journal is unavailable")
	}
	length := journal.Length()
	if length == 0 {
		return "", errors.New("agent produced no journal records")
	}
	record, err := journal.Load(length - 1)
	if err != nil {
		return "", err
	}
	if record.Call.Name != callAgentFinish {
		return "", fmt.Errorf("agent did not finish (last journal call was %q)", record.Call.Name)
	}
	var args finishArgs
	if err := json.Unmarshal(record.Call.Args, &args); err != nil {
		return "", fmt.Errorf("decode finish answer: %w", err)
	}
	if strings.TrimSpace(args.Answer) == "" {
		return "", errors.New("agent finish call is missing an answer")
	}
	return args.Answer, nil
}
