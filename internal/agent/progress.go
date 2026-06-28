package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aurora-capcompute/capcompute/dispatcher"
)

type progressDispatcher struct {
	next     dispatcher.Dispatcher[RunContext]
	publish  func(threadID string, event Event)
	threadID string
	runID    string
}

type progressArgs struct {
	Message string `json:"message"`
}

func newProgressDispatcher(next dispatcher.Dispatcher[RunContext], publish func(string, Event), threadID, runID string) *progressDispatcher {
	return &progressDispatcher{next: next, publish: publish, threadID: threadID, runID: runID}
}

func (d *progressDispatcher) Dispatch(ctx context.Context, key RunContext, call dispatcher.Call, auth dispatcher.Authorization) (dispatcher.Outcome, error) {
	if call.Name == "aurora.log" {
		var args progressArgs
		if err := json.Unmarshal(call.Args, &args); err != nil {
			return dispatcher.Fail(fmt.Sprintf("decode aurora.log: %v", err)), nil
		}
		d.publish(d.threadID, Event{
			Type: "progress",
			Data: ProgressEvent{RunID: d.runID, Message: args.Message},
		})
		return dispatcher.Result(json.RawMessage(`{}`)), nil
	}
	return d.next.Dispatch(ctx, key, call, auth)
}

func (d *progressDispatcher) Capabilities() []dispatcher.Capability {
	return d.next.Capabilities()
}

type ProgressEvent struct {
	RunID   string `json:"run_id"`
	Message string `json:"message"`
}
