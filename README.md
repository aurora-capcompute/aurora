# aurora-capcompute

Minimal Aurora-on-`capcompute` prototype.

All rights reserved.

The TinyGo guest owns the agent loop and calls only `extism:host/compute/play`.
The Go host owns all side effects: `llm.chat` and `internet.read`. The guest does
not use Extism built-in HTTP or Extism network policy.

Each model turn returns a JSON action array. The guest executes batched
`internet.read` actions sequentially and sends their results back as one
aggregated observation array before asking the model for its next action batch.
For compatibility with imperfect model output, the guest also accepts
whitespace-delimited action objects and a JSON string containing either form.

## Layout

- `cmd/aurora-agent`: runnable host.
- `cmd/aurora-server`: long-lived REST/SSE agent server.
- `guest/agent.go`: TinyGo/Wasm guest entrypoint `run`.
- `internal/agent`: process-lifetime thread, run, session, and journal ownership.
- `internal/server`: HTTP and SSE transport.
- `internal/host`: `dispatcher.Call` handlers.
- `internal/llm`: fake and OpenAI-compatible LLM clients.
- `internal/internet`: allowlisted HTTP reads.

## Build And Test

```sh
GOCACHE=/tmp/aurora-capcompute-go-build go test ./...
sh guest/build.sh
AURORA_LLM=fake AURORA_HTTP_ALLOW=GET:https://example.com go run ./cmd/aurora-agent
```

## Agent Server

The server compiles the guest once and keeps threads, physical Wasm sessions,
active play handles, and replay journals in memory for the lifetime of the
process:

```sh
sh guest/build.sh

AURORA_LLM=openai \
AURORA_HTTP_ALLOW=GET:https://go.dev \
go run ./cmd/aurora-server
```

The default address is `127.0.0.1:8080`; override it with
`AURORA_SERVER_ADDR`.

Create a thread and send its first message:

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/threads

curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{"content":"Research the Go 1.26 release."}' \
  http://127.0.0.1:8080/v1/threads/THREAD_ID/messages
```

Inspect a run or its complete journal:

```sh
curl -sS http://127.0.0.1:8080/v1/runs/RUN_ID
curl -sS http://127.0.0.1:8080/v1/runs/RUN_ID/journal
```

Receive thread events:

```sh
curl -N http://127.0.0.1:8080/v1/threads/THREAD_ID/events
```

Stop or retry a run:

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/runs/RUN_ID/stop

curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{"mode":"resume"}' \
  http://127.0.0.1:8080/v1/runs/RUN_ID/retry
```

`resume` preserves the journal and replays committed calls. `restart` replaces
the journal and reruns the turn from scratch. Only the latest run in a thread
can be retried.

Each user message creates a fresh Wasm session. The new session receives only
completed user/assistant message pairs from the thread, not previous tool calls
or downloaded pages. A yielded run remains the active thread run until retried
or stopped.

The server has no authentication or CORS policy. It stores everything in memory,
so all state disappears when the process exits.

## Runtime Configuration

- `AURORA_LLM=fake|openai`, default `fake`.
- `AURORA_FAKE_READ_URL`, default `https://example.com`.
- `AURORA_HTTP_ALLOW`, for example `GET:https://example.com,GET:https://docs.example.org`.
- `AURORA_GUEST_WASM`, default `guest/agent.wasm`.
- `AURORA_SERVER_ADDR`, default `127.0.0.1:8080`.
- `AURORA_MESSAGE`, or pass the message as CLI arguments.

The guest has no step limit. The host controls execution through the
`capcompute.PlayHandle`: `Stop` forcefully terminates the current Wasm instance,
while a host `yield` outcome leaves the session available for replay.

OpenAI-compatible mode uses Chat Completions:

- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`, default `https://api.openai.com/v1`
- `OPENAI_MODEL`, default `gpt-5.4-mini`
- `OPENAI_TIMEOUT`
- `OPENAI_MAX_RETRIES`
- `OPENAI_MAX_TOKENS`
- `OPENAI_TEMPERATURE`

Internet reads are v0 GET-only. Allowlist entries are exact origin matches with
lowercased host comparison and ports respected. Redirect targets must also match
the allowlist. Response bodies are bounded and binary-looking content types are
rejected.
