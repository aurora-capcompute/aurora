# aurora-capcompute

Implementation-neutral Aurora orchestration library built on `capcompute`.

The module owns thread and run lifecycle, the replay journal, durable approval
tasks, retries, event subscriptions, and execution of caller-supplied Wasm
brains. All durable state is a fold of a single append-only event log — the
runtime keeps no mutable row store. It does not provide:

- LLM, internet, Kubernetes, Helm, MCP, or other capabilities.
- Dispatcher registries or capability-specific manifest settings.
- Memory, SQLite, or other persistence implementations.
- Filesystem or remote brain loaders.
- A CLI, HTTP server, or environment-based application wiring.

Applications compose those implementations explicitly.

## Required dependencies

`aurora.NewRuntime` requires all implementation dependencies:

```go
runtime, err := aurora.NewRuntime(ctx, aurora.Config{
    Brains:       brainProvider,
    Dispatchers:  dispatcherProvider,
    Log:          eventLog,
    Leases:       leases,
    SessionStore: sessionStore,
    TaskSecret:   taskSecret,
})
```

Construction fails when any required dependency is missing. The runtime owns
and closes compiled Wasm instances and active sessions. Callers retain ownership
of injected stores and providers.

## Brain provider

A brain provider supplies immutable Wasm bytes:

```go
type BrainProvider interface {
    DefaultID() string
    List(context.Context) ([]aurora.BrainSource, error)
}
```

The runtime copies the bytes, computes SHA-256 digests, and pins each run to its
brain digest. Filesystem, object-store, embedded, and remote loaders belong in
application or adapter modules.

## Dispatcher provider

A dispatcher provider owns capability-specific settings and implementation:

```go
type DispatcherProvider interface {
    Normalize(name string, settings json.RawMessage) (json.RawMessage, error)
    NewDispatcher(
        context.Context,
        aurora.RunContext,
        aurora.Manifest,
    ) (dispatcher.Dispatcher[aurora.RunContext], error)
}
```

The core validates and normalizes thread manifests and run overrides through
this provider. For each run, it wraps the returned dispatcher with durable task
approval and replay middleware.

## Storage contracts

State is event-sourced. The application supplies one append-only log; the
runtime appends domain events and folds them back into projections on restore.

```go
type EventLog interface {
    Append(ctx context.Context, scope LogScope, events ...LogEvent) (head uint64, err error)
    Read(ctx context.Context, scope LogScope, after uint64) ([]LogEvent, error)
    Streams(ctx context.Context, tenantID string) ([]LogScope, error)
}
```

One stream per thread carries every thread, run, task, and capability-journal
event. There is no mutable row store: threads, runs, tasks, and the replay
journal are all projections of the log. Cross-instance coordination uses a
separate `Leases` interface — an ephemeral fencing token with a TTL, kept out of
a thread's immutable history. Physical Wasm sessions use the
`capcompute.SessionStore[string, aurora.RunContext]` contract.

Concrete in-memory and SQLite event logs (and leases) live in the separate
`aurora-stores` repository.

## Manifest helpers

Manifest operations require the same dispatcher provider used by the runtime:

```go
validated, err := aurora.ValidateManifest(manifest, dispatcherProvider)
effective, err := aurora.EffectiveManifest(
    validated,
    capabilityOverrides,
    dispatcherProvider,
)
```

Aurora defines only the generic capability envelope:

```json
{
  "name": "provider.operation",
  "settings": {}
}
```

The provider decides which names and settings are valid.

## Verification

```sh
GOCACHE=/tmp/aurora-capcompute-go-build go test -race ./...
GOCACHE=/tmp/aurora-capcompute-go-build go vet ./...
```
