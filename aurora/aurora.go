// Package aurora is the module's public surface: an implementation-neutral
// orchestration library built on capcompute. It owns thread and run lifecycle,
// the replay journal, durable approval tasks, retries, event subscriptions, and
// execution of caller-supplied Wasm brains — all folded from a single
// append-only event log, with no mutable row store.
//
// It owns no capabilities, dispatchers, persistence, or application wiring:
// brains, dispatchers, the event log, leases, and the session store are injected
// through Config, and applications compose those implementations explicitly.
// The types here are thin re-exports of the internal/agent runtime.
package aurora

import (
	"context"

	"aurora-capcompute/internal/agent"
)

func NewRuntime(ctx context.Context, config Config) (Runtime, error) {
	return agent.NewRuntime(ctx, config)
}

func ValidateManifest(m Manifest, provider DispatcherProvider) (Manifest, error) {
	return agent.ValidateManifest(m, provider)
}

func EffectiveManifest(base Manifest, overrides []CapabilityConfig, provider DispatcherProvider) (Manifest, error) {
	return agent.EffectiveManifest(base, overrides, provider)
}
