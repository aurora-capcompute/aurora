package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"capcompute/dispatcher"
)

const (
	LegacyManifestVersion = 1
	ManifestVersion       = 2
)

type Manifest struct {
	Version      int                `json:"version"`
	Brain        string             `json:"brain,omitempty"`
	SystemPrompt string             `json:"system_prompt,omitempty"`
	Capabilities []CapabilityConfig `json:"capabilities"`
	Children     []ChildManifest    `json:"children,omitempty"`
}

type ChildManifest struct {
	Name         string             `json:"name"`
	Brain        string             `json:"brain"`
	SystemPrompt string             `json:"system_prompt,omitempty"`
	Capabilities []CapabilityConfig `json:"capabilities"`
	Children     []ChildManifest    `json:"children,omitempty"`
	// OnFailure selects how a failure of this delegated child is handled:
	// OnFailureReport (default) surfaces it to the parent brain as a recoverable
	// failed observation; OnFailurePropagate fails the parent run outright.
	OnFailure string `json:"on_failure,omitempty"`
}

// Child failure-handling modes for ChildManifest.OnFailure.
const (
	OnFailureReport    = "report"
	OnFailurePropagate = "propagate"
)

type CapabilityConfig struct {
	Name     string          `json:"name"`
	Settings json.RawMessage `json:"settings,omitempty"`
	Hidden   bool            `json:"hidden,omitempty"`
}

type DispatcherProvider interface {
	Normalize(name string, settings json.RawMessage) (json.RawMessage, error)
	NewDispatcher(context.Context, RunContext, Manifest) (dispatcher.Dispatcher[RunContext], error)
	IsSubset(name string, parent, child json.RawMessage) error
}

func ValidateManifest(manifest Manifest, provider DispatcherProvider) (Manifest, error) {
	if provider == nil {
		return Manifest{}, fmt.Errorf("%w: dispatcher provider is required", ErrInvalid)
	}
	if manifest.Version == LegacyManifestVersion {
		manifest.Version = ManifestVersion
	}
	if manifest.Version != ManifestVersion {
		return Manifest{}, fmt.Errorf("%w: manifest version must be %d", ErrInvalid, ManifestVersion)
	}
	manifest.SystemPrompt = strings.TrimSpace(manifest.SystemPrompt)
	manifest.Brain = strings.TrimSpace(manifest.Brain)
	seen := make(map[string]struct{}, len(manifest.Capabilities))
	for i := range manifest.Capabilities {
		capability := &manifest.Capabilities[i]
		capability.Name = strings.TrimSpace(capability.Name)
		if capability.Name == "" {
			return Manifest{}, fmt.Errorf("%w: capability %d name is required", ErrInvalid, i)
		}
		if _, exists := seen[capability.Name]; exists {
			return Manifest{}, fmt.Errorf("%w: duplicate capability %q", ErrInvalid, capability.Name)
		}
		seen[capability.Name] = struct{}{}
		normalized, err := provider.Normalize(capability.Name, capability.Settings)
		if err != nil {
			return Manifest{}, fmt.Errorf("%w: %s settings: %v", ErrInvalid, capability.Name, err)
		}
		capability.Settings = append(json.RawMessage(nil), normalized...)
	}
	if err := validateChildren(manifest.Children, provider); err != nil {
		return Manifest{}, err
	}
	return cloneManifest(manifest), nil
}

func EffectiveManifest(base Manifest, overrides []CapabilityConfig, provider DispatcherProvider) (Manifest, error) {
	effective := cloneManifest(base)
	index := make(map[string]int, len(effective.Capabilities))
	for i, capability := range effective.Capabilities {
		index[capability.Name] = i
	}
	for _, override := range overrides {
		overrideManifest, err := ValidateManifest(Manifest{
			Version:      ManifestVersion,
			SystemPrompt: effective.SystemPrompt,
			Brain:        effective.Brain,
			Capabilities: []CapabilityConfig{override},
		}, provider)
		if err != nil {
			return Manifest{}, err
		}
		validated := overrideManifest.Capabilities[0]
		if i, exists := index[validated.Name]; exists {
			effective.Capabilities[i] = validated
		} else {
			index[validated.Name] = len(effective.Capabilities)
			effective.Capabilities = append(effective.Capabilities, validated)
		}
	}
	return effective, nil
}

func validateChildren(children []ChildManifest, provider DispatcherProvider) error {
	seen := make(map[string]struct{}, len(children))
	for i := range children {
		child := &children[i]
		child.Brain = strings.TrimSpace(child.Brain)
		if child.Brain == "" {
			return fmt.Errorf("%w: child %d brain is required", ErrInvalid, i)
		}
		child.Name = strings.TrimSpace(child.Name)
		if child.Name == "" {
			child.Name = child.Brain
		}
		if _, exists := seen[child.Name]; exists {
			return fmt.Errorf("%w: duplicate child name %q", ErrInvalid, child.Name)
		}
		seen[child.Name] = struct{}{}
		switch child.OnFailure {
		case "", OnFailureReport, OnFailurePropagate:
		default:
			return fmt.Errorf("%w: child %q on_failure must be %q or %q", ErrInvalid, child.Name, OnFailureReport, OnFailurePropagate)
		}
		for j, cap := range child.Capabilities {
			cap.Name = strings.TrimSpace(cap.Name)
			if cap.Name == "" {
				return fmt.Errorf("%w: child %q capability %d name is required", ErrInvalid, child.Brain, j)
			}
			normalized, err := provider.Normalize(cap.Name, cap.Settings)
			if err != nil {
				return fmt.Errorf("%w: child %q %s settings: %v", ErrInvalid, child.Brain, cap.Name, err)
			}
			if settingsRequireApproval(normalized) {
				return fmt.Errorf("%w: child %q capability %q requires approval; child capabilities cannot require approval — set require_approval: false explicitly", ErrInvalid, child.Name, cap.Name)
			}
			child.Capabilities[j].Settings = append(json.RawMessage(nil), normalized...)
		}
		if err := validateChildren(child.Children, provider); err != nil {
			return fmt.Errorf("child %q: %w", child.Brain, err)
		}
	}
	return nil
}

func cloneManifest(manifest Manifest) Manifest {
	out := manifest
	out.Capabilities = make([]CapabilityConfig, len(manifest.Capabilities))
	for i, capability := range manifest.Capabilities {
		out.Capabilities[i] = capability
		out.Capabilities[i].Settings = append(json.RawMessage(nil), capability.Settings...)
	}
	out.Children = cloneChildren(manifest.Children)
	return out
}

func cloneChildren(children []ChildManifest) []ChildManifest {
	if len(children) == 0 {
		return nil
	}
	out := make([]ChildManifest, len(children))
	for i, child := range children {
		out[i] = child
		out[i].Capabilities = make([]CapabilityConfig, len(child.Capabilities))
		for j, cap := range child.Capabilities {
			out[i].Capabilities[j] = cap
			out[i].Capabilities[j].Settings = append(json.RawMessage(nil), cap.Settings...)
		}
		out[i].Children = cloneChildren(child.Children)
	}
	return out
}
