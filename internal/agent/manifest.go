package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aurora-capcompute/capcompute/dispatcher"
)

const ManifestVersion = 2

// AgentToolType is the tool `type` for a sub-agent. A tool of this type is not a
// leaf I/O dispatcher; it is run by the runtime as a child agent (see agent.go).
const AgentToolType = "core.agent"

// Manifest is one agent node (root or child). Brain/SystemPrompt configure this
// node; Tools is its unified composition — leaf I/O tools plus `core.agent`
// sub-agents, all sharing one shape.
type Manifest struct {
	Version      int    `json:"version"`
	Name         string `json:"name,omitempty"`
	Brain        string `json:"brain,omitempty"`
	BindingRef   string `json:"binding_ref,omitempty"`
	SystemPrompt string `json:"system_prompt,omitempty"`
	// OnFailure selects how a failure of this node (when it is a delegated child)
	// is handled: OnFailureReport (default) surfaces it to the parent brain as a
	// recoverable failed observation; OnFailurePropagate fails the parent outright.
	OnFailure string `json:"on_failure,omitempty"`
	Tools     []Tool `json:"tools"`
}

// Tool is one entry in an agent's composition. `Type` selects the dispatcher
// implementation; `Name` is the local handle the brain routes to. For a
// `core.agent` tool, Settings decodes to AgentSettings and Tools holds the
// sub-agent's own composition.
type Tool struct {
	Name     string          `json:"name"`
	Type     string          `json:"type"`
	Settings json.RawMessage `json:"settings,omitempty"`
	Tools    []Tool          `json:"tools,omitempty"`
	Hidden   bool            `json:"hidden,omitempty"`
}

// AgentSettings is the Settings shape of a `core.agent` tool.
type AgentSettings struct {
	Code         string `json:"code,omitempty"`
	BindingRef   string `json:"binding_ref,omitempty"`
	SystemPrompt string `json:"system_prompt,omitempty"`
	OnFailure    string `json:"on_failure,omitempty"`
}

// Child failure-handling modes for AgentSettings.OnFailure.
const (
	OnFailureReport    = "report"
	OnFailurePropagate = "propagate"
)

// isAgent reports whether a tool is a sub-agent rather than a leaf I/O tool.
func (t Tool) isAgent() bool { return t.Type == AgentToolType }

// LeafTools returns the node's non-agent tools. Dispatcher providers build these
// via the registry; `core.agent` tools are handled by the runtime instead.
func (m Manifest) LeafTools() []Tool {
	out := make([]Tool, 0, len(m.Tools))
	for _, t := range m.Tools {
		if !t.isAgent() {
			out = append(out, t)
		}
	}
	return out
}

// agentTools returns the node's `core.agent` tools (run by the agent router).
func (m Manifest) agentTools() []Tool {
	out := make([]Tool, 0, len(m.Tools))
	for _, t := range m.Tools {
		if t.isAgent() {
			out = append(out, t)
		}
	}
	return out
}

func decodeAgentSettings(tool Tool) (AgentSettings, error) {
	var settings AgentSettings
	if len(tool.Settings) > 0 {
		if err := json.Unmarshal(tool.Settings, &settings); err != nil {
			return AgentSettings{}, err
		}
	}
	settings.Code = strings.TrimSpace(settings.Code)
	settings.BindingRef = strings.TrimSpace(settings.BindingRef)
	settings.SystemPrompt = strings.TrimSpace(settings.SystemPrompt)
	return settings, nil
}

type DispatcherProvider interface {
	Normalize(toolType string, settings json.RawMessage) (json.RawMessage, error)
	NewDispatcher(context.Context, RunContext, Manifest) (dispatcher.Dispatcher[RunContext], error)
}

func ValidateManifest(manifest Manifest, provider DispatcherProvider) (Manifest, error) {
	if provider == nil {
		return Manifest{}, fmt.Errorf("%w: dispatcher provider is required", ErrInvalid)
	}
	if manifest.Version != ManifestVersion {
		return Manifest{}, fmt.Errorf("%w: manifest version must be %d", ErrInvalid, ManifestVersion)
	}
	manifest.SystemPrompt = strings.TrimSpace(manifest.SystemPrompt)
	manifest.Brain = strings.TrimSpace(manifest.Brain)
	if err := validateTools(manifest.Tools, provider); err != nil {
		return Manifest{}, err
	}
	return cloneManifest(manifest), nil
}

// validateTools normalizes leaf tools and recursively validates sub-agents,
// enforcing unique names within each node.
func validateTools(tools []Tool, provider DispatcherProvider) error {
	seen := make(map[string]struct{}, len(tools))
	for i := range tools {
		tool := &tools[i]
		tool.Name = strings.TrimSpace(tool.Name)
		tool.Type = strings.TrimSpace(tool.Type)
		if tool.Type == "" {
			return fmt.Errorf("%w: tool %d type is required", ErrInvalid, i)
		}
		if tool.isAgent() {
			settings, err := decodeAgentSettings(*tool)
			if err != nil {
				return fmt.Errorf("%w: agent tool %d settings: %v", ErrInvalid, i, err)
			}
			if settings.Code == "" {
				return fmt.Errorf("%w: agent tool %q requires settings.code (brain)", ErrInvalid, tool.Name)
			}
			if tool.Name == "" {
				tool.Name = settings.Code
			}
			switch settings.OnFailure {
			case "", OnFailureReport, OnFailurePropagate:
			default:
				return fmt.Errorf("%w: agent tool %q on_failure must be %q or %q", ErrInvalid, tool.Name, OnFailureReport, OnFailurePropagate)
			}
			if err := validateTools(tool.Tools, provider); err != nil {
				return fmt.Errorf("agent %q: %w", tool.Name, err)
			}
		} else {
			if tool.Name == "" {
				return fmt.Errorf("%w: tool %d name is required", ErrInvalid, i)
			}
			normalized, err := provider.Normalize(tool.Type, tool.Settings)
			if err != nil {
				return fmt.Errorf("%w: tool %q (%s) settings: %v", ErrInvalid, tool.Name, tool.Type, err)
			}
			tool.Settings = append(json.RawMessage(nil), normalized...)
		}
		if _, exists := seen[tool.Name]; exists {
			return fmt.Errorf("%w: duplicate tool name %q", ErrInvalid, tool.Name)
		}
		seen[tool.Name] = struct{}{}
	}
	return nil
}

func cloneManifest(manifest Manifest) Manifest {
	out := manifest
	out.Tools = cloneTools(manifest.Tools)
	return out
}

func cloneTools(tools []Tool) []Tool {
	if len(tools) == 0 {
		return nil
	}
	out := make([]Tool, len(tools))
	for i, tool := range tools {
		out[i] = tool
		out[i].Settings = append(json.RawMessage(nil), tool.Settings...)
		out[i].Tools = cloneTools(tool.Tools)
	}
	return out
}
