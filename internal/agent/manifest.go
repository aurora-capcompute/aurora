package agent

import (
	"capcompute/dispatcher"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"aurora-capcompute/internal/host"
	"aurora-capcompute/internal/internet"
	"aurora-capcompute/internal/llm"
)

const ManifestVersion = 1

type Manifest struct {
	Version      int                `json:"version"`
	SystemPrompt string             `json:"system_prompt,omitempty"`
	Capabilities []CapabilityConfig `json:"capabilities"`
}

type CapabilityConfig struct {
	Name     string          `json:"name"`
	Settings json.RawMessage `json:"settings,omitempty"`
}

type InternetSettings struct {
	Allow            []string `json:"allow"`
	TimeoutMS        int64    `json:"timeout_ms,omitempty"`
	MaxResponseBytes int64    `json:"max_response_bytes,omitempty"`
}

func DefaultManifest(allowlist string) (Manifest, error) {
	settings := InternetSettings{}
	for _, entry := range strings.Split(allowlist, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		method, origin, ok := strings.Cut(entry, ":")
		if !ok || !strings.EqualFold(method, "GET") {
			return Manifest{}, fmt.Errorf("%w: only GET allowlist entries are supported", ErrInvalid)
		}
		settings.Allow = append(settings.Allow, origin)
	}
	raw, err := json.Marshal(settings)
	if err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{Version: ManifestVersion}
	if len(settings.Allow) > 0 {
		manifest.Capabilities = []CapabilityConfig{{Name: "internet.read", Settings: raw}}
	}
	return manifest, nil
}

func ValidateManifest(manifest Manifest) (Manifest, error) {
	if manifest.Version != ManifestVersion {
		return Manifest{}, fmt.Errorf("%w: manifest version must be %d", ErrInvalid, ManifestVersion)
	}
	manifest.SystemPrompt = strings.TrimSpace(manifest.SystemPrompt)
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
		switch capability.Name {
		case "internet.read":
			settings, err := decodeInternetSettings(capability.Settings)
			if err != nil {
				return Manifest{}, fmt.Errorf("%w: internet.read settings: %v", ErrInvalid, err)
			}
			capability.Settings, _ = json.Marshal(settings)
		default:
			return Manifest{}, fmt.Errorf("%w: unsupported capability %q", ErrInvalid, capability.Name)
		}
	}
	return cloneManifest(manifest), nil
}

func EffectiveManifest(base Manifest, overrides []CapabilityConfig) (Manifest, error) {
	effective := cloneManifest(base)
	index := make(map[string]int, len(effective.Capabilities))
	for i, capability := range effective.Capabilities {
		index[capability.Name] = i
	}
	for _, override := range overrides {
		overrideManifest, err := ValidateManifest(Manifest{
			Version:      ManifestVersion,
			SystemPrompt: effective.SystemPrompt,
			Capabilities: []CapabilityConfig{override},
		})
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

func DispatcherConfig(manifest Manifest, llmClient llm.Client) (host.Config, error) {
	config := host.Config{LLM: llmClient}
	for _, capability := range manifest.Capabilities {
		switch capability.Name {
		case "internet.read":
			settings, err := decodeInternetSettings(capability.Settings)
			if err != nil {
				return host.Config{}, err
			}
			entries := make([]string, 0, len(settings.Allow))
			for _, origin := range settings.Allow {
				entries = append(entries, "GET:"+origin)
			}
			policy, err := internet.ParseAllowlist(strings.Join(entries, ","))
			if err != nil {
				return host.Config{}, err
			}
			timeout := time.Duration(settings.TimeoutMS) * time.Millisecond
			config.Internet = internet.NewConfiguredClient(policy, timeout, settings.MaxResponseBytes)
			description := "Read textual content with HTTP GET. Allowed origins: " + strings.Join(settings.Allow, ", ")
			config.Capabilities = append(config.Capabilities, dispatcher.Capability{
				Name:        "internet.read",
				Description: description,
				InputSchema: json.RawMessage(`{"type":"object","properties":{"method":{"type":"string","const":"GET"},"url":{"type":"string","format":"uri"}},"required":["method","url"],"additionalProperties":false}`),
			})
		}
	}
	return config, nil
}

func decodeInternetSettings(raw json.RawMessage) (InternetSettings, error) {
	settings := InternetSettings{
		TimeoutMS:        int64(internet.DefaultTimeout / time.Millisecond),
		MaxResponseBytes: internet.DefaultMaxResponseBytes,
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &settings); err != nil {
			return InternetSettings{}, err
		}
	}
	if len(settings.Allow) == 0 {
		return InternetSettings{}, fmt.Errorf("allow must contain at least one origin or *")
	}
	for i, origin := range settings.Allow {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			return InternetSettings{}, fmt.Errorf("allow entry %d is empty", i)
		}
		settings.Allow[i] = origin
	}
	if settings.TimeoutMS <= 0 {
		return InternetSettings{}, fmt.Errorf("timeout_ms must be positive")
	}
	if settings.MaxResponseBytes <= 0 {
		return InternetSettings{}, fmt.Errorf("max_response_bytes must be positive")
	}
	return settings, nil
}

func cloneManifest(manifest Manifest) Manifest {
	out := manifest
	out.Capabilities = make([]CapabilityConfig, len(manifest.Capabilities))
	for i, capability := range manifest.Capabilities {
		out.Capabilities[i] = capability
		out.Capabilities[i].Settings = append(json.RawMessage(nil), capability.Settings...)
	}
	return out
}
