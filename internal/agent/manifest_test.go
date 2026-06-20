package agent

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEffectiveManifestReplacesAndAddsSupportedCapabilities(t *testing.T) {
	baseSettings := json.RawMessage(`{"allow":["https://go.dev"]}`)
	overrideSettings := json.RawMessage(`{"allow":["*"],"timeout_ms":2000,"max_response_bytes":1024}`)
	base, err := ValidateManifest(Manifest{
		Version:      ManifestVersion,
		SystemPrompt: "research carefully",
		Capabilities: []CapabilityConfig{{Name: "internet.read", Settings: baseSettings}},
	})
	if err != nil {
		t.Fatalf("validate base: %v", err)
	}

	effective, err := EffectiveManifest(base, []CapabilityConfig{{
		Name: "internet.read", Settings: overrideSettings,
	}})
	if err != nil {
		t.Fatalf("effective manifest: %v", err)
	}
	config, err := DispatcherConfig(effective, &finalLLM{})
	if err != nil {
		t.Fatalf("dispatcher config: %v", err)
	}
	if len(config.Capabilities) != 1 || config.Capabilities[0].Name != "internet.read" {
		t.Fatalf("capabilities = %+v", config.Capabilities)
	}
	if !strings.Contains(config.Capabilities[0].Description, "*") {
		t.Fatalf("description does not expose effective wildcard policy: %q", config.Capabilities[0].Description)
	}
}

func TestManifestRejectsUnknownCapability(t *testing.T) {
	_, err := ValidateManifest(Manifest{
		Version: ManifestVersion,
		Capabilities: []CapabilityConfig{{
			Name: "shell.exec",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported capability") {
		t.Fatalf("error = %v", err)
	}
}
