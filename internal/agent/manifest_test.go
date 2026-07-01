package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aurora-capcompute/capcompute/dispatcher"
)

type testDispatchers struct {
	normalized []string
}

func (p *testDispatchers) Normalize(toolType string, settings json.RawMessage) (json.RawMessage, error) {
	if toolType == "unknown" {
		return nil, fmt.Errorf("unsupported tool type")
	}
	p.normalized = append(p.normalized, toolType)
	if len(settings) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return append(json.RawMessage(nil), settings...), nil
}

func (*testDispatchers) NewDispatcher(context.Context, RunContext, Manifest) (dispatcher.Dispatcher[RunContext], error) {
	return nil, nil
}

func TestValidateManifestUsesInjectedProvider(t *testing.T) {
	provider := &testDispatchers{}
	manifest, err := ValidateManifest(Manifest{
		Version: ManifestVersion,
		Brain:   "brain",
		Tools: []Tool{{
			Name: " custom ",
			Type: "core.custom",
		}},
	}, provider)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if manifest.Tools[0].Name != "custom" {
		t.Fatalf("manifest = %+v", manifest)
	}
	if string(manifest.Tools[0].Settings) != "{}" {
		t.Fatalf("settings = %s", manifest.Tools[0].Settings)
	}
}

func TestValidateManifestRejectsMissingProviderAndUnknownType(t *testing.T) {
	if _, err := ValidateManifest(Manifest{Version: ManifestVersion}, nil); err == nil {
		t.Fatal("expected missing provider error")
	}
	if _, err := ValidateManifest(Manifest{
		Version: ManifestVersion,
		Tools:   []Tool{{Name: "x", Type: "unknown"}},
	}, &testDispatchers{}); err == nil {
		t.Fatal("expected unsupported type error")
	}
}

// A core.agent tool requires a brain (settings.code) and recurses into its tools.
func TestValidateManifestValidatesNestedAgent(t *testing.T) {
	_, err := ValidateManifest(Manifest{
		Version: ManifestVersion,
		Brain:   "root",
		Tools: []Tool{{
			Name:     "scout",
			Type:     AgentToolType,
			Settings: json.RawMessage(`{}`),
		}},
	}, &testDispatchers{})
	if err == nil {
		t.Fatal("expected error: agent tool without settings.code")
	}
}
