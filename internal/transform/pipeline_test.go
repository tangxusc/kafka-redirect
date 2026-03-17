package transform

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestPipelineApplyRewriteHeadersAndKey(t *testing.T) {
	registry, err := NewDefaultRegistry()
	if err != nil {
		t.Fatalf("NewDefaultRegistry() error = %v", err)
	}

	pipeline, err := registry.BuildPipeline(&Config{
		Steps: []StepConfig{
			{
				Plugin: PluginRewriteValue,
				Expr:   `{"id": value.id, "ts": value.ts}`,
			},
			{
				Plugin: PluginSetHeaders,
				Headers: map[string]string{
					"trace-id": `value.id`,
					"source":   `"redirect"`,
				},
			},
			{
				Plugin: PluginSetKey,
				Expr:   `value.id`,
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildPipeline() error = %v", err)
	}

	msg := &Message{
		Value:     []byte(`{"id":"abc","ts":"2026-03-17T00:00:00Z"}`),
		Headers:   []Header{{Key: "source", Value: []byte("legacy")}},
		Timestamp: time.Now().UTC(),
	}

	result, err := pipeline.Apply(context.Background(), msg)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if result.Dropped {
		t.Fatalf("Apply() dropped = true, want false")
	}

	var payload map[string]any
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		t.Fatalf("json.Unmarshal transformed value error = %v", err)
	}
	if payload["id"] != "abc" {
		t.Fatalf("transformed value id = %v, want abc", payload["id"])
	}
	if payload["ts"] != "2026-03-17T00:00:00Z" {
		t.Fatalf("transformed value ts = %v, want timestamp", payload["ts"])
	}

	if string(msg.Key) != "abc" {
		t.Fatalf("message key = %q, want abc", string(msg.Key))
	}

	headers := msg.HeaderStringMap()
	if headers["trace-id"] != "abc" {
		t.Fatalf("header trace-id = %q, want abc", headers["trace-id"])
	}
	if headers["source"] != "redirect" {
		t.Fatalf("header source = %q, want redirect", headers["source"])
	}
}

func TestPipelineApplyDropIf(t *testing.T) {
	registry, err := NewDefaultRegistry()
	if err != nil {
		t.Fatalf("NewDefaultRegistry() error = %v", err)
	}

	pipeline, err := registry.BuildPipeline(&Config{
		Steps: []StepConfig{
			{
				Plugin: PluginDropIf,
				Expr:   `value.type == "noise"`,
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildPipeline() error = %v", err)
	}

	msg := &Message{Value: []byte(`{"type":"noise"}`)}
	result, err := pipeline.Apply(context.Background(), msg)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !result.Dropped {
		t.Fatalf("Apply() dropped = false, want true")
	}
}

func TestBuildPipelineInvalidExpression(t *testing.T) {
	registry, err := NewDefaultRegistry()
	if err != nil {
		t.Fatalf("NewDefaultRegistry() error = %v", err)
	}

	_, err = registry.BuildPipeline(&Config{
		Steps: []StepConfig{
			{
				Plugin: PluginDropIf,
				Expr:   `value.`,
			},
		},
	})
	if err == nil {
		t.Fatalf("BuildPipeline() error = nil, want error")
	}
}

func TestPipelineApplyDropIfTypeMismatch(t *testing.T) {
	registry, err := NewDefaultRegistry()
	if err != nil {
		t.Fatalf("NewDefaultRegistry() error = %v", err)
	}

	pipeline, err := registry.BuildPipeline(&Config{
		Steps: []StepConfig{
			{
				Plugin: PluginDropIf,
				Expr:   `"bad"`,
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildPipeline() error = %v", err)
	}

	_, err = pipeline.Apply(context.Background(), &Message{
		Value: []byte(`{"ok":true}`),
	})
	if err == nil {
		t.Fatalf("Apply() error = nil, want error")
	}
}
