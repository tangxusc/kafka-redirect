package transform

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type Action int

const (
	ActionContinue Action = iota
	ActionDrop
)

type ApplyResult struct {
	Dropped bool
}

type Step interface {
	Apply(ctx context.Context, msg *Message) (Action, error)
}

type CompileFunc func(expr string) (*compiledExpression, error)

type BuildFunc func(step StepConfig, compile CompileFunc) (Step, error)

type pipelineStep struct {
	name   string
	plugin string
	step   Step
}

type Pipeline struct {
	steps []pipelineStep
}

type Registry struct {
	compiler *celCompiler
	plugins  map[string]BuildFunc
}

func NewDefaultRegistry() (*Registry, error) {
	compiler, err := newCELCompiler()
	if err != nil {
		return nil, err
	}

	registry := &Registry{
		compiler: compiler,
		plugins:  make(map[string]BuildFunc),
	}
	registry.Register(PluginDropIf, buildDropIfStep)
	registry.Register(PluginRewriteValue, buildRewriteValueStep)
	registry.Register(PluginSetHeaders, buildSetHeadersStep)
	registry.Register(PluginSetKey, buildSetKeyStep)
	return registry, nil
}

func (r *Registry) Register(plugin string, build BuildFunc) {
	name := strings.ToLower(strings.TrimSpace(plugin))
	if name == "" || build == nil {
		return
	}
	r.plugins[name] = build
}

func (r *Registry) BuildPipeline(cfg *Config) (*Pipeline, error) {
	if cfg == nil || len(cfg.Steps) == 0 {
		return &Pipeline{}, nil
	}

	if r == nil || r.compiler == nil {
		return nil, errors.New("transform registry is not initialized")
	}

	steps := make([]pipelineStep, 0, len(cfg.Steps))
	for idx := range cfg.Steps {
		stepCfg := normalizeStepConfig(cfg.Steps[idx])
		build, ok := r.plugins[stepCfg.Plugin]
		if !ok {
			return nil, fmt.Errorf("steps[%d] unknown plugin %q", idx, stepCfg.Plugin)
		}

		compiledStep, err := build(stepCfg, r.compiler.Compile)
		if err != nil {
			return nil, fmt.Errorf("steps[%d] build plugin %q: %w", idx, stepCfg.Plugin, err)
		}

		stepName := stepCfg.Name
		if stepName == "" {
			stepName = fmt.Sprintf("%s-%d", stepCfg.Plugin, idx)
		}

		steps = append(steps, pipelineStep{
			name:   stepName,
			plugin: stepCfg.Plugin,
			step:   compiledStep,
		})
	}

	return &Pipeline{steps: steps}, nil
}

func (p *Pipeline) Enabled() bool {
	return p != nil && len(p.steps) > 0
}

func (p *Pipeline) Apply(ctx context.Context, msg *Message) (ApplyResult, error) {
	if msg == nil {
		return ApplyResult{}, errors.New("message is nil")
	}
	if !p.Enabled() {
		return ApplyResult{}, nil
	}

	for _, step := range p.steps {
		action, err := step.step.Apply(ctx, msg)
		if err != nil {
			return ApplyResult{}, fmt.Errorf("step %q plugin=%s: %w", step.name, step.plugin, err)
		}
		if action == ActionDrop {
			return ApplyResult{Dropped: true}, nil
		}
	}

	return ApplyResult{}, nil
}
