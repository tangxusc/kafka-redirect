package service

import (
	"fmt"

	"kafka-redirect/internal/config"
	"kafka-redirect/internal/transform"
)

func buildTransformPipeline(cfg *config.TransformConfig) (*transform.Pipeline, error) {
	if cfg == nil || len(cfg.Steps) == 0 {
		return nil, nil
	}

	registry, err := transform.NewDefaultRegistry()
	if err != nil {
		return nil, fmt.Errorf("create transform registry: %w", err)
	}

	stepConfigs := make([]transform.StepConfig, 0, len(cfg.Steps))
	for _, step := range cfg.Steps {
		stepConfigs = append(stepConfigs, transform.StepConfig{
			Name:    step.Name,
			Plugin:  step.Plugin,
			Expr:    step.Expr,
			Headers: step.Headers,
		})
	}

	pipeline, err := registry.BuildPipeline(&transform.Config{
		Steps: stepConfigs,
	})
	if err != nil {
		return nil, fmt.Errorf("build transform pipeline: %w", err)
	}
	return pipeline, nil
}
