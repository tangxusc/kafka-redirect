package transform

import "strings"

const (
	PluginDropIf       = "drop_if"
	PluginRewriteValue = "rewrite_value"
	PluginSetHeaders   = "set_headers"
	PluginSetKey       = "set_key"
)

type Config struct {
	Steps []StepConfig
}

type StepConfig struct {
	Name    string
	Plugin  string
	Expr    string
	Headers map[string]string
}

func normalizeStepConfig(step StepConfig) StepConfig {
	step.Name = strings.TrimSpace(step.Name)
	step.Plugin = strings.ToLower(strings.TrimSpace(step.Plugin))
	step.Expr = strings.TrimSpace(step.Expr)

	if len(step.Headers) == 0 {
		return step
	}

	normalizedHeaders := make(map[string]string, len(step.Headers))
	for key, expr := range step.Headers {
		trimmedKey := strings.TrimSpace(key)
		trimmedExpr := strings.TrimSpace(expr)
		if trimmedKey == "" {
			continue
		}
		normalizedHeaders[trimmedKey] = trimmedExpr
	}
	step.Headers = normalizedHeaders
	return step
}
