package config

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

const (
	defaultRetryAttempts = 3
	defaultRetryBackoff  = 500
)

var sizePattern = regexp.MustCompile(`^(\d+)\s*(B|KB|MB|GB)?$`)

type Mode string

const (
	ModeForward Mode = "forward"
	ModeReplay  Mode = "replay"
)

type Config struct {
	Source     *SourceConfig     `mapstructure:"source" json:"source,omitempty"`
	FileSource *FileSourceConfig `mapstructure:"file-source" json:"file-source,omitempty"`
	Target     *TargetConfig     `mapstructure:"target" json:"target,omitempty"`
	Transform  *TransformConfig  `mapstructure:"transform" json:"transform,omitempty"`
	Retry      RetryConfig       `mapstructure:"retry" json:"retry"`
}

type Job struct {
	Name       string            `mapstructure:"name" json:"name,omitempty"`
	Source     *SourceConfig     `mapstructure:"source" json:"source,omitempty"`
	FileSource *FileSourceConfig `mapstructure:"file-source" json:"file-source,omitempty"`
	Target     *TargetConfig     `mapstructure:"target" json:"target,omitempty"`
	Transform  *TransformConfig  `mapstructure:"transform" json:"transform,omitempty"`
	Retry      RetryConfig       `mapstructure:"retry" json:"retry"`
}

type MultiConfig struct {
	Jobs []Job `mapstructure:"jobs" json:"jobs"`
}

type ValidatedJob struct {
	Name   string
	Mode   Mode
	Config *Config
}

type SourceConfig struct {
	BootstrapServers string     `mapstructure:"bootstrap_servers" json:"bootstrap_servers"`
	Topic            string     `mapstructure:"topic" json:"topic"`
	GroupID          string     `mapstructure:"group_id" json:"group_id,omitempty"`
	InitialOffset    string     `mapstructure:"initial_offset" json:"initial_offset,omitempty"`
	Save             SaveConfig `mapstructure:"save" json:"save"`
}

type SaveConfig struct {
	Path         string `mapstructure:"path" json:"path"`
	Enabled      bool   `mapstructure:"enabled" json:"enabled"`
	MaxSize      string `mapstructure:"max_size" json:"max_size,omitempty"`
	MaxSizeBytes int64  `mapstructure:"-" json:"-"`
}

type FileSourceConfig struct {
	File     string `mapstructure:"file" json:"file"`
	Interval int64  `mapstructure:"interval" json:"interval"`
}

type TargetConfig struct {
	BootstrapServers string `mapstructure:"bootstrap_servers" json:"bootstrap_servers"`
	Topic            string `mapstructure:"topic" json:"topic"`
}

type RetryConfig struct {
	MaxAttempts int `mapstructure:"max_attempts" json:"max_attempts"`
	BackoffMS   int `mapstructure:"backoff_ms" json:"backoff_ms"`
}

type TransformConfig struct {
	Steps []TransformStepConfig `mapstructure:"steps" json:"steps"`
}

type TransformStepConfig struct {
	Name    string            `mapstructure:"name" json:"name,omitempty"`
	Plugin  string            `mapstructure:"plugin" json:"plugin"`
	Expr    string            `mapstructure:"expr" json:"expr,omitempty"`
	Headers map[string]string `mapstructure:"headers" json:"headers,omitempty"`
}

func Load(path string) (*Config, error) {
	v, err := loadViper(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	normalizeConfig(cfg)
	return cfg, nil
}

func loadViper(path string) (*viper.Viper, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("config path is empty")
	}

	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("json")
	v.SetDefault("retry.max_attempts", defaultRetryAttempts)
	v.SetDefault("retry.backoff_ms", defaultRetryBackoff)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return v, nil
}

func LoadAndValidate(path string) (*Config, Mode, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, "", err
	}

	mode, err := DetectMode(cfg)
	if err != nil {
		return nil, "", err
	}

	if err := cfg.Validate(mode); err != nil {
		return nil, "", err
	}

	return cfg, mode, nil
}

func LoadJobsAndValidate(path string) ([]ValidatedJob, error) {
	v, err := loadViper(path)
	if err != nil {
		return nil, err
	}

	if !v.IsSet("jobs") {
		cfg, mode, loadErr := LoadAndValidate(path)
		if loadErr != nil {
			return nil, loadErr
		}
		return []ValidatedJob{
			{
				Name:   "job-0",
				Mode:   mode,
				Config: cfg,
			},
		}, nil
	}

	var multiCfg MultiConfig
	if err := v.Unmarshal(&multiCfg); err != nil {
		return nil, fmt.Errorf("unmarshal multi config: %w", err)
	}

	if len(multiCfg.Jobs) == 0 {
		return nil, errors.New("jobs is empty")
	}

	validated := make([]ValidatedJob, 0, len(multiCfg.Jobs))
	nameSet := make(map[string]struct{}, len(multiCfg.Jobs))
	for idx := range multiCfg.Jobs {
		job := multiCfg.Jobs[idx]
		cfg := &Config{
			Source:     job.Source,
			FileSource: job.FileSource,
			Target:     job.Target,
			Transform:  job.Transform,
			Retry:      job.Retry,
		}
		normalizeConfig(cfg)

		mode, modeErr := DetectMode(cfg)
		if modeErr != nil {
			return nil, fmt.Errorf("jobs[%d] mode validate failed: %w", idx, modeErr)
		}
		if validateErr := cfg.Validate(mode); validateErr != nil {
			return nil, fmt.Errorf("jobs[%d] config validate failed: %w", idx, validateErr)
		}

		name := strings.TrimSpace(job.Name)
		if name == "" {
			name = fmt.Sprintf("job-%d", idx)
		}

		if _, exists := nameSet[name]; exists {
			return nil, fmt.Errorf("duplicate job name: %s", name)
		}
		nameSet[name] = struct{}{}

		validated = append(validated, ValidatedJob{
			Name:   name,
			Mode:   mode,
			Config: cfg,
		})
	}
	return validated, nil
}

func DetectMode(cfg *Config) (Mode, error) {
	if cfg == nil {
		return "", errors.New("config is nil")
	}

	hasSource := cfg.Source != nil
	hasFileSource := cfg.FileSource != nil

	switch {
	case hasSource && !hasFileSource:
		return ModeForward, nil
	case !hasSource && hasFileSource:
		return ModeReplay, nil
	case hasSource && hasFileSource:
		return "", errors.New("invalid config: source and file-source cannot both exist")
	default:
		return "", errors.New("invalid config: source or file-source must exist")
	}
}

func (cfg *Config) Validate(mode Mode) error {
	if cfg.Target == nil {
		return errors.New("target config is required")
	}
	if cfg.Target.BootstrapServers == "" {
		return errors.New("target.bootstrap_servers is required")
	}
	if cfg.Target.Topic == "" {
		return errors.New("target.topic is required")
	}

	if cfg.Retry.MaxAttempts <= 0 {
		return errors.New("retry.max_attempts must be greater than 0")
	}
	if cfg.Retry.BackoffMS < 0 {
		return errors.New("retry.backoff_ms must be greater than or equal to 0")
	}
	if err := cfg.validateTransform(); err != nil {
		return err
	}

	switch mode {
	case ModeForward:
		return cfg.validateForward()
	case ModeReplay:
		return cfg.validateReplay()
	default:
		return fmt.Errorf("unknown mode: %s", mode)
	}
}

var transformPlugins = map[string]struct{}{
	"drop_if":       {},
	"rewrite_value": {},
	"set_headers":   {},
	"set_key":       {},
}

func (cfg *Config) validateTransform() error {
	if cfg.Transform == nil {
		return nil
	}

	if len(cfg.Transform.Steps) == 0 {
		return errors.New("transform.steps is empty")
	}

	for idx := range cfg.Transform.Steps {
		step := cfg.Transform.Steps[idx]
		if step.Plugin == "" {
			return fmt.Errorf("transform.steps[%d].plugin is required", idx)
		}
		if _, exists := transformPlugins[step.Plugin]; !exists {
			return fmt.Errorf("transform.steps[%d].plugin is invalid: %s", idx, step.Plugin)
		}

		switch step.Plugin {
		case "drop_if", "rewrite_value", "set_key":
			if step.Expr == "" {
				return fmt.Errorf("transform.steps[%d].expr is required for plugin %s", idx, step.Plugin)
			}
		case "set_headers":
			if len(step.Headers) == 0 {
				return fmt.Errorf("transform.steps[%d].headers is required for plugin %s", idx, step.Plugin)
			}
			for key, expr := range step.Headers {
				if strings.TrimSpace(key) == "" {
					return fmt.Errorf("transform.steps[%d].headers contains empty key", idx)
				}
				if strings.TrimSpace(expr) == "" {
					return fmt.Errorf("transform.steps[%d].headers[%s] expression is empty", idx, key)
				}
			}
		}
	}
	return nil
}

func (cfg *Config) validateForward() error {
	if cfg.Source == nil {
		return errors.New("source config is required in forward mode")
	}
	if cfg.Source.BootstrapServers == "" {
		return errors.New("source.bootstrap_servers is required")
	}
	if cfg.Source.Topic == "" {
		return errors.New("source.topic is required")
	}
	if cfg.Source.GroupID == "" {
		cfg.Source.GroupID = fmt.Sprintf("kafka-redirect-%s", cfg.Source.Topic)
	}

	switch cfg.Source.InitialOffset {
	case "latest", "earliest":
	default:
		return errors.New("source.initial_offset must be latest or earliest")
	}

	if cfg.Source.Save.Enabled {
		if cfg.Source.Save.Path == "" {
			return errors.New("source.save.path is required when source.save.enabled=true")
		}

		if cfg.Source.Save.MaxSize == "" {
			cfg.Source.Save.MaxSizeBytes = 0
			return nil
		}

		maxBytes, err := ParseByteSize(cfg.Source.Save.MaxSize)
		if err != nil {
			return fmt.Errorf("invalid source.save.max_size: %w", err)
		}
		cfg.Source.Save.MaxSizeBytes = maxBytes
		return nil
	}

	cfg.Source.Save.MaxSizeBytes = 0
	return nil
}

func (cfg *Config) validateReplay() error {
	if cfg.FileSource == nil {
		return errors.New("file-source config is required in replay mode")
	}
	if cfg.FileSource.File == "" {
		return errors.New("file-source.file is required")
	}
	if cfg.FileSource.Interval < 0 {
		return errors.New("file-source.interval must be greater than or equal to 0")
	}
	return nil
}

func ParseByteSize(value string) (int64, error) {
	raw := strings.ToUpper(strings.TrimSpace(value))
	if raw == "" {
		return 0, errors.New("size is empty")
	}

	matches := sizePattern.FindStringSubmatch(raw)
	if matches == nil {
		return 0, errors.New("size must match [number][B|KB|MB|GB], e.g. 1GB")
	}

	number, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size number: %w", err)
	}
	if number < 0 {
		return 0, errors.New("size cannot be negative")
	}

	unit := matches[2]
	multiplier := int64(1)
	switch unit {
	case "", "B":
		multiplier = 1
	case "KB":
		multiplier = 1024
	case "MB":
		multiplier = 1024 * 1024
	case "GB":
		multiplier = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unsupported unit: %s", unit)
	}

	if number > math.MaxInt64/multiplier {
		return 0, errors.New("size overflows int64")
	}

	return number * multiplier, nil
}

func normalizeConfig(cfg *Config) {
	if cfg == nil {
		return
	}

	if cfg.Source != nil {
		cfg.Source.BootstrapServers = strings.TrimSpace(cfg.Source.BootstrapServers)
		cfg.Source.Topic = strings.TrimSpace(cfg.Source.Topic)
		cfg.Source.GroupID = strings.TrimSpace(cfg.Source.GroupID)
		cfg.Source.InitialOffset = strings.ToLower(strings.TrimSpace(cfg.Source.InitialOffset))
		if cfg.Source.InitialOffset == "" {
			cfg.Source.InitialOffset = "latest"
		}

		cfg.Source.Save.Path = strings.TrimSpace(cfg.Source.Save.Path)
		cfg.Source.Save.MaxSize = strings.TrimSpace(cfg.Source.Save.MaxSize)
	}

	if cfg.FileSource != nil {
		cfg.FileSource.File = strings.TrimSpace(cfg.FileSource.File)
	}

	if cfg.Target != nil {
		cfg.Target.BootstrapServers = strings.TrimSpace(cfg.Target.BootstrapServers)
		cfg.Target.Topic = strings.TrimSpace(cfg.Target.Topic)
	}

	if cfg.Transform != nil {
		for idx := range cfg.Transform.Steps {
			step := &cfg.Transform.Steps[idx]
			step.Name = strings.TrimSpace(step.Name)
			step.Plugin = strings.ToLower(strings.TrimSpace(step.Plugin))
			step.Expr = strings.TrimSpace(step.Expr)
			if len(step.Headers) == 0 {
				continue
			}

			normalizedHeaders := make(map[string]string, len(step.Headers))
			for key, expr := range step.Headers {
				trimmedKey := strings.TrimSpace(key)
				if trimmedKey == "" {
					continue
				}
				normalizedHeaders[trimmedKey] = strings.TrimSpace(expr)
			}
			step.Headers = normalizedHeaders
		}
	}

	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = defaultRetryAttempts
	}
	if cfg.Retry.BackoffMS == 0 {
		cfg.Retry.BackoffMS = defaultRetryBackoff
	}
}
