package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAndValidateForward(t *testing.T) {
	configText := `{
  "source": {
    "bootstrap_servers": "localhost:9092",
    "topic": "source-topic",
    "initial_offset": "latest",
    "save": {
      "enabled": true,
      "path": "./tmp",
      "max_size": "1GB"
    }
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  }
}`

	configPath := writeTempFile(t, configText)

	cfg, mode, err := LoadAndValidate(configPath)
	if err != nil {
		t.Fatalf("LoadAndValidate() error = %v", err)
	}
	if mode != ModeForward {
		t.Fatalf("mode = %s, want %s", mode, ModeForward)
	}
	if cfg.Source.GroupID != "kafka-redirect-source-topic" {
		t.Fatalf("group_id = %q, want default group id", cfg.Source.GroupID)
	}
	if cfg.Source.Save.MaxSizeBytes != 1024*1024*1024 {
		t.Fatalf("max_size_bytes = %d, want %d", cfg.Source.Save.MaxSizeBytes, 1024*1024*1024)
	}
	if cfg.Retry.MaxAttempts != 3 || cfg.Retry.BackoffMS != 500 {
		t.Fatalf("retry defaults are not set: %+v", cfg.Retry)
	}
}

func TestLoadAndValidateReplay(t *testing.T) {
	configText := `{
  "file-source": {
    "file": "./records.jsonl",
    "interval": 0
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  },
  "retry": {
    "max_attempts": 5,
    "backoff_ms": 200
  }
}`

	configPath := writeTempFile(t, configText)

	cfg, mode, err := LoadAndValidate(configPath)
	if err != nil {
		t.Fatalf("LoadAndValidate() error = %v", err)
	}
	if mode != ModeReplay {
		t.Fatalf("mode = %s, want %s", mode, ModeReplay)
	}
	if cfg.Retry.MaxAttempts != 5 || cfg.Retry.BackoffMS != 200 {
		t.Fatalf("retry config mismatch: %+v", cfg.Retry)
	}
}

func TestLoadAndValidateConflictMode(t *testing.T) {
	configText := `{
  "source": {
    "bootstrap_servers": "localhost:9092",
    "topic": "source-topic"
  },
  "file-source": {
    "file": "./records.jsonl",
    "interval": 0
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  }
}`

	configPath := writeTempFile(t, configText)

	_, _, err := LoadAndValidate(configPath)
	if err == nil {
		t.Fatalf("LoadAndValidate() error = nil, want error")
	}
}

func TestLoadAndValidateInvalidInitialOffset(t *testing.T) {
	configText := `{
  "source": {
    "bootstrap_servers": "localhost:9092",
    "topic": "source-topic",
    "initial_offset": "middle"
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  }
}`

	configPath := writeTempFile(t, configText)

	_, _, err := LoadAndValidate(configPath)
	if err == nil {
		t.Fatalf("LoadAndValidate() error = nil, want error")
	}
}

func TestParseByteSize(t *testing.T) {
	cases := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		{input: "1GB", want: 1024 * 1024 * 1024},
		{input: "10MB", want: 10 * 1024 * 1024},
		{input: "256KB", want: 256 * 1024},
		{input: "512", want: 512},
		{input: "1TB", wantErr: true},
		{input: "abc", wantErr: true},
	}

	for _, c := range cases {
		got, err := ParseByteSize(c.input)
		if c.wantErr {
			if err == nil {
				t.Fatalf("ParseByteSize(%q) error = nil, want error", c.input)
			}
			continue
		}

		if err != nil {
			t.Fatalf("ParseByteSize(%q) error = %v", c.input, err)
		}
		if got != c.want {
			t.Fatalf("ParseByteSize(%q) = %d, want %d", c.input, got, c.want)
		}
	}
}

func TestLoadJobsAndValidateLegacyCompat(t *testing.T) {
	configText := `{
  "source": {
    "bootstrap_servers": "localhost:9092",
    "topic": "source-topic",
    "initial_offset": "latest"
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  }
}`

	configPath := writeTempFile(t, configText)
	jobs, err := LoadJobsAndValidate(configPath)
	if err != nil {
		t.Fatalf("LoadJobsAndValidate() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs len = %d, want 1", len(jobs))
	}
	if jobs[0].Name != "job-0" {
		t.Fatalf("job name = %q, want job-0", jobs[0].Name)
	}
	if jobs[0].Mode != ModeForward {
		t.Fatalf("job mode = %q, want %q", jobs[0].Mode, ModeForward)
	}
}

func TestLoadJobsAndValidateMixedModes(t *testing.T) {
	configText := `{
  "jobs": [
    {
      "name": "forward-a",
      "source": {
        "bootstrap_servers": "localhost:9092",
        "topic": "source-topic"
      },
      "target": {
        "bootstrap_servers": "localhost:9092",
        "topic": "target-topic"
      }
    },
    {
      "file-source": {
        "file": "./records.jsonl",
        "interval": 0
      },
      "target": {
        "bootstrap_servers": "localhost:9092",
        "topic": "target-topic-2"
      }
    }
  ]
}`

	configPath := writeTempFile(t, configText)
	jobs, err := LoadJobsAndValidate(configPath)
	if err != nil {
		t.Fatalf("LoadJobsAndValidate() error = %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("jobs len = %d, want 2", len(jobs))
	}
	if jobs[0].Name != "forward-a" || jobs[0].Mode != ModeForward {
		t.Fatalf("job0 mismatch: %+v", jobs[0])
	}
	if jobs[1].Name != "job-1" || jobs[1].Mode != ModeReplay {
		t.Fatalf("job1 mismatch: %+v", jobs[1])
	}
}

func TestLoadJobsAndValidateEmptyJobs(t *testing.T) {
	configPath := writeTempFile(t, `{"jobs":[]}`)
	_, err := LoadJobsAndValidate(configPath)
	if err == nil {
		t.Fatalf("LoadJobsAndValidate() error = nil, want error")
	}
}

func TestLoadJobsAndValidateDuplicateName(t *testing.T) {
	configText := `{
  "jobs": [
    {
      "name": "dup",
      "source": {
        "bootstrap_servers": "localhost:9092",
        "topic": "source-topic"
      },
      "target": {
        "bootstrap_servers": "localhost:9092",
        "topic": "target-topic"
      }
    },
    {
      "name": " dup ",
      "file-source": {
        "file": "./records.jsonl",
        "interval": 0
      },
      "target": {
        "bootstrap_servers": "localhost:9092",
        "topic": "target-topic-2"
      }
    }
  ]
}`

	configPath := writeTempFile(t, configText)
	_, err := LoadJobsAndValidate(configPath)
	if err == nil {
		t.Fatalf("LoadJobsAndValidate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "duplicate job name") {
		t.Fatalf("error = %v, want duplicate name", err)
	}
}

func TestLoadJobsAndValidateModeConflict(t *testing.T) {
	configText := `{
  "jobs": [
    {
      "name": "bad",
      "source": {
        "bootstrap_servers": "localhost:9092",
        "topic": "source-topic"
      },
      "file-source": {
        "file": "./records.jsonl",
        "interval": 0
      },
      "target": {
        "bootstrap_servers": "localhost:9092",
        "topic": "target-topic"
      }
    }
  ]
}`

	configPath := writeTempFile(t, configText)
	_, err := LoadJobsAndValidate(configPath)
	if err == nil {
		t.Fatalf("LoadJobsAndValidate() error = nil, want error")
	}
}

func TestLoadAndValidateTransformConfig(t *testing.T) {
	configText := `{
  "source": {
    "bootstrap_servers": "localhost:9092",
    "topic": "source-topic",
    "initial_offset": "latest"
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  },
  "transform": {
    "steps": [
      {
        "name": "drop-noise",
        "plugin": "DROP_IF",
        "expr": "value.type == 'noise'"
      },
      {
        "name": "set-h",
        "plugin": "set_headers",
        "headers": {
          "trace-id": "value.id",
          "source": "\"redirect\""
        }
      }
    ]
  }
}`

	configPath := writeTempFile(t, configText)
	cfg, mode, err := LoadAndValidate(configPath)
	if err != nil {
		t.Fatalf("LoadAndValidate() error = %v", err)
	}
	if mode != ModeForward {
		t.Fatalf("mode = %s, want %s", mode, ModeForward)
	}
	if cfg.Transform == nil || len(cfg.Transform.Steps) != 2 {
		t.Fatalf("transform steps mismatch: %+v", cfg.Transform)
	}
	if cfg.Transform.Steps[0].Plugin != "drop_if" {
		t.Fatalf("plugin normalization failed: %q", cfg.Transform.Steps[0].Plugin)
	}
}

func TestLoadAndValidateTransformInvalidPlugin(t *testing.T) {
	configText := `{
  "source": {
    "bootstrap_servers": "localhost:9092",
    "topic": "source-topic",
    "initial_offset": "latest"
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  },
  "transform": {
    "steps": [
      {
        "plugin": "unknown",
        "expr": "true"
      }
    ]
  }
}`

	configPath := writeTempFile(t, configText)
	_, _, err := LoadAndValidate(configPath)
	if err == nil {
		t.Fatalf("LoadAndValidate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "transform.steps[0].plugin is invalid") {
		t.Fatalf("error = %v, want invalid plugin error", err)
	}
}

func TestLoadAndValidateTransformMissingExpr(t *testing.T) {
	configText := `{
  "source": {
    "bootstrap_servers": "localhost:9092",
    "topic": "source-topic",
    "initial_offset": "latest"
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  },
  "transform": {
    "steps": [
      {
        "plugin": "set_key"
      }
    ]
  }
}`

	configPath := writeTempFile(t, configText)
	_, _, err := LoadAndValidate(configPath)
	if err == nil {
		t.Fatalf("LoadAndValidate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "transform.steps[0].expr is required") {
		t.Fatalf("error = %v, want missing expr error", err)
	}
}

func TestLoadAndValidateTransformMissingHeaders(t *testing.T) {
	configText := `{
  "source": {
    "bootstrap_servers": "localhost:9092",
    "topic": "source-topic",
    "initial_offset": "latest"
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  },
  "transform": {
    "steps": [
      {
        "plugin": "set_headers"
      }
    ]
  }
}`

	configPath := writeTempFile(t, configText)
	_, _, err := LoadAndValidate(configPath)
	if err == nil {
		t.Fatalf("LoadAndValidate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "transform.steps[0].headers is required") {
		t.Fatalf("error = %v, want missing headers error", err)
	}
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}
	return path
}
