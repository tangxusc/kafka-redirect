package main

import (
	"context"
	"errors"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"kafka-redirect/internal/config"
)

func TestRunJobsDispatchByMode(t *testing.T) {
	origForward := runForwardFunc
	origReplay := runReplayFunc
	defer func() {
		runForwardFunc = origForward
		runReplayFunc = origReplay
	}()

	var forwardCalls atomic.Int32
	var replayCalls atomic.Int32

	runForwardFunc = func(ctx context.Context, cfg *config.Config, logger *log.Logger) error {
		_ = ctx
		_ = cfg
		_ = logger
		forwardCalls.Add(1)
		return nil
	}
	runReplayFunc = func(ctx context.Context, cfg *config.Config, logger *log.Logger) error {
		_ = ctx
		_ = cfg
		_ = logger
		replayCalls.Add(1)
		return nil
	}

	jobs := []config.ValidatedJob{
		{
			Name: "forward-a",
			Mode: config.ModeForward,
			Config: &config.Config{
				Source: &config.SourceConfig{
					BootstrapServers: "localhost:9092",
					Topic:            "source",
					InitialOffset:    "latest",
				},
				Target: &config.TargetConfig{
					BootstrapServers: "localhost:9092",
					Topic:            "target",
				},
				Retry: config.RetryConfig{MaxAttempts: 1, BackoffMS: 0},
			},
		},
		{
			Name: "replay-a",
			Mode: config.ModeReplay,
			Config: &config.Config{
				FileSource: &config.FileSourceConfig{
					File:     "./records.jsonl",
					Interval: 0,
				},
				Target: &config.TargetConfig{
					BootstrapServers: "localhost:9092",
					Topic:            "target",
				},
				Retry: config.RetryConfig{MaxAttempts: 1, BackoffMS: 0},
			},
		},
	}

	if err := runJobs(context.Background(), jobs); err != nil {
		t.Fatalf("runJobs() error = %v", err)
	}

	if forwardCalls.Load() != 1 {
		t.Fatalf("forward calls = %d, want 1", forwardCalls.Load())
	}
	if replayCalls.Load() != 1 {
		t.Fatalf("replay calls = %d, want 1", replayCalls.Load())
	}
}

func TestRunJobsFailFastCancelsOtherJobs(t *testing.T) {
	origForward := runForwardFunc
	origReplay := runReplayFunc
	defer func() {
		runForwardFunc = origForward
		runReplayFunc = origReplay
	}()

	replayStarted := make(chan struct{})
	var replayCanceled atomic.Bool

	runReplayFunc = func(ctx context.Context, cfg *config.Config, logger *log.Logger) error {
		_ = cfg
		_ = logger
		close(replayStarted)
		select {
		case <-ctx.Done():
			replayCanceled.Store(true)
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return errors.New("replay timeout waiting for cancel")
		}
	}
	runForwardFunc = func(ctx context.Context, cfg *config.Config, logger *log.Logger) error {
		_ = ctx
		_ = cfg
		_ = logger
		<-replayStarted
		return errors.New("forward failed")
	}

	jobs := []config.ValidatedJob{
		{
			Name: "forward-a",
			Mode: config.ModeForward,
			Config: &config.Config{
				Source: &config.SourceConfig{
					BootstrapServers: "localhost:9092",
					Topic:            "source",
					InitialOffset:    "latest",
				},
				Target: &config.TargetConfig{
					BootstrapServers: "localhost:9092",
					Topic:            "target",
				},
				Retry: config.RetryConfig{MaxAttempts: 1, BackoffMS: 0},
			},
		},
		{
			Name: "replay-a",
			Mode: config.ModeReplay,
			Config: &config.Config{
				FileSource: &config.FileSourceConfig{
					File:     "./records.jsonl",
					Interval: 0,
				},
				Target: &config.TargetConfig{
					BootstrapServers: "localhost:9092",
					Topic:            "target",
				},
				Retry: config.RetryConfig{MaxAttempts: 1, BackoffMS: 0},
			},
		},
	}

	err := runJobs(context.Background(), jobs)
	if err == nil {
		t.Fatalf("runJobs() error = nil, want error")
	}
	if err.Error() != "forward failed" {
		t.Fatalf("runJobs() error = %v, want forward failed", err)
	}
	if !replayCanceled.Load() {
		t.Fatalf("replay job should be canceled on fail-fast")
	}
}

func TestBuildJobLoggerPrefix(t *testing.T) {
	logger := buildJobLogger("job-a", config.ModeForward)
	if logger == nil {
		t.Fatalf("buildJobLogger() returned nil")
	}

	logger.SetOutput(io.Discard)
	logger.Printf("hello")
}
