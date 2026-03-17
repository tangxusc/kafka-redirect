package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"kafka-redirect/internal/config"
)

type fakeProducer struct {
	errs  []error
	calls int
	msgs  []*sarama.ProducerMessage
}

func (p *fakeProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	p.msgs = append(p.msgs, msg)
	var err error
	if p.calls < len(p.errs) {
		err = p.errs[p.calls]
	}
	p.calls++
	if err != nil {
		return 0, 0, err
	}
	return 0, int64(p.calls), nil
}

func (p *fakeProducer) Close() error {
	return nil
}

func TestRunReplayWithProducerInvalidLine(t *testing.T) {
	validLine1 := mustRecordLine(t, MessageRecord{
		Topic:     "source-topic",
		Value:     "aGVsbG8=",
		Timestamp: time.Unix(10, 0).UTC(),
	})
	validLine2 := mustRecordLine(t, MessageRecord{
		Topic:     "source-topic",
		Value:     "d29ybGQ=",
		Timestamp: time.Unix(11, 0).UTC(),
	})

	replayPath := writeReplayFile(t, validLine1+"\nnot-json\n"+validLine2+"\n")

	cfg := &config.Config{
		FileSource: &config.FileSourceConfig{
			File:     replayPath,
			Interval: 0,
		},
		Target: &config.TargetConfig{
			Topic: "target-topic",
		},
		Retry: config.RetryConfig{
			MaxAttempts: 2,
			BackoffMS:   0,
		},
	}
	producer := &fakeProducer{}

	stats, err := runReplayWithProducer(context.Background(), cfg, producer, nil, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("runReplayWithProducer() error = %v", err)
	}
	if stats.Total != 3 || stats.Invalid != 1 || stats.Sent != 2 || stats.Skipped != 0 {
		t.Fatalf("stats mismatch: %+v", stats)
	}
}

func TestRunReplayWithProducerRetrySkip(t *testing.T) {
	replayPath := writeReplayFile(
		t,
		mustRecordLine(t, MessageRecord{Topic: "source-topic", Value: "MQ==", Timestamp: time.Now().UTC()})+"\n"+
			mustRecordLine(t, MessageRecord{Topic: "source-topic", Value: "Mg==", Timestamp: time.Now().UTC()})+"\n",
	)

	cfg := &config.Config{
		FileSource: &config.FileSourceConfig{
			File:     replayPath,
			Interval: 0,
		},
		Target: &config.TargetConfig{
			Topic: "target-topic",
		},
		Retry: config.RetryConfig{
			MaxAttempts: 2,
			BackoffMS:   0,
		},
	}
	producer := &fakeProducer{
		errs: []error{
			errors.New("first-fail"),
			errors.New("first-fail-again"),
			nil,
		},
	}

	stats, err := runReplayWithProducer(context.Background(), cfg, producer, nil, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("runReplayWithProducer() error = %v", err)
	}
	if stats.Total != 2 || stats.Sent != 1 || stats.Skipped != 1 {
		t.Fatalf("stats mismatch: %+v", stats)
	}
	if producer.calls != 3 {
		t.Fatalf("producer call count = %d, want 3", producer.calls)
	}
}

func TestRunReplayWithProducerTransformSuccessAndFilter(t *testing.T) {
	replayPath := writeReplayFile(
		t,
		mustRecordLine(t, MessageRecord{Topic: "source-topic", Value: "eyJpZCI6InUtMSIsInR5cGUiOiJub3JtYWwifQ==", Timestamp: time.Now().UTC()})+"\n"+
			mustRecordLine(t, MessageRecord{Topic: "source-topic", Value: "eyJpZCI6InUtMiIsInR5cGUiOiJub2lzZSJ9", Timestamp: time.Now().UTC()})+"\n",
	)

	cfg := &config.Config{
		FileSource: &config.FileSourceConfig{
			File:     replayPath,
			Interval: 0,
		},
		Target: &config.TargetConfig{
			Topic: "target-topic",
		},
		Retry: config.RetryConfig{
			MaxAttempts: 1,
			BackoffMS:   0,
		},
	}
	pipeline, err := buildTransformPipeline(&config.TransformConfig{
		Steps: []config.TransformStepConfig{
			{
				Plugin: "drop_if",
				Expr:   `value.type == "noise"`,
			},
			{
				Plugin: "set_key",
				Expr:   `value.id`,
			},
		},
	})
	if err != nil {
		t.Fatalf("buildTransformPipeline() error = %v", err)
	}

	producer := &fakeProducer{}
	stats, err := runReplayWithProducer(context.Background(), cfg, producer, pipeline, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("runReplayWithProducer() error = %v", err)
	}
	if stats.Total != 2 || stats.Sent != 1 || stats.Filtered != 1 || stats.Skipped != 0 {
		t.Fatalf("stats mismatch: %+v", stats)
	}
	if len(producer.msgs) != 1 {
		t.Fatalf("producer message count = %d, want 1", len(producer.msgs))
	}
	key, keyErr := producer.msgs[0].Key.Encode()
	if keyErr != nil {
		t.Fatalf("encode key error: %v", keyErr)
	}
	if string(key) != "u-1" {
		t.Fatalf("produced key = %q, want u-1", string(key))
	}
}

func TestRunReplayWithProducerTransformError(t *testing.T) {
	replayPath := writeReplayFile(
		t,
		mustRecordLine(t, MessageRecord{Topic: "source-topic", Value: "bm90LWpzb24=", Timestamp: time.Now().UTC()})+"\n",
	)

	cfg := &config.Config{
		FileSource: &config.FileSourceConfig{
			File:     replayPath,
			Interval: 0,
		},
		Target: &config.TargetConfig{
			Topic: "target-topic",
		},
		Retry: config.RetryConfig{
			MaxAttempts: 1,
			BackoffMS:   0,
		},
	}
	pipeline, err := buildTransformPipeline(&config.TransformConfig{
		Steps: []config.TransformStepConfig{
			{
				Plugin: "set_key",
				Expr:   `value.id`,
			},
		},
	})
	if err != nil {
		t.Fatalf("buildTransformPipeline() error = %v", err)
	}

	producer := &fakeProducer{}
	stats, err := runReplayWithProducer(context.Background(), cfg, producer, pipeline, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("runReplayWithProducer() error = %v", err)
	}
	if stats.Total != 1 || stats.TransformErrors != 1 || stats.Skipped != 1 || stats.Sent != 0 {
		t.Fatalf("stats mismatch: %+v", stats)
	}
	if len(producer.msgs) != 0 {
		t.Fatalf("producer should not send message on transform error")
	}
}

func mustRecordLine(t *testing.T, record MessageRecord) string {
	t.Helper()
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return string(data)
}

func writeReplayFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "records.jsonl")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write replay file error: %v", err)
	}
	return path
}
