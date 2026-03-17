package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"

	"kafka-redirect/internal/config"
)

type fakeMarker struct {
	count int
}

func (m *fakeMarker) MarkMessage(_ *sarama.ConsumerMessage, _ string) {
	m.count++
}

func TestForwardProcessorHandleSuccess(t *testing.T) {
	producer := mocks.NewSyncProducer(t, testSaramaConfig())
	defer producer.Close()
	producer.ExpectSendMessageAndSucceed()

	processor := NewForwardProcessor(
		producer,
		nil,
		"target-topic",
		RetryPolicy{MaxAttempts: 1},
		nil,
		log.New(io.Discard, "", 0),
	)

	msg := &sarama.ConsumerMessage{
		Topic:     "source-topic",
		Value:     []byte("hello"),
		Timestamp: time.Now().UTC(),
	}
	err := processor.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	stats := processor.Stats()
	if stats.Received != 1 || stats.Forwarded != 1 || stats.Skipped != 0 {
		t.Fatalf("stats mismatch: %+v", stats)
	}
}

func TestForwardProcessorRetryThenSuccess(t *testing.T) {
	producer := mocks.NewSyncProducer(t, testSaramaConfig())
	defer producer.Close()
	producer.ExpectSendMessageAndFail(errors.New("temporary error"))
	producer.ExpectSendMessageAndSucceed()

	processor := NewForwardProcessor(
		producer,
		nil,
		"target-topic",
		RetryPolicy{MaxAttempts: 2},
		nil,
		log.New(io.Discard, "", 0),
	)

	msg := &sarama.ConsumerMessage{
		Topic:     "source-topic",
		Value:     []byte("hello"),
		Timestamp: time.Now().UTC(),
	}
	err := processor.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	stats := processor.Stats()
	if stats.Forwarded != 1 || stats.Skipped != 0 {
		t.Fatalf("stats mismatch: %+v", stats)
	}
}

func TestForwardProcessorSkipAndContinueWithMark(t *testing.T) {
	producer := mocks.NewSyncProducer(t, testSaramaConfig())
	defer producer.Close()

	producer.ExpectSendMessageAndFail(errors.New("fail-1"))
	producer.ExpectSendMessageAndFail(errors.New("fail-2"))
	producer.ExpectSendMessageAndSucceed()

	processor := NewForwardProcessor(
		producer,
		nil,
		"target-topic",
		RetryPolicy{MaxAttempts: 2},
		nil,
		log.New(io.Discard, "", 0),
	)

	marker := &fakeMarker{}
	first := &sarama.ConsumerMessage{
		Topic:     "source-topic",
		Value:     []byte("first"),
		Timestamp: time.Now().UTC(),
	}
	second := &sarama.ConsumerMessage{
		Topic:     "source-topic",
		Value:     []byte("second"),
		Timestamp: time.Now().UTC(),
	}

	processAndMarkMessage(context.Background(), marker, processor, first)
	processAndMarkMessage(context.Background(), marker, processor, second)

	stats := processor.Stats()
	if stats.Received != 2 || stats.Forwarded != 1 || stats.Skipped != 1 {
		t.Fatalf("stats mismatch: %+v", stats)
	}
	if marker.count != 2 {
		t.Fatalf("mark count = %d, want 2", marker.count)
	}
}

func testSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	return cfg
}

type captureProducer struct {
	errs  []error
	calls int
	msgs  []*sarama.ProducerMessage
}

func (p *captureProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
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

func (p *captureProducer) Close() error {
	return nil
}

func TestForwardProcessorHandleWithTransform(t *testing.T) {
	pipeline, err := buildTransformPipeline(&config.TransformConfig{
		Steps: []config.TransformStepConfig{
			{
				Plugin: "rewrite_value",
				Expr:   `{"id": value.id, "kind": "mapped"}`,
			},
			{
				Plugin: "set_key",
				Expr:   `value.id`,
			},
			{
				Plugin: "set_headers",
				Headers: map[string]string{
					"trace-id": `value.id`,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("buildTransformPipeline() error = %v", err)
	}

	producer := &captureProducer{}
	processor := NewForwardProcessor(
		producer,
		nil,
		"target-topic",
		RetryPolicy{MaxAttempts: 1},
		pipeline,
		log.New(io.Discard, "", 0),
	)

	message := &sarama.ConsumerMessage{
		Topic:     "source-topic",
		Value:     []byte(`{"id":"u-1","kind":"raw"}`),
		Timestamp: time.Now().UTC(),
	}
	if err := processor.Handle(context.Background(), message); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if len(producer.msgs) != 1 {
		t.Fatalf("producer message count = %d, want 1", len(producer.msgs))
	}

	gotValue, err := producer.msgs[0].Value.Encode()
	if err != nil {
		t.Fatalf("encode produced value error = %v", err)
	}
	wantValue := []byte(`{"id":"u-1","kind":"mapped"}`)
	if !bytes.Equal(gotValue, wantValue) {
		t.Fatalf("produced value = %s, want %s", gotValue, wantValue)
	}

	gotKey, err := producer.msgs[0].Key.Encode()
	if err != nil {
		t.Fatalf("encode produced key error = %v", err)
	}
	if string(gotKey) != "u-1" {
		t.Fatalf("produced key = %q, want u-1", string(gotKey))
	}
	if len(producer.msgs[0].Headers) != 1 || string(producer.msgs[0].Headers[0].Value) != "u-1" {
		t.Fatalf("produced headers mismatch: %+v", producer.msgs[0].Headers)
	}
}

func TestForwardProcessorHandleDropByTransform(t *testing.T) {
	pipeline, err := buildTransformPipeline(&config.TransformConfig{
		Steps: []config.TransformStepConfig{
			{
				Plugin: "drop_if",
				Expr:   `value.type == "noise"`,
			},
		},
	})
	if err != nil {
		t.Fatalf("buildTransformPipeline() error = %v", err)
	}

	producer := &captureProducer{}
	processor := NewForwardProcessor(
		producer,
		nil,
		"target-topic",
		RetryPolicy{MaxAttempts: 1},
		pipeline,
		log.New(io.Discard, "", 0),
	)

	err = processor.Handle(context.Background(), &sarama.ConsumerMessage{
		Topic:     "source-topic",
		Value:     []byte(`{"type":"noise"}`),
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	stats := processor.Stats()
	if stats.Filtered != 1 || stats.Forwarded != 0 || len(producer.msgs) != 0 {
		t.Fatalf("stats/producer mismatch: stats=%+v msg_count=%d", stats, len(producer.msgs))
	}
}

func TestForwardProcessorHandleTransformError(t *testing.T) {
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

	producer := &captureProducer{}
	processor := NewForwardProcessor(
		producer,
		nil,
		"target-topic",
		RetryPolicy{MaxAttempts: 1},
		pipeline,
		log.New(io.Discard, "", 0),
	)

	err = processor.Handle(context.Background(), &sarama.ConsumerMessage{
		Topic:     "source-topic",
		Value:     []byte(`not-json`),
		Timestamp: time.Now().UTC(),
	})
	if err == nil {
		t.Fatalf("Handle() error = nil, want error")
	}

	stats := processor.Stats()
	if stats.TransformErrors != 1 || stats.Skipped != 1 || len(producer.msgs) != 0 {
		t.Fatalf("stats/producer mismatch: stats=%+v msg_count=%d", stats, len(producer.msgs))
	}
}
