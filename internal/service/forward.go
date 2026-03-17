package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"

	"kafka-redirect/internal/config"
	"kafka-redirect/internal/transform"
)

type Producer interface {
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
	Close() error
}

type ForwardStats struct {
	received        atomic.Int64
	forwarded       atomic.Int64
	skipped         atomic.Int64
	filtered        atomic.Int64
	transformErrors atomic.Int64
	saved           atomic.Int64
	saveErrors      atomic.Int64
}

type ForwardStatsSnapshot struct {
	Received        int64
	Forwarded       int64
	Skipped         int64
	Filtered        int64
	TransformErrors int64
	Saved           int64
	SaveErrors      int64
}

func (s *ForwardStats) Snapshot() ForwardStatsSnapshot {
	return ForwardStatsSnapshot{
		Received:        s.received.Load(),
		Forwarded:       s.forwarded.Load(),
		Skipped:         s.skipped.Load(),
		Filtered:        s.filtered.Load(),
		TransformErrors: s.transformErrors.Load(),
		Saved:           s.saved.Load(),
		SaveErrors:      s.saveErrors.Load(),
	}
}

type ForwardProcessor struct {
	producer    Producer
	saver       *FileSaver
	targetTopic string
	retryPolicy RetryPolicy
	pipeline    *transform.Pipeline
	stats       ForwardStats
	logger      *log.Logger
}

func NewForwardProcessor(
	producer Producer,
	saver *FileSaver,
	targetTopic string,
	retryPolicy RetryPolicy,
	pipeline *transform.Pipeline,
	logger *log.Logger,
) *ForwardProcessor {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	if retryPolicy.MaxAttempts <= 0 {
		retryPolicy.MaxAttempts = 1
	}
	if retryPolicy.Backoff < 0 {
		retryPolicy.Backoff = 0
	}

	return &ForwardProcessor{
		producer:    producer,
		saver:       saver,
		targetTopic: targetTopic,
		retryPolicy: retryPolicy,
		pipeline:    pipeline,
		logger:      logger,
	}
}

func (p *ForwardProcessor) Handle(ctx context.Context, msg *sarama.ConsumerMessage) error {
	if msg == nil {
		return errors.New("consumer message is nil")
	}

	p.stats.received.Add(1)
	record := ConsumerMessageToRecord(msg)
	transformMessage := ConsumerMessageToTransformMessage(msg)
	if transformMessage == nil {
		p.stats.skipped.Add(1)
		return errors.New("failed to convert consumer message")
	}

	if p.saver != nil {
		if err := p.saver.Save(record); err != nil {
			p.stats.saveErrors.Add(1)
			p.logger.Printf(
				"save message failed topic=%s partition=%d offset=%d err=%v",
				msg.Topic,
				msg.Partition,
				msg.Offset,
				err,
			)
		} else {
			p.stats.saved.Add(1)
		}
	}

	if p.pipeline != nil {
		result, applyErr := p.pipeline.Apply(ctx, transformMessage)
		if applyErr != nil {
			p.stats.transformErrors.Add(1)
			p.stats.skipped.Add(1)
			p.logger.Printf(
				"transform message failed topic=%s partition=%d offset=%d err=%v",
				msg.Topic,
				msg.Partition,
				msg.Offset,
				applyErr,
			)
			return applyErr
		}
		if result.Dropped {
			p.stats.filtered.Add(1)
			return nil
		}
	}

	err := SendWithRetry(ctx, p.retryPolicy, func() error {
		producerMessage, convertErr := TransformMessageToProducerMessage(transformMessage, p.targetTopic)
		if convertErr != nil {
			return convertErr
		}

		_, _, sendErr := p.producer.SendMessage(producerMessage)
		return sendErr
	})
	if err != nil {
		p.stats.skipped.Add(1)
		p.logger.Printf(
			"skip message after retries topic=%s partition=%d offset=%d err=%v",
			msg.Topic,
			msg.Partition,
			msg.Offset,
			err,
		)
		return err
	}

	p.stats.forwarded.Add(1)
	return nil
}

func (p *ForwardProcessor) Stats() ForwardStatsSnapshot {
	return p.stats.Snapshot()
}

type messageMarker interface {
	MarkMessage(msg *sarama.ConsumerMessage, metadata string)
}

func processAndMarkMessage(
	ctx context.Context,
	marker messageMarker,
	processor *ForwardProcessor,
	msg *sarama.ConsumerMessage,
) {
	_ = processor.Handle(ctx, msg)
	marker.MarkMessage(msg, "")
}

type forwardConsumerGroupHandler struct {
	ctx       context.Context
	processor *ForwardProcessor
	ready     chan struct{}
	readyOnce sync.Once
}

func (h *forwardConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	h.readyOnce.Do(func() {
		close(h.ready)
	})
	return nil
}

func (h *forwardConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *forwardConsumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case <-h.ctx.Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			processAndMarkMessage(h.ctx, session, h.processor, msg)
		}
	}
}

func RunForward(ctx context.Context, cfg *config.Config) error {
	return RunForwardWithLogger(ctx, cfg, log.Default())
}

func RunForwardWithLogger(ctx context.Context, cfg *config.Config, logger *log.Logger) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	if logger == nil {
		logger = log.Default()
	}

	sourceBrokers, err := splitBrokers(cfg.Source.BootstrapServers)
	if err != nil {
		return fmt.Errorf("parse source.bootstrap_servers: %w", err)
	}
	targetBrokers, err := splitBrokers(cfg.Target.BootstrapServers)
	if err != nil {
		return fmt.Errorf("parse target.bootstrap_servers: %w", err)
	}
	pipeline, err := buildTransformPipeline(cfg.Transform)
	if err != nil {
		return err
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_6_0_0
	saramaCfg.Consumer.Return.Errors = true
	if cfg.Source.InitialOffset == "earliest" {
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Retry.Max = 0

	producer, err := sarama.NewSyncProducer(targetBrokers, saramaCfg)
	if err != nil {
		return fmt.Errorf("create sync producer: %w", err)
	}
	defer producer.Close()

	consumerGroup, err := sarama.NewConsumerGroup(sourceBrokers, cfg.Source.GroupID, saramaCfg)
	if err != nil {
		return fmt.Errorf("create consumer group: %w", err)
	}
	defer consumerGroup.Close()

	var saver *FileSaver
	if cfg.Source.Save.Enabled {
		saver, err = NewFileSaver(cfg.Source.Save.Path, cfg.Source.Save.MaxSizeBytes)
		if err != nil {
			return fmt.Errorf("create local saver: %w", err)
		}
		defer saver.Close()
	}

	processor := NewForwardProcessor(
		producer,
		saver,
		cfg.Target.Topic,
		RetryPolicy{
			MaxAttempts: cfg.Retry.MaxAttempts,
			Backoff:     time.Duration(cfg.Retry.BackoffMS) * time.Millisecond,
		},
		pipeline,
		logger,
	)
	handler := &forwardConsumerGroupHandler{
		ctx:       ctx,
		processor: processor,
		ready:     make(chan struct{}),
	}

	go func() {
		for err := range consumerGroup.Errors() {
			logger.Printf("consumer group error: %v", err)
		}
	}()

	for {
		if err := consumerGroup.Consume(ctx, []string{cfg.Source.Topic}, handler); err != nil {
			return fmt.Errorf("consume from source topic: %w", err)
		}
		if ctx.Err() != nil {
			break
		}
	}

	snapshot := processor.Stats()
	logger.Printf(
		"forward summary received=%d forwarded=%d skipped=%d filtered=%d transform_errors=%d saved=%d save_errors=%d",
		snapshot.Received,
		snapshot.Forwarded,
		snapshot.Skipped,
		snapshot.Filtered,
		snapshot.TransformErrors,
		snapshot.Saved,
		snapshot.SaveErrors,
	)

	return nil
}

func splitBrokers(value string) ([]string, error) {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		brokers = append(brokers, trimmed)
	}

	if len(brokers) == 0 {
		return nil, errors.New("brokers are empty")
	}
	return brokers, nil
}
