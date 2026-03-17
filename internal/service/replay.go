package service

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"kafka-redirect/internal/config"
	"kafka-redirect/internal/transform"
)

type ReplayStats struct {
	Total           int64
	Sent            int64
	Invalid         int64
	Skipped         int64
	Filtered        int64
	TransformErrors int64
}

func RunReplay(ctx context.Context, cfg *config.Config) error {
	return RunReplayWithLogger(ctx, cfg, log.Default())
}

func RunReplayWithLogger(ctx context.Context, cfg *config.Config, logger *log.Logger) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	if logger == nil {
		logger = log.Default()
	}

	targetBrokers, err := splitBrokers(cfg.Target.BootstrapServers)
	if err != nil {
		return fmt.Errorf("parse target.bootstrap_servers: %w", err)
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_6_0_0
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Retry.Max = 0

	producer, err := sarama.NewSyncProducer(targetBrokers, saramaCfg)
	if err != nil {
		return fmt.Errorf("create replay producer: %w", err)
	}
	defer producer.Close()

	pipeline, err := buildTransformPipeline(cfg.Transform)
	if err != nil {
		return err
	}

	stats, err := runReplayWithProducer(ctx, cfg, producer, pipeline, logger)
	if err != nil {
		return err
	}

	logger.Printf(
		"replay summary total=%d sent=%d invalid=%d skipped=%d filtered=%d transform_errors=%d",
		stats.Total,
		stats.Sent,
		stats.Invalid,
		stats.Skipped,
		stats.Filtered,
		stats.TransformErrors,
	)
	return nil
}

func runReplayWithProducer(
	ctx context.Context,
	cfg *config.Config,
	producer Producer,
	pipeline *transform.Pipeline,
	logger *log.Logger,
) (ReplayStats, error) {
	if cfg == nil || cfg.FileSource == nil || cfg.Target == nil {
		return ReplayStats{}, errors.New("replay config is incomplete")
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}

	file, err := os.Open(cfg.FileSource.File)
	if err != nil {
		return ReplayStats{}, fmt.Errorf("open replay file: %w", err)
	}
	defer file.Close()

	policy := RetryPolicy{
		MaxAttempts: cfg.Retry.MaxAttempts,
		Backoff:     time.Duration(cfg.Retry.BackoffMS) * time.Millisecond,
	}
	interval := time.Duration(cfg.FileSource.Interval) * time.Millisecond

	stats := ReplayStats{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 8*1024*1024)
	lineNo := 0

	for scanner.Scan() {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}

		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		stats.Total++

		var record MessageRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			stats.Invalid++
			logger.Printf("invalid json line=%d err=%v", lineNo, err)
			continue
		}

		sendErr := error(nil)
		if pipeline != nil {
			transformMessage, convertErr := record.ToTransformMessage()
			if convertErr != nil {
				stats.TransformErrors++
				stats.Skipped++
				logger.Printf("transform decode failed line=%d err=%v", lineNo, convertErr)
				continue
			}

			result, applyErr := pipeline.Apply(ctx, transformMessage)
			if applyErr != nil {
				stats.TransformErrors++
				stats.Skipped++
				logger.Printf("transform apply failed line=%d err=%v", lineNo, applyErr)
				continue
			}
			if result.Dropped {
				stats.Filtered++
				continue
			}

			sendErr = SendWithRetry(ctx, policy, func() error {
				msg, messageErr := TransformMessageToProducerMessage(transformMessage, cfg.Target.Topic)
				if messageErr != nil {
					return messageErr
				}
				_, _, produceErr := producer.SendMessage(msg)
				return produceErr
			})
		} else {
			sendErr = SendWithRetry(ctx, policy, func() error {
				msg, convertErr := record.ToProducerMessage(cfg.Target.Topic)
				if convertErr != nil {
					return convertErr
				}
				_, _, produceErr := producer.SendMessage(msg)
				return produceErr
			})
		}
		if sendErr != nil {
			stats.Skipped++
			logger.Printf("skip replay line=%d err=%v", lineNo, sendErr)
		} else {
			stats.Sent++
		}

		if interval > 0 {
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return stats, ctx.Err()
			case <-timer.C:
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return stats, fmt.Errorf("scan replay file: %w", err)
	}
	return stats, nil
}
