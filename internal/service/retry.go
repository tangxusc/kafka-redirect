package service

import (
	"context"
	"errors"
	"time"
)

type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

func SendWithRetry(ctx context.Context, policy RetryPolicy, op func() error) error {
	if op == nil {
		return errors.New("retry operation is nil")
	}

	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	if policy.Backoff < 0 {
		policy.Backoff = 0
	}

	var lastErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lastErr = op()
		if lastErr == nil {
			return nil
		}

		if attempt == policy.MaxAttempts {
			break
		}
		if policy.Backoff == 0 {
			continue
		}

		timer := time.NewTimer(policy.Backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}
