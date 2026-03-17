package service

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSendWithRetrySuccessAfterRetry(t *testing.T) {
	attempt := 0
	err := SendWithRetry(context.Background(), RetryPolicy{
		MaxAttempts: 3,
		Backoff:     0,
	}, func() error {
		attempt++
		if attempt < 2 {
			return errors.New("temporary error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("SendWithRetry() error = %v", err)
	}
	if attempt != 2 {
		t.Fatalf("attempt = %d, want 2", attempt)
	}
}

func TestSendWithRetryContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := SendWithRetry(ctx, RetryPolicy{
		MaxAttempts: 3,
		Backoff:     10 * time.Millisecond,
	}, func() error {
		return errors.New("will not be called")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SendWithRetry() error = %v, want context canceled", err)
	}
}
