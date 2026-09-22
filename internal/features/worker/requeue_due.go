package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func (w *Worker) requeueDue(ctx context.Context) error {
	ids, err := w.retry.DueRetries(ctx, time.Now(), retryBatchSize)
	if err != nil {
		return fmt.Errorf("due retries: %w", err)
	}

	for _, id := range ids {
		if err := w.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: w.stream,
			Values: map[string]any{"alert_id": id},
		}); err != nil {
			return fmt.Errorf("requeue %s: %w", id, err)
		}
		if err := w.retry.RemoveRetry(ctx, id); err != nil {
			return fmt.Errorf("remove retry %s: %w", id, err)
		}
	}
	return nil
}
