package worker

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

func (w *Worker) claimStale(ctx context.Context) error {
	messages, err := w.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   w.stream,
		Group:    w.group,
		Consumer: w.consumer,
		MinIdle:  claimMinIdle,
		Start:    "0-0",
		Count:    10,
	})

	if err != nil {
		return fmt.Errorf("xautoclaim: %w", err)
	}
	for _, msg := range messages {
		if err := w.processMessage(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("process claimed message %s: %v", msg.ID, err)
		}
	}

	return nil

}
