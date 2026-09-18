package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func (w *Worker) Run(ctx context.Context) error {
	lastClaim := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if time.Since(lastClaim) >= claimInterval {
				if err := w.claimStale(ctx); err != nil {
					return err
				}
				lastClaim = time.Now()
			}

			results, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    w.group,
				Consumer: w.consumer,
				Streams:  []string{w.stream, ">"},
				Count:    10,
				Block:    5 * time.Second,
			})

			if errors.Is(err, redis.Nil) {
				continue
			}
			if err != nil {
				return fmt.Errorf("xreadgroup: %w", err)
			}

			for _, stream := range results {
				for _, msg := range stream.Messages {
					if err := w.processMessage(ctx, msg); err != nil {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						log.Printf("process message %s: %v", msg.ID, err)
					}
				}
			}

		}
	}
}
