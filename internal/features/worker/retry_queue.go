package worker

import (
	"context"
	"strconv"
	"time"

	"github.com/cagitic1-source/alert-dispatcher/internal/alertstream"
	"github.com/redis/go-redis/v9"
)

type retryQueue struct{ rdb *redis.Client }

func NewRetryQueue(rdb *redis.Client) RetryQueue {
	return &retryQueue{
		rdb: rdb,
	}
}

func (q *retryQueue) ScheduleRetry(ctx context.Context, alertID string, at time.Time) error {
	return q.rdb.ZAdd(ctx, alertstream.Retry, redis.Z{
		Score:  float64(at.UnixMilli()),
		Member: alertID,
	}).Err()
}

func (q *retryQueue) DueRetries(ctx context.Context, now time.Time, limit int64) ([]string, error) {
	return q.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     alertstream.Retry,
		Start:   "-inf",
		Stop:    strconv.FormatInt(now.UnixMilli(), 10),
		ByScore: true,
		Count:   limit,
	}).Result()
}

func (q *retryQueue) RemoveRetry(ctx context.Context, alertID string) error {
	return q.rdb.ZRem(ctx, alertstream.Retry, alertID).Err()
}
