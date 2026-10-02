package worker

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/cagitic1-source/alert-dispatcher/internal/alertstream"
	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	rdb      RedisStreamClient
	repo     AlertRepository
	dlq      DeadLetterStore
	retry    RetryQueue
	deliver  func(ctx context.Context, alert domain.Alert) error
	stream   string
	group    string
	consumer string
}

const (
	runBackoffMin     = time.Second
	runBackoffMax     = 30 * time.Second
	retryScanInterval = 5 * time.Second
	retryBatchSize    = 10
	baseRetryDelay    = 30 * time.Second
	maxAttempts       = 3
	markerTTL         = 24 * time.Hour
	trimMaxAge        = 24 * time.Hour
	trimInterval      = time.Minute
	deliverTimeout    = 30 * time.Second
	claimMinIdle      = 2 * time.Minute
	claimInterval     = 10 * time.Second
)

func NewWorker(repo AlertRepository, rdb RedisStreamClient, dlq DeadLetterStore, retry RetryQueue, consumer string) *Worker {
	return &Worker{
		rdb:      rdb,
		repo:     repo,
		retry:    retry,
		dlq:      dlq,
		deliver:  defaultDeliver,
		stream:   alertstream.Pending,
		group:    alertstream.Workers,
		consumer: consumer,
	}
}

func defaultDeliver(_ context.Context, _ domain.Alert) error {
	return nil
}

type DeadLetterStore interface {
	SendOnce(ctx context.Context, alertID string, reason string) (bool, error)
}

type RetryQueue interface {
	ScheduleRetry(ctx context.Context, alertID string, at time.Time) error
	DueRetries(ctx context.Context, now time.Time, limit int64) ([]string, error)
	RemoveRetry(ctx context.Context, alertID string) error
}

type AlertRepository interface {
	GetAlert(ctx context.Context, id string) (domain.Alert, error)
	UpdateStatus(ctx context.Context, id string, status domain.AlertStatus) error
	IncrementAttempts(ctx context.Context, id string) (int64, error)
}

type RedisStreamClient interface {
	XAck(ctx context.Context, stream, group string, ids ...string) error
	XAdd(ctx context.Context, a *redis.XAddArgs) error
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) ([]redis.XStream, error)
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) error
	XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) ([]redis.XMessage, error)
	XTrim(ctx context.Context, stream string, minID string) error
}

// NewRedisStreamClient оборачивает клиент go-redis в RedisStreamClient.
func NewRedisStreamClient(rdb *redis.Client) RedisStreamClient {
	return &redisAdapter{rdb: rdb}
}

type redisAdapter struct{ rdb *redis.Client }

func (a *redisAdapter) XAck(ctx context.Context, stream, group string, ids ...string) error {
	return a.rdb.XAck(ctx, stream, group, ids...).Err()
}

func (a *redisAdapter) XAdd(ctx context.Context, args *redis.XAddArgs) error {
	return a.rdb.XAdd(ctx, args).Err()
}

func (a *redisAdapter) XReadGroup(ctx context.Context, args *redis.XReadGroupArgs) ([]redis.XStream, error) {
	res, err := a.rdb.XReadGroup(ctx, args).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return res, err
}

func (a *redisAdapter) XGroupCreateMkStream(ctx context.Context, stream, group, start string) error {
	return a.rdb.XGroupCreateMkStream(ctx, stream, group, start).Err()
}

func (a *redisAdapter) XTrim(ctx context.Context, stream, minID string) error {
	return a.rdb.XTrimMinID(ctx, stream, minID).Err()
}

func (a *redisAdapter) XAutoClaim(ctx context.Context, args *redis.XAutoClaimArgs) ([]redis.XMessage, error) {
	messages, _, err := a.rdb.XAutoClaim(ctx, args).Result()
	return messages, err
}

// sleepCtx спит d или до отмены ctx. Возвращает false, если ctx отменён.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// retryBackoff даёт растущую паузу между попытками после сбоя Redis.
type retryBackoff struct {
	current time.Duration
}

func (b *retryBackoff) next() time.Duration {
	if b.current == 0 {
		b.current = runBackoffMin
	} else {
		b.current *= 2
		if b.current > runBackoffMax {
			b.current = runBackoffMax
		}
	}
	return b.current
}

func (b *retryBackoff) reset() {
	b.current = 0
}

// handleRunError разбирает ошибку Redis: пересоздаёт consumer group,
// если она пропала, иначе просто логирует.
func (w *Worker) handleRunError(ctx context.Context, op string, err error) {
	if strings.Contains(err.Error(), "NOGROUP") {
		log.Printf("%s: consumer group missing, recreating", op)
		if err := w.EnsureGroup(ctx); err != nil {
			log.Printf("ensure group: %v", err)
		}
		return
	}
	log.Printf("%s: %v", op, err)
}
