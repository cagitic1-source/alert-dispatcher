package worker

import (
	"context"
	_ "embed"

	"github.com/cagitic1-source/alert-dispatcher/internal/alertstream"
	"github.com/redis/go-redis/v9"
)

//go:embed dlq_send_once.lua
var dlqSendOnceSrc string

var dlqSendOnceScript = redis.NewScript(dlqSendOnceSrc)

type dlqStore struct{ rdb *redis.Client }

func NewDeadLetterStore(rdb *redis.Client) DeadLetterStore {
	return &dlqStore{rdb: rdb}
}

// -- KEYS[1] = dlq:sent:<alert_id>   маркер «уже отправляли»
// -- KEYS[2] = alerts:dead           стрим DLQ
// -- ARGV[1] = alert_id
// -- ARGV[2] = reason
// -- ARGV[3] = TTL маркера в секундах

func (s *dlqStore) SendOnce(ctx context.Context, alertID string, reason string) (bool, error) {
	keys := []string{
		"dlq:sent:" + alertID,
		alertstream.Dead,
	}
	args := []any{
		alertID,
		reason,
		int(markerTTL.Seconds()),
	}
	res, err := dlqSendOnceScript.Run(ctx, s.rdb, keys, args...).Int64()
	if err != nil {
		return false, err
	}

	if res == 1 {
		return true, nil
	}

	return false, nil
}
