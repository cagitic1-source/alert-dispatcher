package redis_repo

import (
	"context"
	_ "embed"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"github.com/redis/go-redis/v9"
)

//go:embed increment_attempts.lua
var incrementAttemptsScriptSrc string

var incrementAttemptsScript = redis.NewScript(incrementAttemptsScriptSrc)

func (r *AlertRepository) IncrementAttempts(ctx context.Context, id string) (int64, error) {
	keys := []string{alertKey(id)}

	result, err := incrementAttemptsScript.Run(ctx, r.rdb, keys).Int64()
	if err != nil {
		return 0, err
	}

	if result == -1 {
		return 0, domain.ErrAlertNotFound
	}
	return result, nil
}
