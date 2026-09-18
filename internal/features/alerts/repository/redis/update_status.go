package redis_repo

import (
	"context"
	_ "embed"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"github.com/redis/go-redis/v9"
)

//go:embed update_status.lua
var updateStatusScriptSrc string

var updateStatusScript = redis.NewScript(updateStatusScriptSrc)

func (r *AlertRepository) UpdateStatus(ctx context.Context, id string, status domain.AlertStatus) error {
	keys := []string{
		"alert:" + id,
	}
	args := []any{string(status)}
	res, err := updateStatusScript.Run(ctx, r.rdb, keys, args...).Int64()
	if err != nil {
		return err
	}

	if res == 0 {
		return domain.ErrAlertNotFound
	}

	return nil
}
