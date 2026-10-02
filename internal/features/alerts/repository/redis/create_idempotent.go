package redis_repo

import (
	"context"
	_ "embed"

	"github.com/cagitic1-source/alert-dispatcher/internal/alertstream"
	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"github.com/redis/go-redis/v9"
)

//go:embed create_idempotent.lua
var createIdempotentScriptSrc string

var createIdempotentScript = redis.NewScript(createIdempotentScriptSrc)

func (r *AlertRepository) CreateAlertIdempotent(
	ctx context.Context,
	a domain.Alert,
	idemKey string,
) (domain.Alert, error) {

	keys := []string{
		"idempotency:" + idemKey,
		alertKey(a.ID),
		alertstream.Pending,
	}
	args := []any{
		a.ID,
		int(idemTTL.Seconds()),
		int(alertTTL.Seconds()),
		"message", a.Message,
		"severity", string(a.Severity),
		"status", string(a.Status),
		"attempts", a.Attempts,
		"created_at", a.CreatedAt,
	}
	res, err := createIdempotentScript.Run(ctx, r.rdb, keys, args...).Slice()
	if err != nil {
		return domain.Alert{}, err
	}
	id, _ := res[0].(string)
	created, _ := res[1].(int64)

	if created == 1 {
		return a, nil
	}

	return r.GetAlert(ctx, id)
}
