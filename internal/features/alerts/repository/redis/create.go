package redis_repo

import (
	"context"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

func (r *AlertRepository) CreateAlert(ctx context.Context, a domain.Alert) error {
	pipe := r.rdb.Pipeline()
	pipe.HSet(ctx, alertKey(a.ID), map[string]any{
		"id":         a.ID,
		"message":    a.Message,
		"severity":   string(a.Severity),
		"status":     string(a.Status),
		"attempts":   a.Attempts,
		"created_at": a.CreatedAt,
	})
	pipe.Expire(ctx, alertKey(a.ID), alertTTL)

	_, err := pipe.Exec(ctx)
	return err
}
