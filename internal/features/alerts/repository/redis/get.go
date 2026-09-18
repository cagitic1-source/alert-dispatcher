package redis_repo

import (
	"context"
	"strconv"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

func (r *AlertRepository) GetAlert(ctx context.Context, id string) (domain.Alert, error) {
	vals, err := r.rdb.HGetAll(ctx, alertKey(id)).Result()
	if err != nil {
		return domain.Alert{}, err
	}
	if len(vals) == 0 {
		return domain.Alert{}, domain.ErrAlertNotFound
	}

	attempts, err := strconv.ParseInt(vals["attempts"], 10, 32)
	if err != nil {
		return domain.Alert{}, domain.ErrInvalidInput
	}

	createdAt, err := strconv.ParseInt(vals["created_at"], 10, 64)
	if err != nil {
		return domain.Alert{}, domain.ErrInvalidInput
	}

	severity := domain.AlertSeverity(vals["severity"])

	status := domain.AlertStatus(vals["status"])

	return domain.Alert{
		ID:        id,
		Message:   vals["message"],
		Severity:  severity,
		Status:    status,
		Attempts:  int32(attempts),
		CreatedAt: createdAt,
	}, nil
}
