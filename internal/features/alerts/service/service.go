package service

import (
	"context"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

type AlertService struct {
	alertRepo AlertRepository
}

func NewAlertService(alertRepo AlertRepository) *AlertService {
	return &AlertService{
		alertRepo: alertRepo,
	}
}

type AlertRepository interface {
	CreateAlertIdempotent(ctx context.Context, a domain.Alert, idemKey string) (domain.Alert, error)
	GetAlert(ctx context.Context, id string) (domain.Alert, error)
}
type AlertEvents interface {
	Subscribe(ctx context.Context, alertID string) (Subscription, error)
}

type Subscription interface {
	Next(ctx context.Context) (string, error)
	Close() error
}
