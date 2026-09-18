package service

import (
	"context"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

type AlertRepository interface {
	CreateAlert(ctx context.Context, a domain.Alert) error
	CreateAlertIdempotent(ctx context.Context, a domain.Alert, idemKey string) (domain.Alert, error)
	GetAlert(ctx context.Context, id string) (domain.Alert, error)
}

type AlertService struct {
	alertRepo AlertRepository
}

func NewAlertService(alertRepo AlertRepository) *AlertService {
	return &AlertService{
		alertRepo: alertRepo,
	}
}
