package service

import (
	"context"
	"time"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

const watchTimeout = 10 * time.Minute

type AlertService struct {
	alertRepo AlertRepository
	events    AlertEvents
}

func NewAlertService(alertRepo AlertRepository, events AlertEvents) *AlertService {
	return &AlertService{
		alertRepo: alertRepo,
		events:    events,
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
