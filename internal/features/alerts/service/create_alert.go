package service

import (
	"context"
	"time"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"github.com/google/uuid"
)

func (s *AlertService) CreateAlert(ctx context.Context, msg string, severity domain.AlertSeverity, idemKey string) (domain.Alert, error) {
	if msg == "" {
		return domain.Alert{}, domain.ErrInvalidInput
	}
	if idemKey == "" {
		return domain.Alert{}, domain.ErrInvalidInput
	}

	if !severity.IsValid() {
		return domain.Alert{}, domain.ErrInvalidInput
	}

	alert := domain.Alert{
		ID:        uuid.NewString(),
		Message:   msg,
		Severity:  severity,
		Status:    domain.StatusPending,
		CreatedAt: time.Now().Unix(),
	}

	return s.alertRepo.CreateAlertIdempotent(ctx, alert, idemKey)
}
