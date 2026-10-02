package service

import (
	"context"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

func (s *AlertService) GetAlert(ctx context.Context, id string) (domain.Alert, error) {
	if id == "" {
		return domain.Alert{}, domain.ErrInvalidInput
	}
	return s.alertRepo.GetAlert(ctx, id)
}
