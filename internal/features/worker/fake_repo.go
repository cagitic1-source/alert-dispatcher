package worker

import (
	"context"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

type fakeRepo struct {
	alert    domain.Alert
	attempts int64
	statuses []domain.AlertStatus
	getCalls int // количество вызово алерта
}

func (r *fakeRepo) GetAlert(ctx context.Context, id string) (domain.Alert, error) {
	r.getCalls++
	return r.alert, nil
}

func (r *fakeRepo) IncrementAttempts(ctx context.Context, id string) (int64, error) {
	return r.attempts, nil
}

func (r *fakeRepo) UpdateStatus(ctx context.Context, id string, status domain.AlertStatus) error {
	r.statuses = append(r.statuses, status)
	return nil
}
