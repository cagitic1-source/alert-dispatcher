package worker

import (
	"context"
	"time"
)

type retryCall struct {
	alertID string
	at      time.Time
}

type fakeRetry struct {
	scheduled []retryCall // spy: что запланировали
	due       []string    // stub: что вернёт DueRetries
	removed   []string    // spy: что удалили

	scheduleErr error
	dueErr      error
	removeErr   error
}

func (r *fakeRetry) ScheduleRetry(ctx context.Context, alertID string, at time.Time) error {
	r.scheduled = append(r.scheduled, retryCall{
		alertID: alertID,
		at:      at,
	})

	return r.scheduleErr
}

func (r *fakeRetry) DueRetries(ctx context.Context, now time.Time, limit int64) ([]string, error) {
	if r.dueErr != nil {
		return nil, r.dueErr
	}

	if limit <= 0 {
		return []string{}, nil
	}

	if int64(len(r.due)) <= limit {
		return r.due, nil
	}

	return r.due[:limit], nil
}
func (r *fakeRetry) RemoveRetry(ctx context.Context, alertID string) error {
	r.removed = append(r.removed, alertID)

	return r.removeErr
}
