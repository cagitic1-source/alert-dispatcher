package worker

import (
	"context"
)

type fakeDLQ struct {
	sent        []dlqCall
	alreadySent bool // если true, SendOnce вернёт false
}

type dlqCall struct {
	alertID string
	reason  string
}

func (f *fakeDLQ) SendOnce(ctx context.Context, alertID, reason string) (bool, error) {
	f.sent = append(f.sent, dlqCall{alertID: alertID, reason: reason})
	return !f.alreadySent, nil
}
