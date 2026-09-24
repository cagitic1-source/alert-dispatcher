package test

import (
	"context"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/service"
)

var (
	_ service.AlertRepository = (*fakeRepo)(nil)
	_ service.AlertEvents     = (*fakeEvents)(nil)
	_ service.Subscription    = (*fakeSub)(nil)
)

type fakeRepo struct {
	log    *callLog
	alerts []domain.Alert
	err    error // что вернёт GetAlert
	calls  int   // сколько раз звали GetAlert
}

type fakeEvents struct {
	log *callLog
	sub *fakeSub
	err error // ошибка подписки
}

type fakeSub struct {
	log    *callLog
	events []string // stub: что выдавать по одному
	i      int
	closed bool // spy: был ли Close
}

type callLog struct {
	calls []string
}

func (l *callLog) add(name string) {
	l.calls = append(l.calls, name)
}

func (s *fakeSub) Next(ctx context.Context) (string, error) {
	if s.i < len(s.events) {
		e := s.events[s.i]
		s.i++
		return e, nil
	}
	<-ctx.Done()
	return "", ctx.Err()
}

func (s *fakeSub) Close() error {
	s.closed = true
	return nil
}

func (e *fakeEvents) Subscribe(ctx context.Context, alertID string) (service.Subscription, error) {
	if e.log != nil {
		e.log.add("Subscribe")
	}
	if e.err != nil {
		return nil, e.err
	}
	return e.sub, nil
}

func (r *fakeRepo) GetAlert(ctx context.Context, id string) (domain.Alert, error) {
	if r.log != nil {
		r.log.add("GetAlert")
	}

	r.calls++
	if r.err != nil {
		return domain.Alert{}, r.err
	}
	if len(r.alerts) == 0 {
		return domain.Alert{}, nil
	}
	i := r.calls - 1
	if i >= len(r.alerts) {
		i = len(r.alerts) - 1
	}
	return r.alerts[i], nil
}

func (r *fakeRepo) CreateAlertIdempotent(ctx context.Context, a domain.Alert, idemKey string) (domain.Alert, error) {
	panic("CreateAlertIdempotent не должен вызываться в этих тестах")
}
