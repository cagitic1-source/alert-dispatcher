package test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/service"
)

// Подписка обязана быть установлена ДО чтения состояния: иначе обновление,
// случившееся между чтением и подпиской, потеряется, и клиент зависнет
// с устаревшим статусом.
func TestWatchAlert_SubscribesBeforeRead(t *testing.T) {
	log := &callLog{}
	repo := &fakeRepo{log: log, alerts: []domain.Alert{
		{ID: "alert-123", Status: domain.StatusDelivered},
	}}
	sub := &fakeSub{}
	events := &fakeEvents{log: log, sub: sub}
	svc := service.NewAlertService(repo, events)

	err := svc.WatchAlert(context.Background(), "alert-123", func(domain.Alert) error {
		return nil
	})
	if err != nil {
		t.Fatalf("WatchAlert: %v", err)
	}

	want := []string{"Subscribe", "GetAlert"}
	if !slices.Equal(log.calls, want) {
		t.Errorf("порядок вызовов = %v, хотел %v", log.calls, want)
	}
}

func TestWatchAlert_UpdateInStream(t *testing.T) {
	repo := &fakeRepo{alerts: []domain.Alert{
		{ID: "alert-123", Message: "test", Status: domain.StatusPending},
		{ID: "alert-123", Message: "test", Status: domain.StatusDelivered},
	}}
	sub := &fakeSub{events: []string{"delivered"}}
	events := &fakeEvents{sub: sub}
	svc := service.NewAlertService(repo, events)

	var got []domain.Alert
	err := svc.WatchAlert(context.Background(), "alert-123", func(a domain.Alert) error {
		got = append(got, a)
		return nil
	})

	if err != nil {
		t.Fatalf("WatchAlert: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("send вызван %d раз, хотел 2: %v", len(got), got)
	}
	if got[0].Status != domain.StatusPending {
		t.Errorf("первое сообщение: status = %v, хотел pending", got[0].Status)
	}
	if got[1].Status != domain.StatusDelivered {
		t.Errorf("второе сообщение: status = %v, хотел delivered", got[1].Status)
	}

	if !sub.closed {
		t.Error("подписка не закрыта")
	}
}

// Алерт уже в терминальном статусе: отдаём текущее состояние и закрываемся,
// не заходя в цикл обновлений.
func TestWatchAlert_TerminalStatusImmediately(t *testing.T) {
	repo := &fakeRepo{alerts: []domain.Alert{
		{ID: "alert-123", Message: "test", Status: domain.StatusDelivered},
	}}
	sub := &fakeSub{}
	events := &fakeEvents{sub: sub}
	svc := service.NewAlertService(repo, events)

	var got []domain.Alert
	err := svc.WatchAlert(context.Background(), "alert-123", func(a domain.Alert) error {
		got = append(got, a)
		return nil
	})

	if err != nil {
		t.Fatalf("WatchAlert: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("send вызван %d раз, хотел 1: %v", len(got), got)
	}
	if got[0].Status != domain.StatusDelivered {
		t.Errorf("status = %v, хотел %v", got[0].Status, domain.StatusDelivered)
	}

	if !sub.closed {
		t.Error("подписка не закрыта")
	}

	if repo.calls != 1 {
		t.Errorf("GetAlert вызван %d раз, хотел 1", repo.calls)
	}
}

// Алерта нет: ошибка уходит наверх как есть, send не вызывается,
// но подписка, созданная до чтения состояния, обязана быть закрыта.
func TestWatchAlert_NotFound(t *testing.T) {
	repo := &fakeRepo{err: domain.ErrAlertNotFound}
	sub := &fakeSub{}
	events := &fakeEvents{sub: sub}
	svc := service.NewAlertService(repo, events)

	var got []domain.Alert
	err := svc.WatchAlert(context.Background(), "нет-такого", func(a domain.Alert) error {
		got = append(got, a)
		return nil
	})

	if !errors.Is(err, domain.ErrAlertNotFound) {
		t.Fatalf("err = %v, хотел ErrAlertNotFound", err)
	}

	if len(got) != 0 {
		t.Errorf("send вызван %d раз, хотел 0: %v", len(got), got)
	}

	if !sub.closed {
		t.Error("подписка не закрыта при выходе по NotFound")
	}

	if repo.calls != 1 {
		t.Errorf("GetAlert вызван %d раз, хотел 1", repo.calls)
	}
}
