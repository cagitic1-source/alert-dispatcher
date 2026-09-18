package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/cagitic1-source/alert-dispatcher/internal/alertstream"
	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"github.com/redis/go-redis/v9"
)

func TestProcessMessage_InvalidAlertID(t *testing.T) {
	rdb := &fakeRedis{}
	repo := &fakeRepo{alert: domain.Alert{ID: "alert-123", Message: "test"}}

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]any{"alert_id": 42},
	}

	w := NewWorker(rdb, repo, "test")

	err := w.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatal("failed processMessage")
	}

	if len(rdb.acked) != 1 || rdb.acked[0] != "1-0" {
		t.Errorf("acked = %v, хотел [1-0]", rdb.acked)
	}

	if len(repo.statuses) != 0 {
		t.Errorf("statuses = %v, хотел пусто", repo.statuses)
	}

	if repo.getCalls != 0 {
		t.Errorf("calls = %v, хотел 0", repo.getCalls)
	}

}

func TestProcessMessage_DLQPath(t *testing.T) {
	rdb := &fakeRedis{}
	repo := &fakeRepo{
		alert:    domain.Alert{ID: "alert-123", Message: "test"},
		attempts: 3,
	}

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]any{"alert_id": "alert-123"},
	}

	w := NewWorker(rdb, repo, "test")

	w.deliver = func(alert domain.Alert) error {
		return errors.New("boom")
	}

	err := w.processMessage(context.Background(), msg)

	if err != nil {
		t.Fatalf("processMessage: %v", err)
	}

	if len(rdb.added) != 1 {
		t.Fatalf("added = %v, хотел одно сообщение", rdb.added)
	}
	if rdb.added[0].Stream != alertstream.Dead {
		t.Errorf("stream = %q, хотел %q", rdb.added[0].Stream, alertstream.Dead)
	}
	values, ok := rdb.added[0].Values.(map[string]any)
	if !ok {
		t.Fatalf("Values имеет неожиданный тип %T", rdb.added[0].Values)
	}
	if values["alert_id"] != "alert-123" {
		t.Errorf("alert_id = %v, хотел alert-123", values["alert_id"])
	}

	if len(rdb.acked) != 1 || rdb.acked[0] != "1-0" {
		t.Errorf("acked = %v, хотел [1-0]", rdb.acked)
	}

	if len(repo.statuses) != 1 || repo.statuses[0] != domain.StatusFailed {
		t.Errorf("statuses = %v, хотел [StatusFailed]", repo.statuses)
	}
}

func TestProcessMessage_HappyPath(t *testing.T) {

	rdb := &fakeRedis{}
	repo := &fakeRepo{alert: domain.Alert{ID: "alert-123", Message: "test"}}

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]any{"alert_id": "alert-123"},
	}

	w := NewWorker(rdb, repo, "test")

	err := w.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatal("failed processMessage")
	}

	if err != nil {
		t.Fatalf("processMessage: %v", err)
	}

	if len(rdb.acked) != 1 || rdb.acked[0] != "1-0" {
		t.Errorf("acked = %v, хотел [1-0]", rdb.acked)
	}
	if len(repo.statuses) != 1 || repo.statuses[0] != domain.StatusDelivered {
		t.Errorf("statuses = %v, хотел [StatusDelivered]", repo.statuses)
	}
}
