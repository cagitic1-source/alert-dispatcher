package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"github.com/redis/go-redis/v9"
)

func TestProcessMessage_RetryScheduled(t *testing.T) {
	rdb := &fakeRedis{}
	repo := &fakeRepo{
		alert:    domain.Alert{ID: "alert-123", Message: "test"},
		attempts: 1,
	}
	dlq := &fakeDLQ{}
	retry := &fakeRetry{}

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]any{"alert_id": "alert-123"},
	}

	w := NewWorker(repo, rdb, dlq, retry, "test")
	w.deliver = func(alert domain.Alert) error {
		return errors.New("boom")
	}

	before := time.Now()
	err := w.processMessage(context.Background(), msg)
	after := time.Now()

	if err != nil {
		t.Fatalf("processMessage: %v", err)
	}

	if len(retry.scheduled) != 1 {
		t.Fatalf("scheduled = %v, хотел одну запись", retry.scheduled)
	}
	if retry.scheduled[0].alertID != "alert-123" {
		t.Errorf("alertID = %q, хотел alert-123", retry.scheduled[0].alertID)
	}
	got := retry.scheduled[0].at
	if got.Before(before.Add(baseRetryDelay)) || got.After(after.Add(baseRetryDelay)) {
		t.Errorf("at = %v, хотел now+%v", got, baseRetryDelay)
	}

	if len(rdb.acked) != 1 || rdb.acked[0] != "1-0" {
		t.Errorf("acked = %v, хотел [1-0]", rdb.acked)
	}

	if len(dlq.sent) != 0 {
		t.Errorf("sent = %v, хотел пусто — попытки не исчерпаны", dlq.sent)
	}

	if len(repo.statuses) != 0 {
		t.Errorf("statuses = %v, хотел пусто — статус при ретрае не меняется", repo.statuses)
	}
}

func TestProcessMessage_InvalidAlertID(t *testing.T) {
	repo := &fakeRepo{alert: domain.Alert{ID: "alert-123", Message: "test"}}
	rdb := &fakeRedis{}
	dlq := &fakeDLQ{}
	retry := &fakeRetry{}

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]any{"alert_id": 42},
	}

	w := NewWorker(repo, rdb, dlq, retry, "test")

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
	repo := &fakeRepo{
		alert:    domain.Alert{ID: "alert-123", Message: "test"},
		attempts: 3,
	}
	rdb := &fakeRedis{}
	dlq := &fakeDLQ{}
	retry := &fakeRetry{}

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]any{"alert_id": "alert-123"},
	}

	w := NewWorker(repo, rdb, dlq, retry, "test")

	w.deliver = func(alert domain.Alert) error {
		return errors.New("boom")
	}

	err := w.processMessage(context.Background(), msg)

	if err != nil {
		t.Fatalf("processMessage: %v", err)
	}

	if len(dlq.sent) != 1 {
		t.Fatalf("sent = %v, хотел одну отправку в DLQ", dlq.sent)
	}
	if dlq.sent[0].alertID != "alert-123" {
		t.Errorf("alertID = %q, хотел alert-123", dlq.sent[0].alertID)
	}
	if dlq.sent[0].reason != "boom" {
		t.Errorf("reason = %q, хотел boom", dlq.sent[0].reason)
	}

	if len(rdb.acked) != 1 || rdb.acked[0] != "1-0" {
		t.Errorf("acked = %v, хотел [1-0]", rdb.acked)
	}

	if len(repo.statuses) != 1 || repo.statuses[0] != domain.StatusFailed {
		t.Errorf("statuses = %v, хотел [StatusFailed]", repo.statuses)
	}
}

func TestProcessMessage_HappyPath(t *testing.T) {
	repo := &fakeRepo{alert: domain.Alert{ID: "alert-123", Message: "test"}}
	rdb := &fakeRedis{}
	dlq := &fakeDLQ{}
	retry := &fakeRetry{}

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]any{"alert_id": "alert-123"},
	}

	w := NewWorker(repo, rdb, dlq, retry, "test")

	err := w.processMessage(context.Background(), msg)

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

func TestProcessMessage_DLQAlreadySent(t *testing.T) {
	repo := &fakeRepo{
		alert:    domain.Alert{ID: "alert-123", Message: "test"},
		attempts: 3,
	}
	rdb := &fakeRedis{}
	dlq := &fakeDLQ{alreadySent: true}
	retry := &fakeRetry{}

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]any{"alert_id": "alert-123"},
	}

	w := NewWorker(repo, rdb, dlq, retry, "test")
	w.deliver = func(alert domain.Alert) error {
		return errors.New("boom")
	}

	err := w.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("processMessage: %v", err)
	}

	if len(dlq.sent) != 1 {
		t.Fatalf("sent = %v, хотел одну отправку в DLQ", dlq.sent)
	}

	if dlq.sent[0].alertID != "alert-123" {
		t.Errorf("alertID = %q, хотел alert-123", dlq.sent[0].alertID)
	}

	if dlq.sent[0].reason != "boom" {
		t.Errorf("reason = %q, хотел boom", dlq.sent[0].reason)
	}

	if len(repo.statuses) != 1 || repo.statuses[0] != domain.StatusFailed {
		t.Errorf("statuses = %v, хотел [StatusFailed]", repo.statuses)
	}

	if len(rdb.acked) != 1 || rdb.acked[0] != "1-0" {
		t.Errorf("acked = %v, хотел [1-0]", rdb.acked)
	}

}
