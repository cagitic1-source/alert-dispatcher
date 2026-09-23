package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	redis_repo "github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/repository/redis"
	"github.com/redis/go-redis/v9"
)

func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 15})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}

	t.Cleanup(func() {
		rdb.FlushDB(context.Background())
		rdb.Close()
	})

	return rdb
}

func TestRepository_CreateAndGet(t *testing.T) {
	rdb := newTestRedisClient(t)
	repo := redis_repo.NewAlertRepository(rdb)

	alert := domain.Alert{
		ID:        "test-id-1",
		Message:   "temp high",
		Severity:  domain.SeverityHigh,
		Status:    domain.StatusPending,
		CreatedAt: time.Now().Unix(),
	}

	if _, err := repo.CreateAlertIdempotent(context.Background(), alert, "test-idem-1"); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := repo.GetAlert(context.Background(), "test-id-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Message != alert.Message {
		t.Errorf("message = %q, want %q", got.Message, alert.Message)
	}
}

func TestRepository_Get_NotFound(t *testing.T) {
	rdb := newTestRedisClient(t)
	repo := redis_repo.NewAlertRepository(rdb)

	_, err := repo.GetAlert(context.Background(), "nonexistent")
	if !errors.Is(err, domain.ErrAlertNotFound) {
		t.Fatalf("err = %v, want ErrAlertNotFound", err)
	}
}
