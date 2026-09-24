package test

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/maintnotifications.(*CircuitBreakerManager).cleanupLoop"),
	)
}

func TestWatchAlert_Stream(t *testing.T) {
	client, repo := newTestClientAndRepo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	created, err := client.CreateAlert(ctx, &alertv1.CreateAlertRequest{
		Message:        "watch me",
		Severity:       alertv1.AlertSeverity_ALERT_SEVERITY_LOW,
		IdempotencyKey: "watch-key-1",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := created.GetId()

	stream, err := client.WatchAlert(ctx, &alertv1.WatchAlertRequest{Id: id})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Первое сообщение приходит только после того, как сервер подписался
	// и прочитал состояние, — дальше публиковать безопасно.
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv first: %v", err)
	}
	if first.GetStatus() != alertv1.AlertStatus_ALERT_STATUS_PENDING {
		t.Fatalf("первое сообщение: status = %v, хотел PENDING", first.GetStatus())
	}

	if err := repo.UpdateStatus(ctx, id, domain.StatusDelivered); err != nil {
		t.Fatalf("update status: %v", err)
	}

	second, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv second: %v", err)
	}
	if second.GetStatus() != alertv1.AlertStatus_ALERT_STATUS_DELIVERED {
		t.Errorf("второе сообщение: status = %v, хотел DELIVERED", second.GetStatus())
	}

	// Терминальный статус закрывает поток.
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("третий Recv: err = %v, хотел io.EOF", err)
	}
}

func TestCreateAlert_Idempotent_Concurrent(t *testing.T) {
	client := newTestClientWithRedis(t)

	const n = 20
	ids := make([]string, n)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			resp, err := client.CreateAlert(context.Background(), &alertv1.CreateAlertRequest{
				Message:        "concurrent",
				Severity:       alertv1.AlertSeverity_ALERT_SEVERITY_HIGH,
				IdempotencyKey: "same-key",
			})
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			ids[i] = resp.GetId()
		}(i)
	}

	close(start)
	wg.Wait()

	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("got different ids: %s vs %s", ids[0], ids[i])
		}
	}
}
