package test

import (
	"context"
	"sync"
	"testing"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/maintnotifications.(*CircuitBreakerManager).cleanupLoop"),
	)
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
