package test

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"testing"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	redis_repo "github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/repository/redis"
	"github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/service"
	grpc_handler "github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/transport/grpc"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// testRedisDB отличается от БД, которую использует
// internal/features/alerts/repository/redis/test: оба пакета делают FlushDB,
// а go test запускает пакеты параллельно.
const testRedisDB = 14

type fakeRepo struct {
	mu     sync.Mutex
	alerts map[string]domain.Alert
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{alerts: make(map[string]domain.Alert)}
}

func (r *fakeRepo) CreateAlert(ctx context.Context, a domain.Alert) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.alerts[a.ID] = a
	return nil
}

func (r *fakeRepo) CreateAlertIdempotent(ctx context.Context, a domain.Alert, idemKey string) (domain.Alert, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.alerts[a.ID] = a
	return a, nil
}

func (r *fakeRepo) GetAlert(ctx context.Context, id string) (domain.Alert, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.alerts[id]
	if !ok {
		return domain.Alert{}, domain.ErrAlertNotFound
	}
	return a, nil
}

func (r *fakeRepo) UpdateStatus(ctx context.Context, id string, status domain.AlertStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	a, ok := r.alerts[id]
	if !ok {
		return domain.ErrAlertNotFound
	}

	a.Status = status
	r.alerts[id] = a
	return nil
}

// startServer поднимает gRPC-сервер поверх bufconn с заданным сервисом
// и возвращает подключённого к нему клиента.
func startServer(t *testing.T, svc *service.AlertService) alertv1.AlertServiceClient {
	t.Helper()

	handler := grpc_handler.NewAlertHandler(svc)

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	alertv1.RegisterAlertServiceServer(srv, handler)

	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("serve: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
		lis.Close()
	})

	return alertv1.NewAlertServiceClient(conn)
}

// events == nil: WatchAlert в этих тестах не используется
// newTestClient — сервис поверх in-memory репозитория, без внешних зависимостей.
func newTestClient(t *testing.T) alertv1.AlertServiceClient {
	t.Helper()
	return startServer(t, service.NewAlertService(newFakeRepo(), nil))
}

// newTestClientAndRepo — как newTestClientWithRedis, но отдаёт ещё и
// репозиторий: тестам WatchAlert нужно менять статус в обход gRPC API.
func newTestClientAndRepo(t *testing.T) (alertv1.AlertServiceClient, *redis_repo.AlertRepository) {
	t.Helper()

	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: testRedisDB})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(func() {
		rdb.FlushDB(context.Background())
		rdb.Close()
	})

	repo := redis_repo.NewAlertRepository(rdb)
	return startServer(t, service.NewAlertService(repo, repo)), repo
}

// newTestClientWithRedis — сервис поверх настоящего Redis.
// Пропускает тест, если Redis недоступен.
func newTestClientWithRedis(t *testing.T) alertv1.AlertServiceClient {
	t.Helper()

	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: testRedisDB})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(func() {
		rdb.FlushDB(context.Background())
		rdb.Close()
	})

	return startServer(t, service.NewAlertService(redis_repo.NewAlertRepository(rdb), nil))
}
