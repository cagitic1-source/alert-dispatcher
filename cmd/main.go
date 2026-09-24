package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	redis_repo "github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/repository/redis"
	"github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/service"
	grpc_handler "github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/transport/grpc"
	"github.com/cagitic1-source/alert-dispatcher/internal/features/worker"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const shutdownTimeout = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	repo := redis_repo.NewAlertRepository(rdb)
	svc := service.NewAlertService(repo, repo)

	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("redis unavailable: %v", err)
	}

	w := worker.NewWorker(repo, worker.NewRedisStreamClient(rdb), worker.NewDeadLetterStore(rdb), worker.NewRetryQueue(rdb), "worker-1")
	if err := w.EnsureGroup(ctx); err != nil {
		log.Fatalf("ensure group: %v", err)
	}

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		// if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		// 	log.Printf("worker stopped: %v", err)
		// }
	}()

	handler := grpc_handler.NewAlertHandler(svc, ctx)

	srv := grpc.NewServer()
	alertv1.RegisterAlertServiceServer(srv, handler)

	reflection.Register(srv)

	// Serve блокирует, поэтому уводим в горутину.
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(lis)
	}()

	log.Println("gRPC server listening on :50051")

	select {
	case err := <-serveErr:
		log.Fatalf("serve: %v", err)
	case <-ctx.Done():
		log.Println("shutdown signal received")
	}

	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		log.Println("graceful shutdown complete")
	case <-time.After(shutdownTimeout):
		log.Println("shutdown timed out, forcing stop")
		srv.Stop()
	}

	<-workerDone

	if err := rdb.Close(); err != nil {
		log.Printf("redis close: %v", err)
	}

}
