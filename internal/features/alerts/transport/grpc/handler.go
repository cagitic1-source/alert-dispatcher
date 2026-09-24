package grpc_handler

import (
	"context"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/service"
)

type AlertHandler struct {
	alertv1.UnimplementedAlertServiceServer
	svc *service.AlertService
	// shutdown отменяется при SIGTERM. Контекст в поле — осознанное
	// исключение: сигнатуру WatchAlert диктует gRPC, параметра там нет.
	shutdown context.Context
}

func NewAlertHandler(svc *service.AlertService, shutdown context.Context) *AlertHandler {
	return &AlertHandler{
		svc:      svc,
		shutdown: shutdown,
	}
}
