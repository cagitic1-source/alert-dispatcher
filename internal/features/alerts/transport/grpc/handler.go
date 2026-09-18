package grpc_handler

import (
	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/service"
)

type AlertHandler struct {
	alertv1.UnimplementedAlertServiceServer
	svc *service.AlertService
}

func NewAlertHandler(svc *service.AlertService) *AlertHandler {
	return &AlertHandler{
		svc: svc,
	}
}
