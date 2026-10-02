package grpc_handler

import (
	"context"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *AlertHandler) CreateAlert(ctx context.Context, req *alertv1.CreateAlertRequest) (*alertv1.AlertResponse, error) {
	if req.GetMessage() == "" {
		return nil, status.Error(codes.InvalidArgument, "message is required")
	}

	if req.GetSeverity() == alertv1.AlertSeverity_ALERT_SEVERITY_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "severity is required")
	}

	severity := toDomainSeverity(req.GetSeverity())

	alert, err := s.svc.CreateAlert(ctx, req.GetMessage(), severity, req.GetIdempotencyKey())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return toProtoAlert(alert), nil
}
