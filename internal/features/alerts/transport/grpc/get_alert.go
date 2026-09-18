package grpc_handler

import (
	"context"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *AlertHandler) GetAlert(ctx context.Context, req *alertv1.GetAlertRequest) (*alertv1.AlertResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	alert, err := s.svc.GetAlert(ctx, req.GetId())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoAlert(alert), nil
}
