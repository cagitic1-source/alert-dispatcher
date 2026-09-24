package grpc_handler

import (
	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

func (s *AlertHandler) WatchAlert(
	req *alertv1.WatchAlertRequest,
	stream alertv1.AlertService_WatchAlertServer,
) error {

	err := s.svc.WatchAlert(stream.Context(), req.GetId(), func(a domain.Alert) error {
		return stream.Send(toProtoAlert(a))
	})

	return toGRPCError(err)
}
