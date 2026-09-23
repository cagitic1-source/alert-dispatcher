package grpc_handler

import (
	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
)

func (s *AlertHandler) WatchAlert(
	req *alertv1.WatchAlertRequest,
	stream alertv1.AlertService_WatchAlertServer,
) error {
	return nil
}
