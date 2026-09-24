package grpc_handler

import (
	"context"
	"log"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

func (s *AlertHandler) WatchAlert(
	req *alertv1.WatchAlertRequest,
	stream alertv1.AlertService_WatchAlertServer,
) error {
	ctx, cancel := mergeCancel(stream.Context(), s.shutdown)
	defer cancel()

	go func() {
		<-ctx.Done()
		log.Printf("watch %s: ctx cancelled: %v", req.GetId(), ctx.Err())
	}()

	err := s.svc.WatchAlert(ctx, req.GetId(), func(a domain.Alert) error {
		return stream.Send(toProtoAlert(a))
	})

	log.Printf("watch %s: service returned: %v", req.GetId(), err)
	return toGRPCError(err)
}

func mergeCancel(a, b context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(a)
	stop := context.AfterFunc(b, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}
