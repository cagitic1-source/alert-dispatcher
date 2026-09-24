package grpc_handler

import (
	"context"
	"errors"
	"log"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toProtoAlert(a domain.Alert) *alertv1.AlertResponse {
	return &alertv1.AlertResponse{
		Id:        a.ID,
		Message:   a.Message,
		Severity:  toProtoSeverity(a.Severity),
		Status:    toProtoStatus(a.Status),
		Attempts:  a.Attempts,
		CreatedAt: a.CreatedAt,
	}
}

func toDomainSeverity(s alertv1.AlertSeverity) domain.AlertSeverity {
	switch s {
	case alertv1.AlertSeverity_ALERT_SEVERITY_HIGH:
		return domain.SeverityHigh
	case alertv1.AlertSeverity_ALERT_SEVERITY_LOW:
		return domain.SeverityLow
	case alertv1.AlertSeverity_ALERT_SEVERITY_MEDIUM:
		return domain.SeverityMedium
	case alertv1.AlertSeverity_ALERT_SEVERITY_CRITICAL:
		return domain.SeverityCritical
	default:
		return domain.SeverityUnspecified
	}
}

func toDomainStatus(s alertv1.AlertStatus) domain.AlertStatus {
	switch s {
	case alertv1.AlertStatus_ALERT_STATUS_CANCELLED:
		return domain.StatusCanceled
	case alertv1.AlertStatus_ALERT_STATUS_FAILED:
		return domain.StatusFailed
	case alertv1.AlertStatus_ALERT_STATUS_DELIVERED:
		return domain.StatusDelivered
	case alertv1.AlertStatus_ALERT_STATUS_PROCESSING:
		return domain.StatusProcessing
	case alertv1.AlertStatus_ALERT_STATUS_PENDING:
		return domain.StatusPending

	default:
		return domain.StatusUnspecified
	}
}

func toProtoSeverity(s domain.AlertSeverity) alertv1.AlertSeverity {
	switch s {
	case domain.SeverityHigh:
		return alertv1.AlertSeverity_ALERT_SEVERITY_HIGH
	case domain.SeverityLow:
		return alertv1.AlertSeverity_ALERT_SEVERITY_LOW
	case domain.SeverityMedium:
		return alertv1.AlertSeverity_ALERT_SEVERITY_MEDIUM
	case domain.SeverityCritical:
		return alertv1.AlertSeverity_ALERT_SEVERITY_CRITICAL
	default:
		return alertv1.AlertSeverity_ALERT_SEVERITY_UNSPECIFIED
	}
}

func toProtoStatus(s domain.AlertStatus) alertv1.AlertStatus {
	switch s {
	case domain.StatusCanceled:
		return alertv1.AlertStatus_ALERT_STATUS_CANCELLED
	case domain.StatusFailed:
		return alertv1.AlertStatus_ALERT_STATUS_FAILED
	case domain.StatusDelivered:
		return alertv1.AlertStatus_ALERT_STATUS_DELIVERED
	case domain.StatusProcessing:
		return alertv1.AlertStatus_ALERT_STATUS_PROCESSING
	case domain.StatusPending:
		return alertv1.AlertStatus_ALERT_STATUS_PENDING

	default:
		return alertv1.AlertStatus_ALERT_STATUS_UNSPECIFIED
	}
}

func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, domain.ErrAlertNotFound):
		return status.Error(codes.NotFound, "alert not found")
	case errors.Is(err, domain.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, "invalid argument")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "watch timeout")
	default:
		log.Printf("internal error :%v", err)
		return status.Error(codes.Internal, "service temporarily unavailable")
	}
}
