package grpc_handler

import (
	"context"
	"errors"
	"fmt"
	"testing"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"github.com/cagitic1-source/alert-dispatcher/internal/domain"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{
			name:     "alert not found",
			err:      domain.ErrAlertNotFound,
			wantCode: codes.NotFound,
		},
		{
			name:     "invalid argument",
			err:      domain.ErrInvalidInput,
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "internal error",
			err:      errors.New("boom"),
			wantCode: codes.Internal,
		},
		{
			name:     "wrapped not found",
			err:      fmt.Errorf("repo: %w", domain.ErrAlertNotFound),
			wantCode: codes.NotFound,
		},
		{
			name:     "nil",
			err:      nil,
			wantCode: codes.OK,
		},
		{
			name:     "canceled",
			err:      context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "deadline exceeded",
			err:      context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := status.Code(toGRPCError(tt.err))
			if got != tt.wantCode {
				t.Errorf("got %v, want %v", got, tt.wantCode)
			}
		})
	}

}

func TestSeverityRoundTrip(t *testing.T) {
	all := []alertv1.AlertSeverity{
		alertv1.AlertSeverity_ALERT_SEVERITY_LOW,
		alertv1.AlertSeverity_ALERT_SEVERITY_MEDIUM,
		alertv1.AlertSeverity_ALERT_SEVERITY_HIGH,
		alertv1.AlertSeverity_ALERT_SEVERITY_CRITICAL,
	}

	for v := range alertv1.AlertSeverity_name {
		want := alertv1.AlertSeverity(v)
		if want == alertv1.AlertSeverity_ALERT_SEVERITY_UNSPECIFIED {
			continue
		}
		if got := toProtoSeverity(toDomainSeverity(want)); got != want {
			t.Errorf("round trip %v -> %v", want, got)
		}
	}

	for _, want := range all {
		got := toProtoSeverity(toDomainSeverity(want))
		if got != want {
			t.Errorf("round trip %v -> %v", want, got)
		}
	}
}
