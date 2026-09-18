package grpc_handler

import (
	"errors"
	"fmt"
	"testing"

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
