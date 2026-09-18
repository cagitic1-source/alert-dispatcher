package test

import (
	"context"
	"testing"

	alertv1 "github.com/cagitic1-source/alert-dispatcher/gen/alert/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateAlert_Success(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	created, err := client.CreateAlert(ctx, &alertv1.CreateAlertRequest{
		Message:        "temp high",
		Severity:       alertv1.AlertSeverity_ALERT_SEVERITY_HIGH,
		IdempotencyKey: uuid.NewString(),
	})

	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if created.GetId() == "" {
		t.Error("expected non-empty id")
	}

	if created.GetStatus() != alertv1.AlertStatus_ALERT_STATUS_PENDING {
		t.Errorf("status = %v, want PENDING", created.GetStatus())
	}
	got, err := client.GetAlert(ctx, &alertv1.GetAlertRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.GetMessage() != "temp high" {
		t.Errorf("message = %q, want %q", got.GetMessage(), "temp high")
	}
}

func TestCreateAlert_EmptyMessage(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	_, err := client.CreateAlert(ctx, &alertv1.CreateAlertRequest{
		Message:  "",
		Severity: alertv1.AlertSeverity_ALERT_SEVERITY_HIGH,
	})

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestGetAlert_NotFound(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	_, err := client.GetAlert(ctx, &alertv1.GetAlertRequest{Id: "nonexistent-id"})

	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
}
