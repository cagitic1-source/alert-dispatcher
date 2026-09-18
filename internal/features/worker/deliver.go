package worker

import (
	"errors"
	"strings"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

var ErrDeliveryFailed = errors.New("delivery failed")

func deliver(alert domain.Alert) error {
	if strings.Contains(alert.Message, "fail") {
		return ErrDeliveryFailed
	}
	return nil
}
