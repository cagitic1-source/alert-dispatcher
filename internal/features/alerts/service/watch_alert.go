package service

import (
	"context"
	"fmt"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
)

func (s *AlertService) WatchAlert(
	ctx context.Context,
	id string,
	send func(domain.Alert) error,
) error {
	sub, err := s.events.Subscribe(ctx, id)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer sub.Close()

	alert, err := s.alertRepo.GetAlert(ctx, id)
	if err != nil {
		return err
	}

	if err := send(alert); err != nil {
		return err
	}

	if domain.IsTerminal(alert.Status) {
		return nil
	}
	for {
		_, err := sub.Next(ctx)
		if err != nil {
			return err
		}

		alert, err = s.alertRepo.GetAlert(ctx, id)
		if err != nil {
			return err
		}

		if err := send(alert); err != nil {
			return err
		}

		if domain.IsTerminal(alert.Status) {
			return nil
		}
	}
}
