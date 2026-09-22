package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/cagitic1-source/alert-dispatcher/internal/domain"
	"github.com/redis/go-redis/v9"
)

func (w *Worker) processMessage(ctx context.Context, msg redis.XMessage) error {
	alertID, ok := msg.Values["alert_id"].(string)
	if !ok || alertID == "" {
		log.Printf("message %s: невалидный alert_id, пропускаем", msg.ID)
		if err := w.rdb.XAck(ctx, w.stream, w.group, msg.ID); err != nil {
			return fmt.Errorf("xack: %w", err)
		}
		return nil
	}

	alert, err := w.repo.GetAlert(ctx, alertID)

	if errors.Is(err, domain.ErrAlertNotFound) {
		log.Printf("alert %s not found, skipping", alertID)
		if err := w.rdb.XAck(ctx, w.stream, w.group, msg.ID); err != nil {
			return fmt.Errorf("xack: %w", err)
		}
		return nil
	}

	if err != nil {
		return fmt.Errorf("get alert: %w", err)
	}

	if deliverErr := w.deliver(alert); deliverErr != nil {
		attempts, incErr := w.repo.IncrementAttempts(ctx, alertID)
		if incErr != nil {
			return fmt.Errorf("increment attempts: %w", incErr)
		}

		if attempts >= maxAttempts {
			sent, dlqErr := w.dlq.SendOnce(ctx, alertID, deliverErr.Error())
			if dlqErr != nil {
				return fmt.Errorf("send to dlq: %w", dlqErr)
			}

			if err := w.repo.UpdateStatus(ctx, alertID, domain.StatusFailed); err != nil {
				return fmt.Errorf("update status: %w", err)
			}

			if err := w.rdb.XAck(ctx, w.stream, w.group, msg.ID); err != nil {
				return fmt.Errorf("xack: %w", err)
			}

			if sent {
				log.Printf("alert %s moved to DLQ after %d attempts", alertID, attempts)
			} else {
				log.Printf("alert %s already in DLQ, finishing status and ack", alertID)
			}
			return nil
		}

		delay := baseRetryDelay * time.Duration(1<<(attempts-1))

		if err := w.retry.ScheduleRetry(ctx, alertID, time.Now().Add(delay)); err != nil {
			return fmt.Errorf("schedule retry: %w", err)
		}
		if err := w.rdb.XAck(ctx, w.stream, w.group, msg.ID); err != nil {
			return fmt.Errorf("xack: %w", err)
		}

		log.Printf("alert %s delivery failed (attempt %d/%d), retry in %s: %v",
			alertID, attempts, maxAttempts, delay, deliverErr)

		return nil
	}

	log.Printf("alert %s delivered: %s", alert.ID, alert.Message)

	if err := w.repo.UpdateStatus(ctx, alertID, domain.StatusDelivered); err != nil {
		return fmt.Errorf("update status: %w", err)
	}

	if err := w.rdb.XAck(ctx, w.stream, w.group, msg.ID); err != nil {
		return fmt.Errorf("xack: %w", err)
	}

	return nil
}
