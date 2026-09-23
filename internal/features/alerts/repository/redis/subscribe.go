package redis_repo

import (
	"context"

	"github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/service"
	goredis "github.com/redis/go-redis/v9"
)

type redisSubscription struct {
	pubsub *goredis.PubSub
}

func (r *AlertRepository) Subscribe(ctx context.Context, alertID string) (service.Subscription, error) {
	pubsub := r.rdb.Subscribe(ctx, alertEventsKey(alertID))

	if _, err := pubsub.Receive(ctx); err != nil {
		pubsub.Close()
		return nil, err
	}

	return &redisSubscription{pubsub: pubsub}, nil
}

func (s *redisSubscription) Next(ctx context.Context) (string, error) {
	msg, err := s.pubsub.ReceiveMessage(ctx)
	if err != nil {
		return "", err
	}
	return msg.Payload, nil
}

func (s *redisSubscription) Close() error {
	return s.pubsub.Close()
}
