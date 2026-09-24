package redis_repo

import (
	"context"
	"errors"

	"github.com/cagitic1-source/alert-dispatcher/internal/features/alerts/service"
	goredis "github.com/redis/go-redis/v9"
)

type redisSubscription struct {
	pubsub *goredis.PubSub
	ch     <-chan *goredis.Message
}

func (r *AlertRepository) Subscribe(ctx context.Context, alertID string) (service.Subscription, error) {
	pubsub := r.rdb.Subscribe(ctx, alertEventsKey(alertID))

	if _, err := pubsub.Receive(ctx); err != nil {
		pubsub.Close()
		return nil, err
	}

	return &redisSubscription{pubsub: pubsub, ch: pubsub.Channel()}, nil
}

func (s *redisSubscription) Next(ctx context.Context) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case msg, ok := <-s.ch:
		if !ok {
			return "", errors.New("subscription closed")
		}
		return msg.Payload, nil
	}
}

func (s *redisSubscription) Close() error {
	return s.pubsub.Close()
}
