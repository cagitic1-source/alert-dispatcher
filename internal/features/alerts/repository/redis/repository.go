package redis_repo

import (
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type AlertRepository struct {
	rdb *goredis.Client
}

func NewAlertRepository(rdb *goredis.Client) *AlertRepository {
	return &AlertRepository{rdb: rdb}
}

const (
	idemTTL  = time.Hour
	alertTTL = 24 * time.Hour
)

func alertKey(id string) string {
	return "alert:{" + id + "}"
}

func alertEventsKey(id string) string {
	return "alert:{" + id + "}:events"
}
