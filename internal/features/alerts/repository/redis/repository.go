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

const alertTTL = 5 * time.Second

func alertKey(id string) string {
	return "alert:" + id
}
