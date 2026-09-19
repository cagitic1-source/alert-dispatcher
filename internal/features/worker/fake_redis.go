package worker

import (
	"context"

	"github.com/redis/go-redis/v9"
)

var _ RedisStreamClient = &fakeRedis{}

type fakeRedis struct {
	acked []string
	added []*redis.XAddArgs
}

func (f *fakeRedis) XAck(ctx context.Context, stream, group string, ids ...string) error {
	f.acked = append(f.acked, ids...)
	return nil
}

func (f *fakeRedis) XAdd(ctx context.Context, a *redis.XAddArgs) error {
	f.added = append(f.added, a)
	return nil
}

func (f *fakeRedis) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) ([]redis.XMessage, error) {
	panic("XAutoClaim не должен вызываться в этом тесте")
}
func (f *fakeRedis) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) ([]redis.XStream, error) {
	panic("XReadGroup не должен вызываться в этом тесте")
}

func (f *fakeRedis) XGroupCreateMkStream(ctx context.Context, stream, group, start string) error {
	panic("XGroupCreateMkStream не должен вызываться в этом тесте")
}

func (f *fakeRedis) XTrim(ctx context.Context, stream string, minID string) error {
	panic("XTrim не должен вызываться в этом тесте")
}
