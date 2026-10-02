-- KEYS[1] = alert:{id}
-- KEYS[2] = alert:{id}:events   канал обновлений статуса
-- ARGV[1] = новый статус

if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
end

redis.call('HSET', KEYS[1], 'status', ARGV[1])
redis.call('PUBLISH', KEYS[2], ARGV[1])
return 1