-- KEYS[1] = dlq:sent:<alert_id>   маркер «уже отправляли»
-- KEYS[2] = alerts:dead           стрим DLQ
-- ARGV[1] = alert_id
-- ARGV[2] = reason
-- ARGV[3] = TTL маркера в секундах


local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[3])
if ok then
    redis.call('XADD', KEYS[2], '*', 'alert_id', ARGV[1], 'reason', ARGV[2])
    return 1
end
return 0