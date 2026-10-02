-- KEYS[1] = idempotency:{key}
-- KEYS[2] = alert:{id}
-- KEYS[3] — новый ключ, имя стрима.
-- '*' — «сгенерируй ID сам» (текущий timestamp + счётчик). Явный ID указывают редко.
-- ARGV[1] = alert_id
-- ARGV[2] = idempotency TTL (сек)
-- ARGV[3] =  alert TTL (сек)
-- ARGV[4..] = пары поле/значение для HSET (message, severity, status, attempts, created_at)

local existing = redis.call('GET', KEYS[1])
if existing then
    return {existing, 0}
end
redis.call('SET', KEYS[1], ARGV[1], 'EX',ARGV[2])
redis.call('HSET', KEYS[2], unpack(ARGV, 4))
redis.call('EXPIRE', KEYS[2], ARGV[3])
redis.call('XADD', KEYS[3], '*', 'alert_id', ARGV[1])

return {ARGV[1], 1}