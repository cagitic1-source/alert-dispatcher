-- KEYS[1] = alert:{id}

if redis.call('EXISTS', KEYS[1]) == 0 then
    return -1
end

return redis.call('HINCRBY', KEYS[1], 'attempts', 1)