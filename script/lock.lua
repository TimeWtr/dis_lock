local value = redis.call('get', KEYS[1])
if value == false then
    -- 分布式锁不存在
    return redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2])
elseif value == ARGV[1] then
    -- 上次抢锁成功了
    redis.call('expire', KEYS[1], ARGV[2])
    return "OK"
else
    -- 锁被别人持有
    return ""
end