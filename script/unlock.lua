-- 1. 检查锁是否存在
-- 2. 检查是不是自己的锁
-- 3. 删除锁
-- KEYS[1] 分布式锁的key
-- ARGV[1] 分布式锁的唯一标识，标识进程/程序
if redis.call("get", KEYS[1]) == ARGV[1] then
    -- 分布式锁存在且是自己的锁
    return redis.call("del", KEYS[1])
else
    -- 别人的锁
    return 0
end