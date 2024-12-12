-- 检查分布式锁是否是自己的
-- 给分布式锁设置过期时间
-- KEYS[1] 分布式锁的key
-- ARGV[1] 分布式锁的ID，标识进程/程序
-- ARGV[2] 分布式锁的续约时间
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], ARGV[2])
else
    return 0
end