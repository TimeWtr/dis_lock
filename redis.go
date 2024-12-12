package distributed_lock

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"
)

//go:embed script/lock.lua
var lockScript string

//go:embed script/unlock.lua
var unlockScript string

//go:embed script/refresh.lua
var refreshScript string

var (
	ErrPreemptLock    = errors.New("抢锁失败")
	ErrLockNotExist   = errors.New("锁不存在")
	ErrRefreshFailed  = errors.New("续约失败")
	ErrMaxRetryFailed = errors.New("超过最大重试次数")
)

type Client struct {
	// redis 客户端
	client redis.Cmdable
}

func NewClient(client redis.Cmdable) *Client {
	cli := &Client{client: client}
	return cli
}

// TryLock 尝试加锁
// @param ctx context.Context 上下文
// @param key string 锁在redis中的key名称
// @param val string 锁的唯一标识，标识是哪一个进程/程序申请的
// @param expiration time.Duration 分布式锁的过期时间
func (c *Client) TryLock(ctx context.Context, key, val string, expiration time.Duration) (*Lock, error) {
	ok, err := c.client.SetNX(ctx, key, val, expiration).Result()
	if err != nil {
		return nil, err
	}

	if !ok {
		// 加锁失败，锁已经被别人抢走
		return nil, ErrPreemptLock
	}

	return &Lock{
		client:     c.client,
		key:        key,
		id:         val,
		expiration: expiration,
		closeCh:    make(chan struct{}),
		once:       sync.Once{},
	}, nil
}

// Lock 申请分布式锁，内部会重试抢锁
func (c *Client) Lock(ctx context.Context, key, val string, expiration time.Duration, retry RetryStrategy) (*Lock, error) {
	var timer *time.Timer
	for {
		// 重试抢锁
		res, err := c.client.Eval(ctx, lockScript, []string{key}, val, expiration.Seconds()).Result()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		if res == "OK" {
			return &Lock{
				client:     c.client,
				key:        key,
				id:         val,
				expiration: expiration,
				closeCh:    make(chan struct{}),
				once:       sync.Once{},
			}, nil
		}

		interval, err := retry.Next()
		if err != nil {
			fmt.Printf("申请重试失败，错误为: %s\n", err.Error())
			return nil, err
		}

		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}

		select {
		case <-timer.C:
			// 重试抢锁
		case <-ctx.Done():
			// 超时
			return nil, ctx.Err()
		}
	}
}

// UUID 生成唯一的ID，用户区分进程/程序，可选
func (c *Client) UUID() string {
	return uuid.New().String()
}

type Lock struct {
	// redis客户端
	client redis.Cmdable
	// 锁的唯一标识，标识是哪一个进程/程序持有的锁
	id string
	// 存储的分布式锁的key
	key string
	// 锁的过期时间
	expiration time.Duration
	// 关闭通道
	closeCh chan struct{}
	// 防止多次关闭
	once sync.Once
}

// 并发不安全
//func (l *Lock) UnLock(ctx context.Context) error {
//	// 判断是否是自己持有的锁
//
//	// 删除锁
//	cnt, er := l.client.Del(ctx, l.key).Result()
//	if er != nil {
//		return er
//	}
//
//	// 锁过期/被人删除了
//	if cnt != 1 {
//		return ErrLockNotExist
//	}
//
//	return nil
//}

// UnLock 并发安全的释放锁
// 释放锁之前需要先判断锁是否存在，锁是否是申请的锁，然后才可以删除
// 防止删除了别人申请的锁
func (l *Lock) UnLock(ctx context.Context) error {
	defer func() {
		l.once.Do(func() {
			close(l.closeCh)
		})
	}()
	// 分布式场景下多条命令不符合原子操作，所以需要用到lua脚本
	res, err := l.client.Eval(ctx, unlockScript, []string{l.key}, l.id).Int64()
	if err != nil {
		return err
	}

	if res != 1 {
		// 分布式锁不存在
		return ErrLockNotExist
	}

	return nil
}

// Refresh 续约分布式锁
// @ctx context.Context 上下文
// 续约多个命令需要考虑原子性，使用lua脚本实现
// 过期时间沿用创建时设置的
func (l *Lock) Refresh(ctx context.Context) error {
	res, err := l.client.Eval(ctx, refreshScript, []string{l.key}, l.id,
		l.expiration.Seconds()).Int64()
	if err != nil {
		return err
	}

	if res != 1 {
		return ErrRefreshFailed
	}

	return nil
}

// AutoRefresh 自动续约，简化业务手动续约处理的难度
// ctx context.Context 上下文
// interval time.Duration 轮询的时间间隔
// expiration time.Duration 分布式锁的单次续约时间
func (l *Lock) AutoRefresh(ctx context.Context, interval, expiration time.Duration, maxRetry ...int) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	retryCh := make(chan struct{}, 1)
	defer func() {
		close(retryCh)
	}()

	maxCounter := -1
	if len(maxRetry) != 0 {
		// 设置了最大重试次数
		maxCounter = maxRetry[0]
	}
	counter := 0

	for {
		select {
		case <-ticker.C:
			// 自动刷新
			// 超时后重试
			if maxCounter > 0 && counter >= maxCounter {
				// 设置了最大次数且重试次数已达到最大次数
				return ErrMaxRetryFailed
			}
			counter++
			cnt, err := l.client.Eval(ctx, refreshScript, []string{l.key}, l.id, expiration.Seconds()).Int64()
			if errors.Is(err, context.DeadlineExceeded) {
				// 网络问题超时
				retryCh <- struct{}{}
				continue
			}

			if err != nil {
				return err
			}

			if cnt != 1 {
				return ErrRefreshFailed
			}
			// 续约成功，重置计数器
			counter = 0
		case <-retryCh:
			// 超时后重试
			if maxCounter > 0 && counter >= maxCounter {
				// 设置了最大次数且重试次数已达到最大次数
				return ErrMaxRetryFailed
			}
			counter++
			cnt, err := l.client.Eval(ctx, refreshScript, []string{l.key}, l.id, expiration.Seconds()).Int64()
			if errors.Is(err, context.DeadlineExceeded) {
				// 网络问题超时
				retryCh <- struct{}{}
				continue
			}

			if err != nil {
				return err
			}

			if cnt != 1 {
				return ErrRefreshFailed
			}

			// 续约成功，重置计数器
			counter = 0
		case <-l.closeCh:
			// 停止续约，释放分布式锁
			return nil
		}
	}
}
