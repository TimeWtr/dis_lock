package distributed_lock

import "time"

type RetryStrategy interface {
	// Next
	// 第一个返回值为重试的时间间隔
	// 第二个返回值为错误信息
	Next() (time.Duration, error)
}

// FixedIntervalStrategy 固定时间间隔策略
type FixedIntervalStrategy struct {
	// 时间间隔
	interval time.Duration
	// 最大重试次数
	maxRetry int
	// 当前的重试次数
	counter int
}

func NewFixedIntervalStrategy(interval time.Duration,
	maxRetry int) *FixedIntervalStrategy {
	return &FixedIntervalStrategy{
		interval: interval,
		maxRetry: maxRetry,
	}
}

func (s *FixedIntervalStrategy) Next() (time.Duration, error) {
	if s.counter >= s.maxRetry {
		return 0, ErrMaxRetryFailed
	}

	s.counter++
	return s.interval, nil
}

type NonFixedIntervalStrategy struct {
	// 时间间隔
	interval time.Duration
	// 最大重试次数
	maxRetry int
	// 当前次数
	counter int
}

func NewNonFixedIntervalStrategy(interval time.Duration, maxRetry int) *NonFixedIntervalStrategy {
	return &NonFixedIntervalStrategy{
		interval: interval,
		maxRetry: maxRetry,
	}
}

func (s *NonFixedIntervalStrategy) Next() (time.Duration, error) {
	// 超过最大重试次数
	if s.counter >= s.maxRetry {
		return 0, ErrMaxRetryFailed
	}

	s.counter++
	// 每次乘2
	return s.interval << 1, nil
}
