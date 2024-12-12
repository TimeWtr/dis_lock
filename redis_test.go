package distributed_lock

import (
	"context"
	"errors"
	mcs "github.com/TimeWtr/dis_lock/mocks"
	"github.com/golang/mock/gomock"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLock_TryLock(t *testing.T) {

	testCases := []struct {
		name       string
		mock       func(ctrl *gomock.Controller) redis.Cmdable
		key        string
		id         string
		expiration time.Duration
		wantLock   *Lock
		wantErr    error
	}{
		{
			name: "set nx context deadline",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mcs.NewMockCmdable(ctrl)
				res := redis.NewBoolResult(false, context.DeadlineExceeded)
				cmd.EXPECT().
					SetNX(context.Background(), "key1", gomock.Any(), time.Minute).
					Return(res)
				return cmd
			},
			id:         "123456666",
			expiration: time.Minute,
			key:        "key1",
			wantLock: &Lock{
				key: "key1",
			},
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "set nx preempt lock error",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mcs.NewMockCmdable(ctrl)
				res := redis.NewBoolResult(false, nil)
				cmd.EXPECT().SetNX(context.Background(), "key1", "123456", time.Minute).
					Return(res)
				return cmd
			},
			key:        "key1",
			id:         "123456",
			expiration: time.Minute,
			wantLock: &Lock{
				key: "key1",
				id:  "123456",
			},
			wantErr: ErrPreemptLock,
		},
		{
			name: "set nx success",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mcs.NewMockCmdable(ctrl)
				res := redis.NewBoolResult(true, nil)
				cmd.EXPECT().SetNX(context.Background(), "key1", "123456", time.Minute).
					Return(res)
				return cmd
			},
			key:        "key1",
			id:         "123456",
			expiration: time.Minute,
			wantLock: &Lock{
				key: "key1",
				id:  "123456",
			},
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			client := NewClient(tc.mock(ctrl))
			lock, err := client.TryLock(context.Background(), tc.key, tc.id, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				t.Log("加锁失败")
				return
			}
			assert.Equal(t, tc.wantLock.key, lock.key)
		})
	}
}

func TestLock_UnLock(t *testing.T) {
	testCases := []struct {
		name       string
		key        string
		id         string
		client     func(ctrl *gomock.Controller) redis.Cmdable
		expiration time.Duration
		wantErr    error
	}{
		{
			name: "unlock deadline error",
			key:  "key1",
			id:   "123456",
			client: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mcs.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetErr(context.DeadlineExceeded)
				cmd.EXPECT().Eval(context.Background(), unlockScript, []string{"key1"}, "123456").
					Return(res)
				return cmd
			},
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "unlock err lock not exist",
			key:  "key1",
			id:   "123456",
			client: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mcs.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetVal(int64(0))
				cmd.EXPECT().Eval(context.Background(), unlockScript, []string{"key1"}, "123456").
					Return(res)
				return cmd
			},
			wantErr: ErrLockNotExist,
		},
		{
			name: "unlock success",
			key:  "key1",
			id:   "123456",
			client: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mcs.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetVal(int64(1))
				cmd.EXPECT().Eval(context.Background(), unlockScript, []string{"key1"}, "123456").
					Return(res)
				return cmd
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			lock := &Lock{
				key:    tc.key,
				id:     tc.id,
				client: tc.client(ctrl),
			}

			err := lock.UnLock(context.Background())
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestLock_Refresh(t *testing.T) {
	testCases := []struct {
		name       string
		key        string
		id         string
		expiration time.Duration
		mock       func(ctrl *gomock.Controller) redis.Cmdable
		wantErr    error
	}{
		{
			name: "context deadlined",
			key:  "key1",
			id:   "123456",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				res := redis.NewCmd(context.Background())
				res.SetErr(context.DeadlineExceeded)
				cmd := mcs.NewMockCmdable(ctrl)
				cmd.EXPECT().Eval(context.Background(), refreshScript, []string{"key1"},
					"123456", float64(60)).Return(res)
				return cmd
			},
			expiration: time.Minute,
			wantErr:    context.DeadlineExceeded,
		},
		{
			name: "context deadlined",
			key:  "key1",
			id:   "123456",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				res := redis.NewCmd(context.Background())
				res.SetVal(int64(0))
				cmd := mcs.NewMockCmdable(ctrl)
				cmd.EXPECT().Eval(context.Background(), refreshScript, []string{"key1"},
					"123456", float64(60)).Return(res)
				return cmd
			},
			expiration: time.Minute,
			wantErr:    ErrRefreshFailed,
		},
		{
			name: "context deadlined",
			key:  "key1",
			id:   "123456",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				res := redis.NewCmd(context.Background())
				res.SetVal(int64(1))
				cmd := mcs.NewMockCmdable(ctrl)
				cmd.EXPECT().Eval(context.Background(), refreshScript, []string{"key1"},
					"123456", float64(60)).Return(res)
				return cmd
			},
			expiration: time.Minute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			lock := &Lock{
				client:     tc.mock(ctrl),
				key:        tc.key,
				id:         tc.id,
				expiration: tc.expiration,
			}
			err := lock.Refresh(context.Background())
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func ExampleLock_Refresh() {
	l := &Lock{
		key:        "key1",
		id:         "123456",
		expiration: time.Second * 10,
	}

	errCh := make(chan error)
	closeCh := make(chan struct{})

	go func() {
		maxCounter := 5
		counter := 0
		retryCh := make(chan struct{}, 1)
		defer close(retryCh)
		// 手动定期续约
		ticker := time.NewTicker(time.Second * 8)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// 开始续约
				if counter >= maxCounter {
					errCh <- ErrMaxRetryFailed
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := l.Refresh(ctx)
				cancel()
				if errors.Is(err, context.DeadlineExceeded) {
					retryCh <- struct{}{}
					continue
				}

				if err != nil {
					errCh <- err
					return
				}

				// 续约成功，重置计数器
				counter = 0
			case <-retryCh:
				if counter >= maxCounter {
					errCh <- ErrMaxRetryFailed
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := l.Refresh(ctx)
				cancel()
				if errors.Is(err, context.DeadlineExceeded) {
					retryCh <- struct{}{}
					continue
				}

				if err != nil {
					errCh <- err
					return
				}

				// 续约成功，重置计数器
				counter = 0
			case <-closeCh:
				// 释放锁
				return
			}
		}
	}()

	// 第一段
	select {
	case <-errCh:
		close(closeCh)
		// 自动续约失败，记录日志
	default:
		// 执行业务逻辑
	}

	// 第二段
	select {
	case <-errCh:
		close(closeCh)
		// 自动续约失败，记录日志
	default:
		// 执行业务逻辑
	}

	// 第三段
	select {
	case <-errCh:
		close(closeCh)
		// 自动续约失败，记录日志
	default:
		// 执行业务逻辑
	}
}

func ExampleLock_AutoRefresh() {
	l := &Lock{
		key:        "key1",
		id:         "123456",
		expiration: time.Second * 10,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errCh := make(chan error)
	go func() {
		err := l.AutoRefresh(ctx, time.Second, time.Second*10, 10)
		if err != nil {
			_ = l.UnLock(ctx)
			errCh <- err
			return
		}
	}()

	// 第一段
	select {
	case <-errCh:
		// 自动续约失败，记录日志
	default:
		// 执行业务逻辑
	}

	// 第二段
	select {
	case <-errCh:
		// 自动续约失败，记录日志
	default:
		// 执行业务逻辑
	}

	// 第三段
	select {
	case <-errCh:
		// 自动续约失败，记录日志
	default:
		// 执行业务逻辑
	}
	// ...
}
