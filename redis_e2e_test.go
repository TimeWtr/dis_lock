//go:build e2e

package distributed_lock

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLock_e2e_Lock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    "81.70.197.7:6379",
	})

	client := NewClient(rdb)

	testCases := []struct {
		name       string
		key        string
		val        string
		expiration time.Duration
		retry      RetryStrategy
		before     func(t *testing.T)
		after      func(t *testing.T)
		lock       *Lock
		wantErr    error
	}{
		{
			name:       "locked",
			key:        "key1",
			val:        "12345690",
			expiration: time.Minute,
			retry:      NewNonFixedIntervalStrategy(time.Second, 3),
			before:     func(t *testing.T) {},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				timeout, err := rdb.TTL(ctx, "key1").Result()
				assert.NoError(t, err)
				assert.True(t, timeout > 50*time.Second)
			},
			lock: &Lock{
				key:        "key1",
				id:         "12345690",
				expiration: time.Minute,
			},
		},
		{
			name:       "lock failed, key exist",
			key:        "key1",
			val:        "123456098",
			expiration: time.Minute,
			retry:      NewFixedIntervalStrategy(time.Second, 3),
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				res, err := rdb.Set(ctx, "key1", "123456", time.Minute).Result()
				assert.NoError(t, err)
				assert.Equal(t, "OK", res)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				timeout, err := rdb.TTL(ctx, "key1").Result()
				assert.NoError(t, err)
				assert.True(t, timeout > 50*time.Second)
			},
			lock: &Lock{
				key:        "key1",
				id:         "123456",
				expiration: time.Minute,
			},
			wantErr: ErrMaxRetryFailed,
		},
		{
			name:       "lock deadlineExceeded",
			key:        "key1",
			val:        "123456098",
			expiration: time.Minute,
			retry:      NewFixedIntervalStrategy(time.Second, 15),
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				res, err := rdb.Set(ctx, "key1", "123456", time.Minute).Result()
				assert.NoError(t, err)
				assert.Equal(t, "OK", res)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				timeout, err := rdb.TTL(ctx, "key1").Result()
				assert.NoError(t, err)
				assert.True(t, timeout > 50*time.Second)
			},
			lock: &Lock{
				key:        "key1",
				id:         "123456",
				expiration: time.Minute,
			},
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			tc.before(t)
			lock, err := client.Lock(ctx, tc.key, tc.val, tc.expiration, tc.retry)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				t.Log("抢锁失败")
				return
			}
			assert.Equal(t, tc.lock.key, lock.key)
			assert.Equal(t, tc.lock.id, lock.id)
			tc.after(t)
		})
	}
}

func TestLock_2e2_TryLock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    "81.70.197.7:6379",
	})

	type Info struct {
		Name  string
		Desc  string
		Level int
	}

	info := Info{
		Name:  "test",
		Desc:  "test infod,s,nfds,fsdf,sfs,mfdsmfsdfnsdfs,nfs,fnds,fnds,fndsfns,fnsd,fnsd,fnsdf,dsnf,snfs,fns,fnds,fnds,fndsfns,fdsnfs,nfs,fns,fns",
		Level: 1,
	}

	bs, err := json.Marshal(info)
	assert.NoError(t, err)

	testCases := []struct {
		name       string
		key        string
		id         string
		expiration time.Duration
		before     func(t *testing.T)
		after      func(t *testing.T)
		wantErr    error
		wantLock   *Lock
	}{
		{
			name: "key exist",
			key:  "key1",
			id:   "123456",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				res, err := rdb.Set(ctx, "key1", "value1", time.Second*10).Result()
				assert.NoError(t, err)
				assert.Equal(t, "OK", res)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				// redis-server >= 6.2.0
				//res, err := rdb.GetDel(ctx, "key1").Result()
				//assert.NoError(t, err)
				//assert.Equal(t, res, "value1")
				res, err := rdb.Get(ctx, "key1").Result()
				assert.NoError(t, err)
				assert.Equal(t, res, "value1")
				cnt, err := rdb.Del(ctx, "key1").Result()
				assert.NoError(t, err)
				assert.Equal(t, cnt, int64(1))
			},
			wantErr: ErrPreemptLock,
		},
		{
			name:   "lock success",
			key:    "key1",
			id:     "123456",
			before: func(t *testing.T) {},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				res, err := rdb.Get(ctx, "key1").Result()
				assert.NoError(t, err)
				assert.Equal(t, res, "123456")
				cnt, err := rdb.Del(ctx, "key1").Result()
				assert.NoError(t, err)
				assert.Equal(t, cnt, int64(1))
			},
			wantLock: &Lock{
				key: "key1",
				id:  "123456",
			},
		},

		{
			name:       "lock json string success",
			key:        "key1",
			id:         string(bs),
			expiration: time.Second * 5,
			before:     func(t *testing.T) {},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				res, err := rdb.Get(ctx, "key1").Result()
				assert.NoError(t, err)
				assert.Equal(t, res, string(bs))
				//cnt, err := rdb.Del(ctx, "key1").Result()
				//assert.NoError(t, err)
				//assert.Equal(t, cnt, int64(1))
			},
			wantLock: &Lock{
				key: "key1",
				id:  string(bs),
			},
		},
	}

	client := NewClient(rdb)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			lock, err := client.TryLock(ctx, tc.key, tc.id, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				t.Log("加锁失败")
				tc.after(t)
				return
			}
			assert.Equal(t, tc.key, lock.key)
			assert.Equal(t, tc.id, lock.id)
			tc.after(t)
		})
	}
}

func TestLock_e2e_UnLock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    "81.70.197.7:6379",
	})

	testCases := []struct {
		name    string
		before  func(t *testing.T)
		after   func(t *testing.T)
		lock    *Lock
		wantErr error
	}{
		{
			name:   "lock not exist",
			before: func(t *testing.T) {},
			after:  func(t *testing.T) {},
			lock: &Lock{
				key:     "key1",
				id:      "123456",
				client:  rdb,
				closeCh: make(chan struct{}),
			},
			wantErr: ErrLockNotExist,
		},
		{
			name: "unlock success",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				res, err := rdb.Set(ctx, "key1", "123456", time.Second*10).Result()
				assert.Equal(t, "OK", res)
				assert.NoError(t, err)
			},
			after: func(t *testing.T) {},
			lock: &Lock{
				key:     "key1",
				id:      "123456",
				client:  rdb,
				closeCh: make(chan struct{}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tc.before(t)
			err := tc.lock.UnLock(ctx)
			assert.Equal(t, tc.wantErr, err)
			tc.after(t)
		})
	}
}

func TestLock_e2e_Refresh(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    "81.70.197.7:6379",
	})

	testCases := []struct {
		name       string
		key        string
		id         string
		before     func(t *testing.T)
		after      func(t *testing.T)
		expiration time.Duration
		wantErr    error
	}{
		{
			name:    "refresh error",
			key:     "key1",
			id:      "123456",
			before:  func(t *testing.T) {},
			after:   func(t *testing.T) {},
			wantErr: ErrRefreshFailed,
		},
		{
			name: "refresh success",
			key:  "key1",
			id:   "123456",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				res, err := rdb.Set(ctx, "key1", "123456", 10*time.Second).Result()
				assert.NoError(t, err)
				assert.Equal(t, "OK", res)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				cnt, err := rdb.Del(ctx, "key1").Result()
				assert.NoError(t, err)
				assert.Equal(t, int64(1), cnt)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			lock := &Lock{
				key:        "key1",
				id:         "123456",
				client:     rdb,
				expiration: time.Second * 30,
			}
			err := lock.Refresh(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				t.Logf("续约失败，错误为: %s\n", err.Error())
				return
			}
			tc.after(t)
		})
	}
}
