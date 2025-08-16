package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

func NewClient(addr string) *Client {
	return NewClientWithConfig(addr, false)
}

func NewClientWithConfig(addr string, isUDS bool) *Client {
	opts := &redis.Options{
		Addr:            addr,
		Password:        "",
		DB:              0,
		MaxRetries:      2,
		MinRetryBackoff: 8 * time.Millisecond,
		MaxRetryBackoff: 512 * time.Millisecond,
		DialTimeout:     1 * time.Second,
		ReadTimeout:     1 * time.Second,
		WriteTimeout:    3 * time.Second,
		PoolSize:        30,
		MinIdleConns:    5,
		MaxIdleConns:    10,
		ConnMaxIdleTime: 30 * time.Second,
		ConnMaxLifetime: 0,
	}

	if isUDS {
		opts.Network = "unix"
		opts.PoolSize = 20
		opts.DialTimeout = 500 * time.Millisecond
		opts.ReadTimeout = 500 * time.Millisecond
		opts.WriteTimeout = 1 * time.Second
	}

	rdb := redis.NewClient(opts)
	return &Client{rdb: rdb}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	return c.rdb.ZRangeByScore(ctx, key, opt).Result()
}

func (c *Client) HMGet(ctx context.Context, key string, fields ...string) ([]interface{}, error) {
	return c.rdb.HMGet(ctx, key, fields...).Result()
}


func (c *Client) Pipeline() redis.Pipeliner {
	return c.rdb.Pipeline()
}

func (c *Client) Close() error {
	return c.rdb.Close()
}
