package redis

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

func NewClient(addr string) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:            addr,
		Password:        "",
		DB:              0,
		MaxRetries:      2,
		MinRetryBackoff: 8 * time.Millisecond,
		MaxRetryBackoff: 512 * time.Millisecond,
		DialTimeout:     3 * time.Second,
		ReadTimeout:     3 * time.Second,
		WriteTimeout:    3 * time.Second,
		PoolSize:        30,
		MinIdleConns:    5,
		MaxIdleConns:    10,
		ConnMaxIdleTime: 30 * time.Second,
		ConnMaxLifetime: 0,
	})

	return &Client{rdb: rdb}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) LPush(ctx context.Context, key string, values ...interface{}) error {
	return c.rdb.LPush(ctx, key, values...).Err()
}

func (c *Client) Dequeue(ctx context.Context, key string, count int) ([]string, error) {
	if count == 1 {
		val, err := c.rdb.LPop(ctx, key).Result()
		if err == redis.Nil {
			return []string{}, nil
		}
		if err != nil {
			log.Printf("Failed to LPop: %v at %s", err, key)
			return nil, err
		}
		return []string{val}, nil
	}

	result, err := c.rdb.LPopCount(ctx, key, count).Result()
	if err == redis.Nil {
		return []string{}, nil
	}
	return result, err
}

func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	return c.rdb.ZAdd(ctx, key, members...).Err()
}

func (c *Client) ZAddNX(ctx context.Context, key string, members ...redis.Z) error {
	return c.rdb.ZAddNX(ctx, key, members...).Err()
}

func (c *Client) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	return c.rdb.ZRangeByScore(ctx, key, opt).Result()
}

func (c *Client) HIncrBy(ctx context.Context, key, field string, incr int64) error {
	return c.rdb.HIncrBy(ctx, key, field, incr).Err()
}

func (c *Client) HIncrByFloat(ctx context.Context, key, field string, incr float64) error {
	return c.rdb.HIncrByFloat(ctx, key, field, incr).Err()
}

func (c *Client) HMGet(ctx context.Context, key string, fields ...string) ([]interface{}, error) {
	return c.rdb.HMGet(ctx, key, fields...).Result()
}

func (c *Client) Pipeline() redis.Pipeliner {
	return c.rdb.Pipeline()
}

func (c *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	return c.rdb.Publish(ctx, channel, message).Err()
}

func (c *Client) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return c.rdb.Subscribe(ctx, channels...)
}

func (c *Client) Close() error {
	return c.rdb.Close()
}
