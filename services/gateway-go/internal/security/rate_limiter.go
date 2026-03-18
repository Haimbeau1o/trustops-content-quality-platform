package security

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type RateLimiter interface {
	Allow(ctx context.Context, key string) (bool, error)
}

type NoopLimiter struct{}

func NewNoopLimiter() *NoopLimiter {
	return &NoopLimiter{}
}

func (l *NoopLimiter) Allow(context.Context, string) (bool, error) {
	return true, nil
}

type inMemoryBucket struct {
	windowStart time.Time
	count       int
}

type InMemoryFixedWindowLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	buckets map[string]inMemoryBucket
}

func NewInMemoryFixedWindowLimiter(limit int, window time.Duration) *InMemoryFixedWindowLimiter {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Minute
	}
	return &InMemoryFixedWindowLimiter{
		limit:   limit,
		window:  window,
		buckets: make(map[string]inMemoryBucket),
	}
}

func (l *InMemoryFixedWindowLimiter) Allow(_ context.Context, key string) (bool, error) {
	if strings.TrimSpace(key) == "" {
		return false, nil
	}

	now := time.Now().UTC()
	windowStart := now.Truncate(l.window)

	l.mu.Lock()
	defer l.mu.Unlock()

	bucket, ok := l.buckets[key]
	if !ok || !bucket.windowStart.Equal(windowStart) {
		bucket = inMemoryBucket{
			windowStart: windowStart,
			count:       0,
		}
	}
	bucket.count++
	l.buckets[key] = bucket
	return bucket.count <= l.limit, nil
}

type RedisFixedWindowLimiter struct {
	client redis.Cmdable
	prefix string
	limit  int
	window time.Duration
}

func NewRedisFixedWindowLimiter(client redis.Cmdable, prefix string, limit int, window time.Duration) *RedisFixedWindowLimiter {
	if prefix == "" {
		prefix = "cq"
	}
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Minute
	}
	return &RedisFixedWindowLimiter{
		client: client,
		prefix: prefix,
		limit:  limit,
		window: window,
	}
}

func (l *RedisFixedWindowLimiter) Allow(ctx context.Context, key string) (bool, error) {
	if strings.TrimSpace(key) == "" {
		return false, nil
	}
	windowStart := time.Now().UTC().Truncate(l.window).Unix()
	redisKey := fmt.Sprintf("%s:ratelimit:%d:%s", l.prefix, windowStart, key)

	count, err := l.client.Incr(ctx, redisKey).Result()
	if err != nil {
		return false, err
	}
	if count == 1 {
		ttlSeconds := int(l.window.Seconds()) + 1
		_, err = l.client.Expire(ctx, redisKey, time.Duration(ttlSeconds)*time.Second).Result()
		if err != nil {
			return false, err
		}
	}
	allowed := count <= int64(l.limit)
	return allowed, nil
}

func BuildLimiterKey(apiKey, path string) string {
	return strings.TrimSpace(apiKey) + ":" + strings.TrimSpace(path)
}

func ParseLimit(input string, fallback int) int {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(trimmed)
	if err != nil {
		return fallback
	}
	return parsed
}
