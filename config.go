package delaygo

import (
	"context"
	"log/slog"
	"time"
)

// NewTickerFunc Ticker 构造函数
type NewTickerFunc func(ctx context.Context, config *Config) (Ticker, error)

// NewStorageFunc Storage 构造函数
type NewStorageFunc func(ctx context.Context, config *Config) (Storage, error)

// Config DelayQueue 配置
// @Description DelayQueue 配置
type Config struct {
	// DefaultTTR 默认 TTR
	// @Description 默认 TTR
	DefaultTTR time.Duration `json:"default_ttr"`
	// MaxDelayJobSize 最大任务大小（字节）
	// @Description 最大任务大小（字节）
	MaxDelayJobSize int `json:"max_delay_job_size"`

	// === Ticker 配置 ===

	// Ticker 自定义 Ticker 实例（优先级高于 NewTickerFunc）
	Ticker Ticker `json:"-"`
	// NewTickerFunc Ticker 构造函数（当 Ticker 为 nil 时使用）
	// 如果都为 nil，则使用默认的 DynamicSleepTicker
	NewTickerFunc NewTickerFunc `json:"-"`

	// === Touch 限制配置 ===

	// MaxTouches 最大 Touch 次数
	// @Description 最大 Touch 次数
	MaxTouches int `json:"max_touches"`
	// MaxTouchDuration 最大延长时间
	// @Description 最大延长时间
	MaxTouchDuration time.Duration `json:"max_touch_duration"`
	// MinTouchInterval 最小 Touch 间隔
	// @Description 最小 Touch 间隔
	MinTouchInterval time.Duration `json:"min_touch_interval"`

	// === Topic 配置 ===

	// MaxTopics 最大 topic 数（0 表示无限制）
	// @Description 最大 topic 数（0 表示无限制）
	MaxTopics int `json:"max_topics"`
	// MaxDelayJobsPerTopic 每个 topic 最大任务数（0 表示无限制）
	// @Description 每个 topic 最大任务数（0 表示无限制）
	MaxDelayJobsPerTopic int `json:"max_delay_jobs_per_topic"`
	// EnableTopicCleanup 启用 Topic 惰性清理（定期清理空 Topic）
	// @Description 启用 Topic 惰性清理（定期清理空 Topic）
	EnableTopicCleanup bool `json:"enable_topic_cleanup"`
	// TopicCleanupInterval Topic 清理间隔（默认 1 小时）
	// @Description Topic 清理间隔（默认 1 小时）
	TopicCleanupInterval time.Duration `json:"topic_cleanup_interval"`

	// === 持久化配置 ===

	// Storage 存储后端实例（优先级高于 NewStorageFunc）
	Storage Storage `json:"-"`
	// NewStorageFunc Storage 构造函数（当 Storage 为 nil 时使用）
	// 如果都为 nil，则使用默认的 MemoryStorage
	NewStorageFunc NewStorageFunc `json:"-"`

	// === 日志配置 ===

	// Logger 日志记录器（可选）
	// 如果为 nil，将使用 slog.Default()
	Logger *slog.Logger `json:"-"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		DefaultTTR:           60 * time.Second,
		MaxDelayJobSize:      64 * 1024, // 64KB
		MaxTouches:           10,
		MaxTouchDuration:     10 * 60 * time.Second, // 10 分钟
		MinTouchInterval:     5 * time.Second,
		MaxTopics:            0,     // 无限制
		MaxDelayJobsPerTopic: 0,     // 无限制
		EnableTopicCleanup:   false, // 默认不启用
		TopicCleanupInterval: 1 * time.Hour,
	}
}
