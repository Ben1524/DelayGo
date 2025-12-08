// Package delaygo 提供简单高效的延迟队列实现
// 受 beanstalkd 启发，提供 topic、优先级、bury/kick、TTR 等特性
package delaygo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sony/sonyflake"
)

var (
	// ErrNotFound 任务不存在
	ErrNotFound = errors.New("delaygo: delayJob not found")
	// ErrNotReserved 任务未被保留
	ErrNotReserved = errors.New("delaygo: delayJob not reserved")
	// ErrNotBuried 任务未被埋葬
	ErrNotBuried = errors.New("delaygo: delayJob not buried")
	// ErrInvalidDelayState 任务状态无效
	ErrInvalidDelayState = errors.New("delaygo: invalid delayJob state")
	// ErrTimeout 操作超时
	ErrTimeout = errors.New("delaygo: timeout")
	// ErrInvalidTopic topic 名称无效
	ErrInvalidTopic = errors.New("delaygo: invalid topic name")
	// ErrTopicRequired topic 不能为空
	ErrTopicRequired = errors.New("delaygo: topic is required")
	// ErrMaxTopicsReached 达到最大 topic 数量
	ErrMaxTopicsReached = errors.New("delaygo: max topics reached")
	// ErrMaxDelayJobsReached 达到最大任务数量
	ErrMaxDelayJobsReached = errors.New("delaygo: max delayJobs reached")
	// ErrTouchLimitExceeded Touch 次数超限
	ErrTouchLimitExceeded = errors.New("delaygo: touch limit exceeded")
	// ErrInvalidTouchTime Touch 时间无效
	ErrInvalidTouchTime = errors.New("delaygo: invalid touch time")
	// ErrTooManyWaiters 等待队列已满
	ErrTooManyWaiters = errors.New("delaygo: too many waiters")
)

// DelayQueue 延迟队列
// 架构设计：
// - 每个 Topic 独立管理自己的 Ready/Delayed/Reserved/Buried 队列
// - 只存储 DelayJobMeta（轻量级），Body 按需从 Storage 加载
// - Wheel Tick 负责定时通知 Topic 处理到期任务
// - Storage 负责持久化
type DelayQueue struct {
	config Config

	// ID 生成器
	idGenerator *sonyflake.Sonyflake

	// === 管理器 ===
	// Topic 管理
	topicManager *TopicManager
	// Reserve 管理
	reserveMgr *reserveManager
	// Recovery 运行器（通过函数调用，无需保存实例）

	// Ticker 定时器
	ticker Ticker

	// Storage 存储后端
	storage Storage

	// Logger 日志记录器
	logger *slog.Logger

	// 上下文控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 恢复完成通知（用于等待异步恢复）
	recoveryDone chan struct{}
	recoveryOnce sync.Once
}

// New 创建新的 DelayQueue 实例
func New(config Config) (*DelayQueue, error) {
	// 设置默认值
	if config.DefaultTTR == 0 {
		config.DefaultTTR = time.Minute
	}
	if config.MaxDelayJobSize == 0 {
		config.MaxDelayJobSize = 64 * 1024
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 初始化 logger
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	q := &DelayQueue{
		config:       config,
		ctx:          ctx,
		cancel:       cancel,
		logger:       logger,
		reserveMgr:   newReserveManager(ctx),
		recoveryDone: make(chan struct{}),
		idGenerator:  sonyflake.NewSonyflake(sonyflake.Settings{}),
	}

	// 创建 Ticker
	if config.Ticker != nil {
		// 优先使用提供的 Ticker 实例
		q.ticker = config.Ticker
	} else if config.NewTickerFunc != nil {
		// 使用构造函数创建
		ticker, err := config.NewTickerFunc(ctx, &config)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("create ticker: %w", err)
		}
		q.ticker = ticker
	} else {
		// 使用默认的 DynamicSleepTicker
		q.ticker = NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)
	}

	// 创建 Storage（必须提供）
	if config.Storage != nil {
		// 优先使用提供的 Storage 实例
		q.storage = config.Storage
	} else if config.NewStorageFunc != nil {
		// 使用构造函数创建
		storage, err := config.NewStorageFunc(ctx, &config)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("create storage: %w", err)
		}
		q.storage = storage
	} else {
		// 未提供 Storage，使用默认的 MemoryStorage
		q.storage = NewMemoryStorage()
	}

	// 创建管理器
	q.topicManager = newTopicManager(&q.config, q.storage, q.ticker)
	q.topicManager.SetOnJobReady(q.notifyWaiters)
	// reserveMgr 已在上面创建
	// recoveryRunner 按需创建（在 Start 中）

	return q, nil
}

// Start 启动 DelayQueue
// 如果配置了 Storage，会从 Storage 恢复任务
// StartOptions 启动选项
type StartOptions struct {
	// RecoveryCallback 恢复进度回调
	RecoveryCallback func(progress *RecoveryProgress)
}

// Start 启动 DelayQueue（使用异步恢复）
func (q *DelayQueue) Start() error {
	return q.StartWithOptions(StartOptions{})
}

// StartWithOptions 使用选项启动 DelayQueue
func (q *DelayQueue) StartWithOptions(opts StartOptions) error {
	q.logger.Info("starting queue",
		slog.Bool("topic_cleanup_enabled", q.config.EnableTopicCleanup),
	)

	// 从 Storage 恢复（异步模式）
	recoveryRunner := newRecoveryRunner(q.ctx, q.storage)

	// 2. 后台异步恢复任务
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		defer q.recoveryOnce.Do(func() { close(q.recoveryDone) })

		q.logger.Info("starting async delayJob recovery")
		progressCh := recoveryRunner.RecoverAsync()
		for progress := range progressCh {
			// 回调进度
			if opts.RecoveryCallback != nil {
				opts.RecoveryCallback(progress)
			}

			// 记录恢复进度
			if progress.Phase == RecoveryPhaseComplete {
				q.logger.Info("delayJob recovery completed",
					slog.Int("total_delayJobs", progress.TotalDelayJobs),
					slog.Int("loaded_delayJobs", progress.LoadedDelayJobs),
					slog.Int("failed_delayJobs", progress.FailedDelayJobs),
				)
			}

			// 恢复完成，应用结果
			if progress.Phase == RecoveryPhaseComplete && progress.Result != nil {
				if err := q.applyRecoveryDelayJobs(progress.Result); err != nil {
					q.logger.Error("failed to apply recovery delayJobs", slog.Any("error", err))
				}
			}
		}
	}()

	// 启动 Ticker
	q.ticker.Start()
	q.logger.Debug("started ticker")

	// 启动 Reserve 管理器
	q.reserveMgr.start()
	q.logger.Debug("started reserve manager")

	// 启动 Topic 清理（如果启用）
	if q.config.EnableTopicCleanup {
		q.wg.Add(1)
		go q.cleanupLoop()
		q.logger.Debug("started topic cleanup loop")
	}

	q.logger.Info("queue started successfully")
	<-q.recoveryDone // 等待恢复完成
	return nil
}

// WaitForRecovery 等待恢复完成
// timeout: 超时时间，0 表示无限等待
// 返回: 如果超时返回 ErrTimeout，如果队列已关闭返回 context.Canceled
func (q *DelayQueue) WaitForRecovery(timeout time.Duration) error {
	if timeout == 0 {
		// 无限等待
		select {
		case <-q.recoveryDone:
			return nil
		case <-q.ctx.Done():
			return q.ctx.Err()
		}
	}

	// 有超时限制
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-q.recoveryDone:
		return nil
	case <-timer.C:
		return ErrTimeout
	case <-q.ctx.Done():
		return q.ctx.Err()
	}
}

// Stop 停止 DelayQueue
func (q *DelayQueue) Stop() error {
	q.logger.Info("stopping queue")

	// 停止 Ticker
	q.ticker.Stop()
	q.logger.Debug("stopped ticker")

	// 停止 Reserve 管理器
	q.reserveMgr.stop()
	q.logger.Debug("stopped reserve manager")

	// 取消 context，停止所有后台 goroutine
	q.cancel()

	// 等待所有后台任务完成
	q.logger.Debug("waiting for background tasks to finish")
	q.wg.Wait()
	q.logger.Debug("all background tasks finished")

	// 关闭 Storage
	if err := q.storage.Close(); err != nil {
		q.logger.Error("failed to close storage", slog.Any("error", err))
		return err
	}
	q.logger.Debug("closed storage")

	q.logger.Info("queue stopped successfully")
	return nil
}

// === 内部辅助方法 ===

// allocateID 分配新的任务 ID
func (q *DelayQueue) allocateID() uint64 {
	id, err := q.idGenerator.NextID()
	if err != nil {
		// 这里简单处理，实际应用中可能需要更复杂的错误处理
		panic(fmt.Sprintf("failed to generate id: %v", err))
	}
	return id
}

// applyRecovery 应用恢复结果到 DelayQueue
// applyRecoveryDelayJobs 应用恢复的任务（用于异步恢复模式）
// MaxID 已经在快速启动阶段设置，这里只应用任务
func (q *DelayQueue) applyRecoveryDelayJobs(result *RecoveryResult) error {
	return q.topicManager.ApplyRecovery(result)
}

// cleanupLoop 定期清理空 Topic
func (q *DelayQueue) cleanupLoop() {
	defer q.wg.Done()

	interval := q.config.TopicCleanupInterval
	if interval == 0 {
		interval = 1 * time.Hour // 默认 1 小时
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-q.ctx.Done():
			return
		case <-ticker.C:
			cleaned := q.topicManager.CleanupEmptyTopics()
			// 可选：记录日志
			// 如果清理了 topic，可以在这里添加日志
			_ = cleaned
		}
	}
}

// === 写操作 API（委托给 TopicManager）===

// Put 添加任务到队列
// topic: topic 名称，不能为空
// body: 任务数据
// priority: 优先级（数字越小优先级越高）
// delay: 延迟时间
// ttr: 执行超时时间，0 使用默认值
func (q *DelayQueue) Put(topic string, body []byte, priority uint32, delay, ttr time.Duration) (uint64, error) {
	// 1. 验证 topic
	if err := q.topicManager.ValidateTopicName(topic); err != nil {
		q.logger.Warn("invalid topic name",
			slog.String("topic", topic),
			slog.Any("error", err),
		)
		return 0, err
	}

	// 2. 验证任务大小
	if len(body) > q.config.MaxDelayJobSize {
		q.logger.Warn("delayJob size exceeds max size",
			slog.String("topic", topic),
			slog.Int("size", len(body)),
			slog.Int("max_size", q.config.MaxDelayJobSize),
		)
		return 0, fmt.Errorf("delaygo: delayJob size %d exceeds max size %d", len(body), q.config.MaxDelayJobSize)
	}

	// 3. 使用默认 TTR
	if ttr == 0 {
		ttr = q.config.DefaultTTR
	}

	// 4. 分配 ID 并创建 DelayJobMeta
	id := q.allocateID()
	meta := NewDelayJobMeta(id, topic, priority, delay, ttr)

	q.logger.Debug("putting delayJob",
		slog.Uint64("id", id),
		slog.String("topic", topic),
		slog.Uint64("priority", uint64(priority)),
		slog.Duration("delay", delay),
		slog.Duration("ttr", ttr),
		slog.Int("body_size", len(body)),
	)

	// 5. 同步持久化 Body 到 Storage
	// 必须在加入内存队列前完成,否则 Reserve 时可能找不到 body
	// SaveDelayJob 内部通过 batchSaveLoop 实现批量优化
	ctx := context.Background()
	if err := q.storage.SaveDelayJob(ctx, meta, body); err != nil {
		// 保存失败不影响任务执行(内存队列中仍然可用)
		// 但重启后会丢失
		q.logger.Error("failed to save delayJob to storage",
			slog.Uint64("id", id),
			slog.String("topic", topic),
			slog.Any("error", err),
		)
	}

	// 6. 加入内存队列
	needsNotify, err := q.topicManager.Put(topic, meta)
	if err != nil {
		q.logger.Error("failed to put delayJob to topic hub",
			slog.Uint64("id", id),
			slog.String("topic", topic),
			slog.Any("error", err),
		)
		return 0, err
	}

	// 7. 通知等待队列
	if needsNotify {
		q.notifyWaiters(topic)
	}

	return id, nil
}

// Delete 删除任务（必须是已保留状态）
func (q *DelayQueue) Delete(id uint64) error {
	q.logger.Debug("deleting delayJob", slog.Uint64("id", id))
	err := q.topicManager.Delete(id)
	if err != nil {
		q.logger.Error("failed to delete delayJob",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}
	return nil
}

// Release 释放已保留的任务
// id: 任务 ID
// priority: 新的优先级
// delay: 延迟时间
func (q *DelayQueue) Release(id uint64, priority uint32, delay time.Duration) error {
	q.logger.Debug("releasing delayJob",
		slog.Uint64("id", id),
		slog.Uint64("priority", uint64(priority)),
		slog.Duration("delay", delay),
	)

	topicName, needsNotify, err := q.topicManager.Release(id, priority, delay)
	if err != nil {
		q.logger.Error("failed to release delayJob",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}

	if needsNotify {
		q.notifyWaiters(topicName)
	}

	return nil
}

// Bury 埋葬已保留的任务
func (q *DelayQueue) Bury(id uint64, priority uint32) error {
	q.logger.Debug("burying delayJob",
		slog.Uint64("id", id),
		slog.Uint64("priority", uint64(priority)),
	)

	err := q.topicManager.Bury(id, priority)
	if err != nil {
		q.logger.Error("failed to bury delayJob",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}
	return nil
}

// Kick 踢出埋葬的任务
// topic: topic 名称
// bound: 最多踢出的任务数
func (q *DelayQueue) Kick(topic string, bound int) (int, error) {
	if err := q.topicManager.ValidateTopicName(topic); err != nil {
		q.logger.Warn("invalid topic name in kick",
			slog.String("topic", topic),
			slog.Any("error", err),
		)
		return 0, err
	}

	q.logger.Debug("kicking delayJobs",
		slog.String("topic", topic),
		slog.Int("bound", bound),
	)

	kicked, needsNotify, err := q.topicManager.Kick(topic, bound)
	if err != nil {
		q.logger.Error("failed to kick delayJobs",
			slog.String("topic", topic),
			slog.Any("error", err),
		)
		return 0, err
	}

	if needsNotify {
		q.notifyWaiters(topic)
	}

	q.logger.Debug("kicked delayJobs",
		slog.String("topic", topic),
		slog.Int("count", kicked),
	)
	return kicked, nil
}

// KickDelayJob 踢出指定的埋葬任务
func (q *DelayQueue) KickDelayJob(id uint64) error {
	q.logger.Debug("kicking delayJob", slog.Uint64("id", id))

	topicName, err := q.topicManager.KickDelayJob(id)
	if err != nil {
		q.logger.Error("failed to kick delayJob",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}

	q.notifyWaiters(topicName)
	return nil
}

// Touch 延长任务的 TTR
// 支持两种模式：
// - Touch(id) - 重置为原始 TTR
// - Touch(id, duration) - 延长指定时间
func (q *DelayQueue) Touch(id uint64, duration ...time.Duration) error {
	q.logger.Debug("touching delayJob",
		slog.Uint64("id", id),
		slog.Any("duration", duration),
	)

	err := q.topicManager.Touch(id, &q.config, duration...)
	if err != nil {
		q.logger.Error("failed to touch delayJob",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}

	// 通知 Ticker 重新计算
	q.ticker.Wakeup()
	return nil
}

// === 查询操作 API（委托给 QueryManager）===

// Peek 查看任务但不保留
func (q *DelayQueue) Peek(id uint64) (*DelayJob, error) {
	q.topicManager.RLock()
	meta, _ := q.topicManager.FindDelayJob(id)
	q.topicManager.RUnlock()

	if meta == nil {
		return nil, ErrNotFound
	}

	// 从 Storage 加载 Body
	body, err := q.storage.GetDelayJobBody(context.Background(), meta.ID)
	if err != nil {
		return nil, err
	}

	// 返回副本
	return NewDelayJob(meta.Clone(), body, q), nil
}

// PeekReady 查看指定 topic 的下一个就绪任务
func (q *DelayQueue) PeekReady(topicName string) (*DelayJob, error) {
	if err := q.topicManager.ValidateTopicName(topicName); err != nil {
		return nil, err
	}

	q.topicManager.RLock()
	t := q.topicManager.GetTopic(topicName)
	if t == nil {
		q.topicManager.RUnlock()
		return nil, ErrNotFound
	}

	meta := t.peekReady()
	q.topicManager.RUnlock()
	if meta == nil {
		return nil, ErrNotFound
	}

	// 从 Storage 加载 Body
	body, err := q.storage.GetDelayJobBody(context.Background(), meta.ID)
	if err != nil {
		return nil, err
	}

	return NewDelayJob(meta.Clone(), body, q), nil
}

// PeekDelayed 查看指定 topic 的下一个将要就绪的延迟任务
func (q *DelayQueue) PeekDelayed(topicName string) (*DelayJob, error) {
	if err := q.topicManager.ValidateTopicName(topicName); err != nil {
		return nil, err
	}

	q.topicManager.RLock()
	t := q.topicManager.GetTopic(topicName)
	if t == nil {
		q.topicManager.RUnlock()
		return nil, ErrNotFound
	}

	meta := t.peekDelayed()
	q.topicManager.RUnlock()
	if meta == nil {
		return nil, ErrNotFound
	}

	// 从 Storage 加载 Body
	body, err := q.storage.GetDelayJobBody(context.Background(), meta.ID)
	if err != nil {
		return nil, err
	}

	return NewDelayJob(meta.Clone(), body, q), nil
}

// PeekBuried 查看指定 topic 的下一个埋葬任务
func (q *DelayQueue) PeekBuried(topicName string) (*DelayJob, error) {
	if err := q.topicManager.ValidateTopicName(topicName); err != nil {
		return nil, err
	}

	q.topicManager.RLock()
	t := q.topicManager.GetTopic(topicName)
	if t == nil {
		q.topicManager.RUnlock()
		return nil, ErrNotFound
	}

	meta := t.peekBuried()
	q.topicManager.RUnlock()
	if meta == nil {
		return nil, ErrNotFound
	}

	// 从 Storage 加载 Body
	body, err := q.storage.GetDelayJobBody(context.Background(), meta.ID)
	if err != nil {
		return nil, err
	}

	return NewDelayJob(meta.Clone(), body, q), nil
}

// StatsDelayJob 返回任务统计信息
func (q *DelayQueue) StatsDelayJob(id uint64) (*DelayJobMeta, error) {
	q.topicManager.RLock()
	defer q.topicManager.RUnlock()

	meta, _ := q.topicManager.FindDelayJob(id)
	if meta == nil {
		return nil, ErrNotFound
	}

	return meta.Clone(), nil
}

// StatsTopic 返回 topic 统计信息
func (q *DelayQueue) StatsTopic(name string) (*TopicStats, error) {
	if err := q.topicManager.ValidateTopicName(name); err != nil {
		return nil, err
	}

	stats := q.topicManager.TopicStats(name)
	if stats == nil {
		return nil, ErrNotFound
	}

	return stats, nil
}

// Stats 返回整体统计信息
func (q *DelayQueue) Stats() *Stats {
	allStats := q.topicManager.AllTopicStats()

	stats := &Stats{
		Topics: len(allStats),
	}

	for _, topicStats := range allStats {
		stats.TotalDelayJobs += topicStats.TotalDelayJobs
		stats.ReadyDelayJobs += topicStats.ReadyDelayJobs
		stats.DelayedDelayJobs += topicStats.DelayedDelayJobs
		stats.ReservedDelayJobs += topicStats.ReservedDelayJobs
		stats.BuriedDelayJobs += topicStats.BuriedDelayJobs
	}

	return stats
}

// ListTopics 返回所有 topic 列表
func (q *DelayQueue) ListTopics() []string {
	return q.topicManager.ListTopics()
}

// === Reserve 操作 API（委托给 ReserveManager + TopicManager）===

// TryReserve 尝试立即预留任务（实现 ReserveHandler 接口）
func (q *DelayQueue) TryReserve(topics []string) *DelayJobMeta {
	return q.topicManager.TryReserve(topics)
}

// GetStorage 获取 storage（实现 ReserveHandler 接口）
func (q *DelayQueue) GetStorage() Storage {
	return q.storage
}

// GetDelayQueue 获取 DelayQueue 引用（实现 ReserveHandler 接口）
func (q *DelayQueue) GetDelayQueue() *DelayQueue {
	return q
}

// Reserve 从指定 topics 保留一个任务
// topics: topic 名称列表，不能为空
// timeout: 等待超时时间，0 表示立即返回
func (q *DelayQueue) Reserve(topics []string, timeout time.Duration) (*DelayJob, error) {
	// 验证 topics
	if len(topics) == 0 {
		q.logger.Warn("reserve called with empty topics")
		return nil, ErrTopicRequired
	}

	for _, topic := range topics {
		if err := q.topicManager.ValidateTopicName(topic); err != nil {
			q.logger.Warn("invalid topic name in reserve",
				slog.String("topic", topic),
				slog.Any("error", err),
			)
			return nil, err
		}
	}

	q.logger.Debug("reserving delayJob",
		slog.Any("topics", topics),
		slog.Duration("timeout", timeout),
	)

	// 委托给 ReserveManager
	delayJob, err := q.reserveMgr.Reserve(topics, timeout, q)
	if err != nil {
		if err == ErrTimeout {
			q.logger.Debug("reserve timeout",
				slog.Any("topics", topics),
			)
		} else {
			q.logger.Error("reserve failed",
				slog.Any("topics", topics),
				slog.Any("error", err),
			)
		}
		return nil, err
	}

	q.logger.Debug("reserved delayJob",
		slog.Uint64("id", delayJob.Meta.ID),
		slog.String("topic", delayJob.Meta.Topic),
	)
	return delayJob, nil
}

// notifyWaiters 通知等待队列（内部方法，供 Put/Kick 调用）
func (q *DelayQueue) notifyWaiters(topic string) {
	q.reserveMgr.notifyWaiters(topic, q)
}

// === 类型定义 ===

// Stats 整体统计信息
type Stats struct {
	TotalDelayJobs    int // 总任务数
	ReadyDelayJobs    int // 就绪任务数
	DelayedDelayJobs  int // 延迟任务数
	ReservedDelayJobs int // 保留任务数
	BuriedDelayJobs   int // 埋葬任务数
	Topics            int // topic 数量
}

// TopicStats Topic 统计信息
// @Description Topic 统计信息
type TopicStats struct {
	// @Description Topic 名称
	Name string `json:"name"`
	// @Description 就绪任务数
	ReadyDelayJobs int `json:"ready_delay_jobs"`
	// @Description 延迟任务数
	DelayedDelayJobs int `json:"delayed_delay_jobs"`
	// @Description 保留任务数
	ReservedDelayJobs int `json:"reserved_delay_jobs"`
	// @Description 埋葬任务数
	BuriedDelayJobs int `json:"buried_delay_jobs"`
	// @Description 总任务数
	TotalDelayJobs int `json:"total_delay_jobs"`
}

// WaitingStats 等待队列统计信息
// @Description 等待队列统计信息
type WaitingStats struct {
	// @Description Topic 名称
	Topic string `json:"topic"`
	// @Description 等待的 worker 数量
	WaitingWorkers int `json:"waiting_workers"`
}

// StatsWaiting 返回所有 topics 的等待队列统计
func (q *DelayQueue) StatsWaiting() []WaitingStats {
	statsMap := q.reserveMgr.stats()

	stats := make([]WaitingStats, 0, len(statsMap))
	for topic, count := range statsMap {
		stats = append(stats, WaitingStats{
			Topic:          topic,
			WaitingWorkers: count,
		})
	}

	return stats
}
