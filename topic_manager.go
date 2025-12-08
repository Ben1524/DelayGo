package delaygo

import (
	"context"
	"sort"
	"sync"
	"time"
)

// TopicManager 管理所有 topic
// 负责 topic 的创建、查找、任务分配等
type TopicManager struct {
	mu            sync.RWMutex
	topics        map[string]*topic
	topicWrappers map[string]*topicWrapper

	config  *Config
	storage Storage
	ticker  Ticker

	onJobReady func(topic string)
}

// newTopicManager 创建新的 TopicManager
func newTopicManager(config *Config, storage Storage, ticker Ticker) *TopicManager {
	// 如果 storage 为 nil，使用 MemoryStorage（主要用于测试）
	if storage == nil {
		storage = NewMemoryStorage()
	}

	return &TopicManager{
		topics:        make(map[string]*topic),
		topicWrappers: make(map[string]*topicWrapper),
		config:        config,
		storage:       storage,
		ticker:        ticker,
	}
}

// GetOrCreateTopic 获取或创建 topic（调用者必须持有锁）
func (h *TopicManager) GetOrCreateTopic(name string) (*topic, error) {
	if t, ok := h.topics[name]; ok {
		return t, nil
	}

	// 检查最大 topic 数
	if h.config.MaxTopics > 0 && len(h.topics) >= h.config.MaxTopics {
		return nil, ErrMaxTopicsReached
	}

	t := newTopic(name)
	h.topics[name] = t
	return t, nil
}

// GetTopic 获取 topic（不创建，调用者必须持有锁）
func (h *TopicManager) GetTopic(name string) *topic {
	return h.topics[name]
}

// GetOrCreateTopicWrapper 获取或创建 topicWrapper
func (h *TopicManager) GetOrCreateTopicWrapper(name string, t *topic) *topicWrapper {
	if wrapper, ok := h.topicWrappers[name]; ok {
		return wrapper
	}
	wrapper := &topicWrapper{
		topic: t,
		mu:    &h.mu, // 使用 TopicManager 的锁
		notify: func() {
			if h.onJobReady != nil {
				h.onJobReady(name)
			}
		},
	}
	h.topicWrappers[name] = wrapper
	return wrapper
}

// SetOnJobReady 设置任务就绪回调
func (h *TopicManager) SetOnJobReady(fn func(topic string)) {
	h.onJobReady = fn
}

// RegisterToTicker 如果需要则注册到 ticker
func (h *TopicManager) RegisterToTicker(name string, t *topic) {
	if t.NeedsTick() {
		h.mu.Lock()
		wrapper := h.GetOrCreateTopicWrapper(name, t)
		h.mu.Unlock()

		// 在锁外注册，避免死锁（Ticker.Register 会调用 NextTickTime，后者会尝试获取锁）
		h.ticker.Register(name, wrapper)
	}
}

// UnregisterFromTicker 从 ticker 注销
func (h *TopicManager) UnregisterFromTicker(name string) {
	h.ticker.Unregister(name)
}

// TryReserve 尝试从指定 topics 预留任务
func (h *TopicManager) TryReserve(topics []string) *DelayJobMeta {
	var reservedTopicName string
	var reservedTopic *topic
	var meta *DelayJobMeta

	h.mu.Lock()
	for _, topicName := range topics {
		t, ok := h.topics[topicName]
		if !ok {
			continue
		}

		meta = t.popReady()
		if meta == nil {
			continue
		}

		// 保留任务
		now := time.Now()
		meta.DelayState = DelayStateReserved
		meta.ReservedAt = now
		meta.Reserves++

		t.addReserved(meta)

		// 记录需要注册到 Ticker 的 topic
		reservedTopicName = topicName
		reservedTopic = t
		break
	}
	h.mu.Unlock()

	if meta != nil {
		// 更新到 Storage（移到锁外）
		_ = h.storage.UpdateDelayJobMeta(context.Background(), meta)
	}

	// 在锁外注册到 Ticker，避免死锁
	if reservedTopic != nil {
		h.RegisterToTicker(reservedTopicName, reservedTopic)
	}

	return meta
}

// CleanupEmptyTopics 清理空 Topic
// 返回清理的 topic 数量
func (h *TopicManager) CleanupEmptyTopics() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	cleaned := 0
	for name, t := range h.topics {
		stats := t.stats()
		if stats.TotalDelayJobs == 0 {
			// 从 Ticker 注销
			h.UnregisterFromTicker(name)

			// 删除 topic
			delete(h.topics, name)
			delete(h.topicWrappers, name)
			cleaned++
		}
	}

	return cleaned
}

// FindDelayJob 查找任务（调用者必须持有锁）
func (h *TopicManager) FindDelayJob(id uint64) (*DelayJobMeta, *topic) {
	for _, t := range h.topics {
		// 检查 Ready
		if meta := t.ready.find(id); meta != nil {
			return meta, t
		}
		// 检查 Delayed
		if meta := t.delayed.find(id); meta != nil {
			return meta, t
		}
		// 检查 Reserved
		if meta := t.getReserved(id); meta != nil {
			return meta, t
		}
		// 检查 Buried
		if meta := t.buried.find(id); meta != nil {
			return meta, t
		}
	}
	return nil, nil
}

// ListTopics 列出所有 topic 名称（按字母排序）
func (h *TopicManager) ListTopics() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	names := make([]string, 0, len(h.topics))
	for name := range h.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TopicStats 获取单个 topic 的统计信息
func (h *TopicManager) TopicStats(name string) *TopicStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	t, ok := h.topics[name]
	if !ok {
		return nil
	}
	return t.stats()
}

// AllTopicStats 获取所有 topic 的统计信息
func (h *TopicManager) AllTopicStats() []*TopicStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := make([]*TopicStats, 0, len(h.topics))
	for _, t := range h.topics {
		stats = append(stats, t.stats())
	}
	return stats
}

// TotalDelayJobs 获取总任务数
func (h *TopicManager) TotalDelayJobs() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, t := range h.topics {
		total += t.ready.Len() + t.delayed.Len() + len(t.reserved) + t.buried.Len()
	}
	return total
}

// Lock 获取写锁（供外部操作使用）
func (h *TopicManager) Lock() {
	h.mu.Lock()
}

// Unlock 释放写锁
func (h *TopicManager) Unlock() {
	h.mu.Unlock()
}

// RLock 获取读锁
func (h *TopicManager) RLock() {
	h.mu.RLock()
}

// RUnlock 释放读锁
func (h *TopicManager) RUnlock() {
	h.mu.RUnlock()
}

// GetStorage 获取 storage
func (h *TopicManager) GetStorage() Storage {
	return h.storage
}

// ValidateTopicName 验证 topic 名称
func (h *TopicManager) ValidateTopicName(name string) error {
	if name == "" {
		return ErrTopicRequired
	}

	// 检查长度
	if len(name) > 200 {
		return ErrInvalidTopic
	}

	// 检查字符（字母、数字、下划线、中划线）
	for _, ch := range name {
		isLower := ch >= 'a' && ch <= 'z'
		isUpper := ch >= 'A' && ch <= 'Z'
		isDigit := ch >= '0' && ch <= '9'
		isSpecial := ch == '_' || ch == '-'
		if !isLower && !isUpper && !isDigit && !isSpecial {
			return ErrInvalidTopic
		}
	}

	return nil
}

// === 写操作 ===

// Put 添加任务到 topic
// 返回 (needsNotify bool, error)
func (h *TopicManager) Put(topicName string, meta *DelayJobMeta) (bool, error) {
	h.mu.Lock()

	// 检查任务是否已存在（防止重复添加，例如 Recovery 和 Put 的竞态）
	// 注意：这里只检查当前 topic，因为 ID 是全局唯一的，理论上不应该出现在其他 topic
	// 但为了安全，如果需要全局检查，性能会有损耗。
	// 鉴于 ID 生成机制和 Recovery 逻辑，我们假设调用者知道自己在做什么。
	// 但为了修复 Recovery 竞态，我们需要检查。
	// 优化：先检查目标 topic，因为最可能在这里。
	if t, ok := h.topics[topicName]; ok {
		if m := t.findDelayJob(meta.ID); m != nil {
			h.mu.Unlock()
			// 已存在，忽略
			return false, nil
		}
	}
	// 如果需要严格全局检查：
	// if m, _ := h.findDelayJobGlobalLocked(meta.ID); m != nil { ... }

	// 获取或创建 topic
	t, err := h.GetOrCreateTopic(topicName)
	if err != nil {
		h.mu.Unlock()
		return false, err
	}

	// 检查 MaxDelayJobsPerTopic 限制
	if h.config.MaxDelayJobsPerTopic > 0 {
		topicStats := t.stats()
		if topicStats.TotalDelayJobs >= h.config.MaxDelayJobsPerTopic {
			h.mu.Unlock()
			return false, ErrMaxDelayJobsReached
		}
	}

	// 加载到内存队列
	needsNotify := false
	needsWakeup := false
	if meta.ReadyAt.IsZero() || time.Now().After(meta.ReadyAt) {
		// Ready 任务
		meta.DelayState = DelayStateReady
		t.pushReady(meta)
		needsNotify = true
	} else {
		// Delayed 任务
		meta.DelayState = DelayStateDelayed
		t.pushDelayed(meta)
		needsWakeup = true // 需要唤醒 ticker
	}

	h.mu.Unlock()

	// 注册到 Ticker（在锁外，避免死锁）
	h.RegisterToTicker(topicName, t)

	// 唤醒 Ticker（在锁外，避免死锁）
	if needsWakeup {
		h.ticker.Wakeup()
	}

	// 注意：不在这里持久化，由 DelayQueue.Put() 的异步 worker 负责持久化
	// 这样可以避免重复持久化，并且可以批量写入提高性能

	return needsNotify, nil
}

// Delete 删除任务（必须是已保留状态）
func (h *TopicManager) Delete(id uint64) error {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.FindDelayJob(id)
	if meta == nil {
		h.mu.Unlock()
		return ErrNotFound
	}

	// 只能删除 Reserved 状态的任务
	if meta.DelayState != DelayStateReserved {
		h.mu.Unlock()
		return ErrInvalidDelayState
	}

	topicName := meta.Topic

	// 从 topic 移除
	topic.removeReserved(id)

	// 如果 topic 不再需要 tick，取消注册
	if !topic.NeedsTick() {
		h.UnregisterFromTicker(topicName)
	}

	h.mu.Unlock()

	// 从 Storage 删除（移到锁外）
	if err := h.storage.DeleteDelayJob(context.Background(), id); err != nil {
		// 即使 Storage 删除失败，内存中已经删除了
		// 这里可以记录日志，但不影响返回结果
		_ = err
	}

	// 释放 DelayJobMeta 回对象池
	ReleaseDelayJobMeta(meta)

	return nil
}

// Release 释放已保留的任务
// 返回 (topicName string, needsNotify bool, error)
func (h *TopicManager) Release(id uint64, priority uint32, delay time.Duration) (string, bool, error) {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.FindDelayJob(id)
	if meta == nil {
		h.mu.Unlock()
		return "", false, ErrNotFound
	}

	if meta.DelayState != DelayStateReserved {
		h.mu.Unlock()
		return "", false, ErrNotReserved
	}

	topicName := meta.Topic

	// 从 Reserved 移除
	topic.removeReserved(id)

	// 更新任务
	meta.Priority = priority
	meta.Releases++
	meta.ReservedAt = time.Time{}

	now := time.Now()
	needsNotify := false
	if delay > 0 {
		meta.DelayState = DelayStateDelayed
		meta.ReadyAt = now.Add(delay)
		topic.pushDelayed(meta)
	} else {
		meta.DelayState = DelayStateReady
		meta.ReadyAt = now
		topic.pushReady(meta)
		needsNotify = true
	}

	// 注册到 Ticker（使用 locked 版本，因为已持有锁）
	// h.registerToTickerLocked(topicName, topic) // 移除：避免死锁

	h.mu.Unlock()

	// 在锁外注册到 Ticker
	h.RegisterToTicker(topicName, topic)

	// 更新到 Storage（移到锁外）
	_ = h.storage.UpdateDelayJobMeta(context.Background(), meta)

	return topicName, needsNotify, nil
}

// Bury 埋葬已保留的任务
func (h *TopicManager) Bury(id uint64, priority uint32) error {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.FindDelayJob(id)
	if meta == nil {
		h.mu.Unlock()
		return ErrNotFound
	}

	if meta.DelayState != DelayStateReserved {
		h.mu.Unlock()
		return ErrNotReserved
	}

	topicName := meta.Topic

	// 从 Reserved 移除
	topic.removeReserved(id)

	// 埋葬任务
	meta.DelayState = DelayStateBuried
	meta.Priority = priority
	meta.BuriedAt = time.Now()
	meta.Buries++
	meta.ReservedAt = time.Time{}

	topic.pushBuried(meta)

	// 如果 topic 不再需要 tick，取消注册
	if !topic.NeedsTick() {
		h.UnregisterFromTicker(topicName)
	}

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	_ = h.storage.UpdateDelayJobMeta(context.Background(), meta)

	return nil
}

// Kick 踢出埋葬的任务
// 返回 (kicked int, needsNotify bool, error)
func (h *TopicManager) Kick(topicName string, bound int) (int, bool, error) {
	h.mu.Lock()

	t := h.GetTopic(topicName)
	if t == nil {
		h.mu.Unlock()
		return 0, false, nil
	}

	kicked := 0
	var metasToUpdate []*DelayJobMeta
	for range bound {
		meta := t.popBuried()
		if meta == nil {
			break
		}

		meta.DelayState = DelayStateReady
		meta.BuriedAt = time.Time{}
		meta.Kicks++
		meta.ReadyAt = time.Now()

		t.pushReady(meta)
		metasToUpdate = append(metasToUpdate, meta)

		kicked++
	}
	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	for _, meta := range metasToUpdate {
		_ = h.storage.UpdateDelayJobMeta(context.Background(), meta)
	}

	needsNotify := kicked > 0
	return kicked, needsNotify, nil
}

// KickDelayJob 踢出指定的埋葬任务
// 返回 (topicName string, error)
func (h *TopicManager) KickDelayJob(id uint64) (string, error) {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.FindDelayJob(id)
	if meta == nil {
		h.mu.Unlock()
		return "", ErrNotFound
	}

	if meta.DelayState != DelayStateBuried {
		h.mu.Unlock()
		return "", ErrNotBuried
	}

	topicName := topic.name

	topic.removeBuried(id)

	meta.DelayState = DelayStateReady
	meta.BuriedAt = time.Time{}
	meta.Kicks++
	meta.ReadyAt = time.Now()

	topic.pushReady(meta)

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	_ = h.storage.UpdateDelayJobMeta(context.Background(), meta)

	return topicName, nil
}

// Touch 延长任务的 TTR
func (h *TopicManager) Touch(id uint64, config *Config, duration ...time.Duration) error {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.FindDelayJob(id)
	if meta == nil {
		h.mu.Unlock()
		return ErrNotFound
	}

	if meta.DelayState != DelayStateReserved {
		h.mu.Unlock()
		return ErrNotReserved
	}

	// 检查 Touch 次数限制
	if config.MaxTouches > 0 && meta.Touches >= config.MaxTouches {
		h.mu.Unlock()
		return ErrTouchLimitExceeded
	}

	now := time.Now()

	// 检查最小 Touch 间隔
	if config.MinTouchInterval > 0 && meta.Touches > 0 {
		// 使用 LastTouchAt 检查间隔
		if now.Sub(meta.LastTouchAt) < config.MinTouchInterval {
			h.mu.Unlock()
			return ErrInvalidTouchTime
		}
	}

	// 计算本次延长的时间
	var extendDuration time.Duration
	if len(duration) > 0 {
		// 模式1：延长指定时间
		extendDuration = duration[0]
	} else {
		// 模式2：重置为原始 TTR
		extendDuration = meta.TTR
	}

	// 检查最大延长时间限制
	if config.MaxTouchDuration > 0 {
		totalExtended := meta.TotalTouchTime + extendDuration
		if totalExtended > config.MaxTouchDuration {
			h.mu.Unlock()
			return ErrTouchLimitExceeded
		}
	}

	// 更新时间和统计
	if len(duration) > 0 {
		// 延长模式：在当前 ReservedAt 基础上延长
		meta.ReservedAt = meta.ReservedAt.Add(extendDuration)
	} else {
		// 重置模式：将 ReservedAt 设为现在
		meta.ReservedAt = now
	}

	meta.Touches++
	meta.LastTouchAt = now
	meta.TotalTouchTime += extendDuration

	// 更新 Reserved 映射中的引用
	topic.addReserved(meta)

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	_ = h.storage.UpdateDelayJobMeta(context.Background(), meta)

	return nil
}

// ApplyRecovery 应用恢复结果
func (h *TopicManager) ApplyRecovery(result *RecoveryResult) error {
	h.mu.Lock()

	// 按 Topic 恢复任务
	skipped := 0
	var topicsToRegister []*topic // 待注册的 topic 列表

	for topicName, delayJobs := range result.TopicDelayJobs {
		// 确保 topic 存在
		t, err := h.GetOrCreateTopic(topicName)
		if err != nil {
			h.mu.Unlock()
			return err
		}

		// 将任务加入对应队列
		for _, meta := range delayJobs {
			// 检查任务是否已存在（防止异步恢复导致的重复）
			if existingMeta, _ := h.FindDelayJob(meta.ID); existingMeta != nil {
				skipped++
				continue
			}

			switch meta.DelayState {
			case DelayStateReady:
				t.pushReady(meta)
			case DelayStateDelayed:
				t.pushDelayed(meta)
			case DelayStateBuried:
				t.pushBuried(meta)
			}
		}

		// 收集需要注册的 topic
		if t.NeedsTick() {
			topicsToRegister = append(topicsToRegister, t)
		}
	}

	h.mu.Unlock()

	// 在锁外注册到 Ticker
	for _, t := range topicsToRegister {
		h.RegisterToTicker(t.name, t)
	}

	// 可选：记录跳过的任务数（用于调试）
	_ = skipped

	return nil
}
