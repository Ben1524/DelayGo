package delaygo

import (
	"container/heap"
	"time"
)

// topic 表示一个命名队列
// 只存储 DelayJobMeta，不存储 Body
// 注意：topic 的所有方法都不是线程安全的，调用者必须持有外部锁（TopicManager.mu）
type topic struct {
	name     string
	ready    *delayJobMetaHeapByPriority // 就绪任务优先级队列（按 Priority 排序）
	delayed  *delayJobMetaHeapByReadyAt  // 延迟任务优先级队列（按 ReadyAt 排序）
	reserved map[uint64]*DelayJobMeta    // 保留任务映射（ID -> Meta）
	buried   *delayJobMetaHeapByPriority // 埋葬任务优先级队列（按 Priority 排序）
}

// topicWrapper wraps a topic and provides thread-safe access
type topicWrapper struct {
	topic  *topic
	mu     RWLocker // 使用外部锁（TopicManager.mu）
	notify func()   // 通知回调（当有任务变为 Ready 时调用）
}

// RWLocker 读写锁接口
type RWLocker interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

// ProcessTick implements Tickable interface with locking
func (w *topicWrapper) ProcessTick(now time.Time) {
	w.mu.Lock()
	hasReady := w.topic.ProcessTick(now)
	w.mu.Unlock()

	if hasReady && w.notify != nil {
		w.notify()
	}
}

// NextTickTime implements Tickable interface with locking
func (w *topicWrapper) NextTickTime() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.topic.NextTickTime()
}

// NeedsTick implements Tickable interface with locking
func (w *topicWrapper) NeedsTick() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.topic.NeedsTick()
}

// newTopic 创建新的 topic
func newTopic(name string) *topic {
	return &topic{
		name:     name,
		ready:    newDelayJobMetaHeap(),
		delayed:  newDelayJobHeap(),
		reserved: make(map[uint64]*DelayJobMeta),
		buried:   newDelayJobMetaHeap(),
	}
}

// === Ready 队列操作 ===
// 注意：调用者必须持有 DelayQueue.mu

// pushReady 添加就绪任务
func (t *topic) pushReady(meta *DelayJobMeta) {
	heap.Push(t.ready, meta)
}

// popReady 获取并移除优先级最高的就绪任务
func (t *topic) popReady() *DelayJobMeta {
	if t.ready.Len() == 0 {
		return nil
	}
	return heap.Pop(t.ready).(*DelayJobMeta)
}

// peekReady 查看但不移除优先级最高的就绪任务
func (t *topic) peekReady() *DelayJobMeta {
	if t.ready.Len() == 0 {
		return nil
	}
	return t.ready.items[0]
}

// removeReady 从就绪队列移除任务
func (t *topic) removeReady(id uint64) bool {
	return t.ready.remove(id)
}

// === Delayed 队列操作 ===
// 注意：调用者必须持有 DelayQueue.mu

// pushDelayed 添加延迟任务
func (t *topic) pushDelayed(meta *DelayJobMeta) {
	heap.Push(t.delayed, meta)
}

// popDelayed 获取并移除最早到期的延迟任务
func (t *topic) popDelayed() *DelayJobMeta {
	if t.delayed.Len() == 0 {
		return nil
	}
	return heap.Pop(t.delayed).(*DelayJobMeta)
}

// peekDelayed 查看但不移除最早到期的延迟任务
func (t *topic) peekDelayed() *DelayJobMeta {
	if t.delayed.Len() == 0 {
		return nil
	}
	return t.delayed.items[0]
}

// removeDelayed 从延迟队列移除任务
func (t *topic) removeDelayed(id uint64) bool {
	return t.delayed.remove(id)
}

// === Reserved 队列操作 ===
// 注意：调用者必须持有 DelayQueue.mu

// addReserved 添加保留任务
func (t *topic) addReserved(meta *DelayJobMeta) {
	t.reserved[meta.ID] = meta
}

// removeReserved 移除保留任务
func (t *topic) removeReserved(id uint64) *DelayJobMeta {
	meta := t.reserved[id]
	delete(t.reserved, id)
	return meta
}

// getReserved 获取保留任务
func (t *topic) getReserved(id uint64) *DelayJobMeta {
	return t.reserved[id]
}

// === Buried 队列操作 ===
// 注意：调用者必须持有 DelayQueue.mu

// pushBuried 添加埋葬任务
func (t *topic) pushBuried(meta *DelayJobMeta) {
	heap.Push(t.buried, meta)
}

// popBuried 获取并移除优先级最高的埋葬任务
func (t *topic) popBuried() *DelayJobMeta {
	if t.buried.Len() == 0 {
		return nil
	}
	return heap.Pop(t.buried).(*DelayJobMeta)
}

// peekBuried 查看但不移除优先级最高的埋葬任务
func (t *topic) peekBuried() *DelayJobMeta {
	if t.buried.Len() == 0 {
		return nil
	}
	return t.buried.items[0]
}

// removeBuried 从埋葬队列移除任务
func (t *topic) removeBuried(id uint64) bool {
	return t.buried.remove(id)
}

// findDelayJob 查找任务
func (t *topic) findDelayJob(id uint64) *DelayJobMeta {
	if meta := t.ready.find(id); meta != nil {
		return meta
	}
	if meta := t.delayed.find(id); meta != nil {
		return meta
	}
	if meta := t.getReserved(id); meta != nil {
		return meta
	}
	if meta := t.buried.find(id); meta != nil {
		return meta
	}
	return nil
}

// === 统计信息 ===

// stats 返回统计信息
// 注意：调用者必须持有 DelayQueue.mu
func (t *topic) stats() *TopicStats {
	return &TopicStats{
		Name:              t.name,
		ReadyDelayJobs:    t.ready.Len(),
		DelayedDelayJobs:  t.delayed.Len(),
		ReservedDelayJobs: len(t.reserved),
		BuriedDelayJobs:   t.buried.Len(),
		TotalDelayJobs:    t.ready.Len() + t.delayed.Len() + len(t.reserved) + t.buried.Len(),
	}
}

// === 实现 Tickable 接口 ===

// ProcessTick 处理 tick 通知
// 返回是否有新任务变为 Ready
func (t *topic) ProcessTick(now time.Time) bool {
	hasReady := false
	// 处理 Delayed → Ready
	if t.processDelayed(now) {
		hasReady = true
	}

	// 处理 Reserved 超时 → Ready
	if t.processReservedTimeout(now) {
		hasReady = true
	}
	return hasReady
}

// NextTickTime 返回下一个需要 tick 的时间
// 注意：调用者必须持有 DelayQueue.mu（通过 topicWrapper）
func (t *topic) NextTickTime() time.Time {
	var nextTime time.Time
	hasNext := false

	// 检查 Delayed 队列
	if delayedMeta := t.peekDelayed(); delayedMeta != nil {
		nextTime = delayedMeta.ReadyAt
		hasNext = true
	}

	// 检查 Reserved 队列
	for _, meta := range t.reserved {
		deadline := meta.ReserveDeadline()
		if !deadline.IsZero() {
			if !hasNext || deadline.Before(nextTime) {
				nextTime = deadline
				hasNext = true
			}
		}
	}

	if !hasNext {
		return time.Time{} // zero time 表示不需要 tick
	}

	return nextTime
}

// NeedsTick 是否需要 tick
// 注意：调用者必须持有 DelayQueue.mu（通过 topicWrapper）
func (t *topic) NeedsTick() bool {
	return t.delayed.Len() > 0 || len(t.reserved) > 0
}

// processDelayed 处理延迟任务到期
// 注意：调用者必须持有 DelayQueue.mu
func (t *topic) processDelayed(now time.Time) bool {
	hasReady := false
	for {
		meta := t.peekDelayed()
		if meta == nil {
			break
		}

		if !meta.ShouldBeReady(now) {
			break
		}

		// 从 Delayed 移到 Ready
		t.popDelayed()
		meta.DelayState = DelayStateReady
		t.pushReady(meta)
		hasReady = true
	}
	return hasReady
}

// processReservedTimeout 处理保留任务超时
// 注意：调用者必须持有 DelayQueue.mu
func (t *topic) processReservedTimeout(now time.Time) bool {
	// 收集超时的任务
	timeoutIDs := make([]uint64, 0)
	for id, meta := range t.reserved {
		if meta.ShouldTimeout(now) {
			timeoutIDs = append(timeoutIDs, id)
		}
	}

	if len(timeoutIDs) == 0 {
		return false
	}

	// 处理超时任务
	for _, id := range timeoutIDs {
		meta := t.removeReserved(id)
		if meta != nil {
			meta.DelayState = DelayStateReady
			meta.Timeouts++
			meta.Releases++
			meta.ReservedAt = time.Time{}
			meta.ReadyAt = now
			t.pushReady(meta)
		}
	}
	return true
}
