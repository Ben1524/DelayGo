package delaygo

import (
	"container/list"
	"context"
	"sync"
	"time"
)

const (
	// MaxWaitersPerTopic 每个 topic 最大等待 worker 数
	// 防止大量 worker 等待导致内存占用过高
	MaxWaitersPerTopic = 1000
)

// ReserveHandler 处理 Reserve 操作的接口
// 由 DelayQueue 实现，提供给 reserveManager 使用
type ReserveHandler interface {
	// TryReserve 尝试立即预留任务
	TryReserve(topics []string) *DelayJobMeta
	// GetStorage 获取 storage
	GetStorage() Storage
	// GetDelayQueue 获取 DelayQueue 引用（用于 DelayJob 操作方法）
	GetDelayQueue() *DelayQueue
}

// reserveWaiter 等待 Reserve 的连接
// 模仿 beanstalkd 的 waiting connection 机制
type reserveWaiter struct {
	topics   []string       // 监听的 topics
	resultCh chan *DelayJob // 结果通道
	cancelCh chan struct{}  // 取消通道
	deadline time.Time      // 超时时间
}

// reserveManager 管理 Reserve 等待队列
// 模仿 beanstalkd 的 waiting connection 机制，防止惊群效应
type reserveManager struct {
	mu          sync.RWMutex
	waitingCons map[string]*list.List // topic -> 等待的 waiter 队列 (FIFO)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newReserveManager 创建新的 reserveManager
func newReserveManager(ctx context.Context) *reserveManager {
	ctx, cancel := context.WithCancel(ctx)
	return &reserveManager{
		waitingCons: make(map[string]*list.List),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// start 启动后台清理任务
func (rm *reserveManager) start() {
	rm.wg.Add(1)
	go func() {
		defer rm.wg.Done()
		ticker := time.NewTicker(1 * time.Second) // 每秒清理一次
		defer ticker.Stop()

		for {
			select {
			case <-rm.ctx.Done():
				return
			case <-ticker.C:
				rm.cleanupExpiredWaiters()
			}
		}
	}()
}

// stop 停止 reserveManager
func (rm *reserveManager) stop() {
	rm.cancel()
	rm.wg.Wait()
}

// Reserve 从指定 topics 保留一个任务
// topics: topic 名称列表
// timeout: 等待超时时间，0 表示立即返回
// handler: 处理 Reserve 操作的接口
func (rm *reserveManager) Reserve(
	topics []string,
	timeout time.Duration,
	handler ReserveHandler,
) (*DelayJob, error) {
	// 1. 先尝试立即获取（快速路径）
	meta := handler.TryReserve(topics)
	if meta != nil {
		return NewDelayJobWithStorage(meta, handler.GetStorage(), handler.GetDelayQueue()), nil
	}

	// 2. 非阻塞模式，立即返回
	if timeout == 0 {
		return nil, ErrTimeout
	}

	// 3. 没有任务，注册到等待队列
	waiter := &reserveWaiter{
		topics:   topics,
		resultCh: make(chan *DelayJob, 1),
		cancelCh: make(chan struct{}),
	}

	if timeout > 0 {
		waiter.deadline = time.Now().Add(timeout)
	}

	// 注册 waiter（可能失败）
	if err := rm.registerWaiter(topics, waiter); err != nil {
		return nil, err
	}

	// 4. 等待结果或超时
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		defer timer.Stop()
	}

	select {
	case delayJob := <-waiter.resultCh:
		// 成功获取任务
		return delayJob, nil

	case <-waiter.cancelCh:
		// 被取消
		return nil, ErrTimeout

	case <-timer.C:
		// 超时
		rm.removeWaiter(waiter)
		return nil, ErrTimeout

	case <-rm.ctx.Done():
		// 管理器关闭
		rm.removeWaiter(waiter)
		return nil, rm.ctx.Err()
	}
}

// registerWaiter 注册等待者到指定的 topics
// 返回 error 如果超过最大等待数量限制
func (rm *reserveManager) registerWaiter(topics []string, waiter *reserveWaiter) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 检查每个 topic 的等待队列长度
	for _, topic := range topics {
		if rm.waitingCons[topic] == nil {
			rm.waitingCons[topic] = list.New()
		}

		// 检查限制
		if rm.waitingCons[topic].Len() >= MaxWaitersPerTopic {
			return ErrTooManyWaiters
		}
	}

	// 全部通过检查后再添加
	for _, topic := range topics {
		rm.waitingCons[topic].PushBack(waiter)
	}

	return nil
}

// notifyWaiters 通知等待队列有新任务可用
// 只唤醒一个 waiter（FIFO），防止惊群效应
// handler: 处理 Reserve 操作的接口
func (rm *reserveManager) notifyWaiters(topic string, handler ReserveHandler) {
	rm.mu.Lock()
	waitList := rm.waitingCons[topic]
	if waitList == nil || waitList.Len() == 0 {
		rm.mu.Unlock()
		return
	}

	// FIFO：从队头开始查找
	for elem := waitList.Front(); elem != nil; {
		next := elem.Next()
		waiter := elem.Value.(*reserveWaiter)

		// 跳过已超时的 waiter
		if !waiter.deadline.IsZero() && time.Now().After(waiter.deadline) {
			waitList.Remove(elem)
			elem = next
			continue
		}

		// 尝试为这个 waiter 预留任务
		meta := handler.TryReserve(waiter.topics)
		if meta != nil {
			// 成功预留，从所有队列中移除这个 waiter
			waitList.Remove(elem)
			rm.removeWaiterFromOtherTopicsLocked(waiter, topic)
			rm.mu.Unlock()

			// 创建 DelayJob 并发送给 waiter
			delayJob := NewDelayJobWithStorage(meta, handler.GetStorage(), handler.GetDelayQueue())
			select {
			case waiter.resultCh <- delayJob:
			default:
				// waiter 已经超时或取消，不再等待
			}

			// 只唤醒一个 waiter
			return
		}

		// 没有可用任务，结束查找
		break
	}

	rm.mu.Unlock()
}

// removeWaiter 从所有 topics 中移除指定的 waiter
func (rm *reserveManager) removeWaiter(waiter *reserveWaiter) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for _, topic := range waiter.topics {
		waitList := rm.waitingCons[topic]
		if waitList == nil {
			continue
		}

		// 遍历查找并移除
		for elem := waitList.Front(); elem != nil; elem = elem.Next() {
			if elem.Value.(*reserveWaiter) == waiter {
				waitList.Remove(elem)
				break
			}
		}
	}
}

// removeWaiterFromOtherTopicsLocked 从其他 topics 中移除 waiter
// 注意：调用者必须持有 rm.mu
func (rm *reserveManager) removeWaiterFromOtherTopicsLocked(waiter *reserveWaiter, exceptTopic string) {
	for _, topic := range waiter.topics {
		if topic == exceptTopic {
			continue
		}

		waitList := rm.waitingCons[topic]
		if waitList == nil {
			continue
		}

		// 遍历查找并移除
		for elem := waitList.Front(); elem != nil; elem = elem.Next() {
			if elem.Value.(*reserveWaiter) == waiter {
				waitList.Remove(elem)
				break
			}
		}
	}
}

// cleanupExpiredWaiters 清理过期的 waiters
func (rm *reserveManager) cleanupExpiredWaiters() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	now := time.Now()
	for topic, waitList := range rm.waitingCons {
		if waitList == nil {
			continue
		}

		for elem := waitList.Front(); elem != nil; {
			next := elem.Next()
			waiter := elem.Value.(*reserveWaiter)

			// 检查是否超时
			if !waiter.deadline.IsZero() && now.After(waiter.deadline) {
				waitList.Remove(elem)

				// 通知 waiter 超时（通过关闭 cancelCh）
				select {
				case <-waiter.cancelCh:
					// 已经关闭
				default:
					close(waiter.cancelCh)
				}
			}

			elem = next
		}

		// 清理空队列
		if waitList.Len() == 0 {
			delete(rm.waitingCons, topic)
		}
	}
}

// stats 返回等待队列统计信息
func (rm *reserveManager) stats() map[string]int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	result := make(map[string]int)
	for topic, waitList := range rm.waitingCons {
		if waitList != nil && waitList.Len() > 0 {
			result[topic] = waitList.Len()
		}
	}
	return result
}
