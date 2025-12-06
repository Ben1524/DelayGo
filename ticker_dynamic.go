package delaygo

import (
	"context"
	"math"
	"sync"
	"time"
)

// cachedTickable 缓存 Tickable 的下次 Tick 时间
type cachedTickable struct {
	tickable Tickable
	nextTime time.Time
	dirty    bool // 是否需要重新计算
}

// DynamicSleepTicker 动态 Sleep 模式的定时器
// 根据最近的到期时间动态计算 sleep 时间
type DynamicSleepTicker struct {
	mu           sync.RWMutex
	registry     map[string]*cachedTickable // 注册的对象（带缓存）
	minInterval  time.Duration              // 最小 tick 间隔
	idleInterval time.Duration              // 空闲时的 tick 间隔

	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	wakeupChan chan struct{} // 唤醒通道
}

// NewDynamicSleepTicker 创建动态 Sleep 模式的定时器
func NewDynamicSleepTicker(minInterval, idleInterval time.Duration) Ticker {
	if minInterval == 0 {
		minInterval = 10 * time.Millisecond
	}
	if idleInterval == 0 {
		idleInterval = 1 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &DynamicSleepTicker{
		registry:     make(map[string]*cachedTickable),
		minInterval:  minInterval,
		idleInterval: idleInterval,
		ctx:          ctx,
		cancel:       cancel,
		wakeupChan:   make(chan struct{}, 1),
	}
}

// Start 启动定时器
func (w *DynamicSleepTicker) Start() {
	w.wg.Add(1)
	go w.run()
}

// Stop 停止定时器
func (w *DynamicSleepTicker) Stop() {
	w.cancel()
	w.wg.Wait()
}

// Register 注册对象
func (w *DynamicSleepTicker) Register(name string, tickable Tickable) {
	w.mu.Lock()
	_, exists := w.registry[name]
	w.mu.Unlock()

	if !exists {
		// 在锁外调用 NextTickTime，避免死锁
		nextTime := tickable.NextTickTime()

		w.mu.Lock()
		// 再次检查，防止并发注册
		if _, exists := w.registry[name]; !exists {
			w.registry[name] = &cachedTickable{
				tickable: tickable,
				nextTime: nextTime,
				dirty:    false,
			}
		}
		w.mu.Unlock()

		// 在锁外唤醒，避免重入锁
		w.wakeup()
	}
}

// Unregister 取消注销
func (w *DynamicSleepTicker) Unregister(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.registry, name)
}

// Wakeup 唤醒定时器（公开方法）
func (w *DynamicSleepTicker) Wakeup() {
	// 标记所有缓存为 dirty，强制重新计算
	w.mu.Lock()
	for _, ct := range w.registry {
		ct.dirty = true
	}
	w.mu.Unlock()

	w.wakeup()
}

// wakeup 内部唤醒方法
func (w *DynamicSleepTicker) wakeup() {
	select {
	case w.wakeupChan <- struct{}{}:
	default:
	}
}

// Stats 返回统计信息
func (w *DynamicSleepTicker) Stats() *TickerStats {
	w.mu.RLock()
	defer w.mu.RUnlock()

	nextTime, hasNext := w.calculateNextTime()

	stats := &TickerStats{
		RegisteredCount: len(w.registry),
		Mode:            "dynamic",
	}

	if hasNext {
		stats.NextTickTime = nextTime
		now := time.Now()
		if nextTime.After(now) {
			stats.TimeUntilTick = nextTime.Sub(now)
		}
	} else {
		stats.TimeUntilTick = time.Duration(math.MaxInt64)
	}

	return stats
}

// run 主循环
func (w *DynamicSleepTicker) run() {
	defer w.wg.Done()

	for {
		// 计算下一个到期时间
		nextTime, hasNext := w.calculateNextTime()

		var sleepDuration time.Duration
		if hasNext {
			now := time.Now()
			if nextTime.After(now) {
				// 限制最小间隔
				sleepDuration = max(nextTime.Sub(now), w.minInterval)
			} else {
				// 已经过期，立即处理
				sleepDuration = 0
			}
		} else {
			// 没有任务，使用空闲间隔
			sleepDuration = w.idleInterval
		}

		// Sleep 或等待唤醒
		if sleepDuration == 0 {
			// 立即处理，但先检查是否需要退出
			select {
			case <-w.ctx.Done():
				return
			default:
				w.processTick()
			}
		} else {
			select {
			case <-w.ctx.Done():
				return
			case <-w.wakeupChan:
				// 被唤醒，重新计算
				continue
			case <-time.After(sleepDuration):
				// 到期，处理 tick
				w.processTick()
			}
		}
	}
}

// calculateNextTime 计算下一个最近的到期时间（使用缓存）
func (w *DynamicSleepTicker) calculateNextTime() (time.Time, bool) {
	w.mu.RLock()
	cached := make([]*cachedTickable, 0, len(w.registry))
	for _, ct := range w.registry {
		cached = append(cached, ct)
	}
	w.mu.RUnlock()

	var nextTime time.Time
	hasNext := false

	for _, ct := range cached {
		// 只在 dirty 时重新计算
		if ct.dirty {
			ct.nextTime = ct.tickable.NextTickTime()
			ct.dirty = false
		}

		t := ct.nextTime
		if t.IsZero() {
			continue
		}

		if !hasNext || t.Before(nextTime) {
			nextTime = t
			hasNext = true
		}
	}

	return nextTime, hasNext
}

// processTick 处理 tick，通知所有注册的对象
func (w *DynamicSleepTicker) processTick() {
	now := time.Now()

	w.mu.RLock()
	// 复制一份列表，避免长时间持锁
	cached := make([]*cachedTickable, 0, len(w.registry))
	for _, ct := range w.registry {
		cached = append(cached, ct)
	}
	w.mu.RUnlock()

	// 通知各个对象处理（只处理到期的）
	for _, ct := range cached {
		// 如果是 dirty，重新获取时间
		if ct.dirty {
			ct.nextTime = ct.tickable.NextTickTime()
			ct.dirty = false
		}

		// 检查是否需要 tick
		if ct.nextTime.IsZero() || now.Before(ct.nextTime) {
			continue
		}

		ct.tickable.ProcessTick(now)
		// 标记为 dirty，下次需要重新计算
		ct.dirty = true
	}
}
