package delaygo

import (
	"context"
	"sync"
	"time"
)

// TimeWheelTicker 时间轮模式的定时器
// 固定间隔 tick，时间精度更高
type TimeWheelTicker struct {
	mu           sync.RWMutex
	registry     map[string]Tickable // 注册的对象
	tickInterval time.Duration       // 固定 tick 间隔
	slots        int                 // 时间槽数量

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	currentSlot int           // 当前时间槽
	wheel       [][]*tickTask // 时间轮：slot -> tasks

	// 暂停控制
	pauseChan  chan struct{} // 暂停信号
	resumeChan chan struct{} // 恢复信号
	paused     bool          // 是否暂停
}

// tickTask tick 任务
type tickTask struct {
	name     string
	tickable Tickable
	tickTime time.Time
}

// NewTimeWheelTicker 创建时间轮模式的定时器
// tickInterval: 每个 tick 的时间间隔（如 10ms）
// slots: 时间槽数量（如 3600，可以覆盖 36 秒）
func NewTimeWheelTicker(tickInterval time.Duration, slots int) Ticker {
	if tickInterval == 0 {
		tickInterval = 10 * time.Millisecond
	}
	if slots == 0 {
		slots = 3600 // 默认 3600 个槽
	}

	ctx, cancel := context.WithCancel(context.Background())

	wheel := make([][]*tickTask, slots)
	for i := range wheel {
		wheel[i] = make([]*tickTask, 0)
	}

	return &TimeWheelTicker{
		registry:     make(map[string]Tickable),
		tickInterval: tickInterval,
		slots:        slots,
		ctx:          ctx,
		cancel:       cancel,
		currentSlot:  0,
		wheel:        wheel,
		pauseChan:    make(chan struct{}, 1),
		resumeChan:   make(chan struct{}, 1),
		paused:       true, // 初始时暂停
	}
}

// Start 启动定时器
func (w *TimeWheelTicker) Start() {
	w.wg.Add(1)
	go w.run()
}

// Stop 停止定时器
func (w *TimeWheelTicker) Stop() {
	w.cancel()

	// 如果处于暂停状态，发送恢复信号以确保 goroutine 能退出
	w.mu.Lock()
	if w.paused {
		select {
		case w.resumeChan <- struct{}{}:
		default:
		}
	}
	w.mu.Unlock()

	w.wg.Wait()
}

// Register 注册对象
func (w *TimeWheelTicker) Register(name string, tickable Tickable) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.registry[name] = tickable
	w.scheduleTask(name, tickable)

	// 如果之前是暂停状态，现在恢复
	if w.paused {
		w.paused = false
		select {
		case w.resumeChan <- struct{}{}:
		default:
		}
	}
}

// Unregister 取消注册
func (w *TimeWheelTicker) Unregister(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.registry, name)
	// 从时间轮中移除任务
	w.removeTask(name)

	// 检查是否应该暂停
	w.checkAndPause()
}

// Wakeup 唤醒定时器（时间轮模式不需要唤醒）
func (w *TimeWheelTicker) Wakeup() {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 重新调度所有任务
	for name, tickable := range w.registry {
		w.scheduleTask(name, tickable)
	}

	// 如果之前是暂停状态，现在恢复
	if w.paused && w.hasAnyTask() {
		w.paused = false
		select {
		case w.resumeChan <- struct{}{}:
		default:
		}
	}
}

// Stats 返回统计信息
func (w *TimeWheelTicker) Stats() *TickerStats {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return &TickerStats{
		RegisteredCount: len(w.registry),
		NextTickTime:    time.Now().Add(w.tickInterval),
		TimeUntilTick:   w.tickInterval,
		Mode:            "timewheel",
	}
}

// run 主循环
func (w *TimeWheelTicker) run() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.tickInterval)
	defer ticker.Stop()

	for {
		// 检查是否暂停
		w.mu.RLock()
		isPaused := w.paused
		w.mu.RUnlock()

		if isPaused {
			// 暂停状态，等待恢复或退出
			select {
			case <-w.ctx.Done():
				return
			case <-w.resumeChan:
				// 恢复运行
				continue
			}
		}

		// 正常运行，等待 tick
		select {
		case <-w.ctx.Done():
			return
		case now := <-ticker.C:
			w.processTick(now)
		}
	}
}

// processTick 处理当前槽的任务
func (w *TimeWheelTicker) processTick(now time.Time) {
	w.mu.Lock()

	// 获取当前槽的任务
	tasks := w.wheel[w.currentSlot]
	w.wheel[w.currentSlot] = make([]*tickTask, 0)

	// 移动到下一个槽
	w.currentSlot = (w.currentSlot + 1) % w.slots

	w.mu.Unlock()

	// 处理任务
	for _, task := range tasks {
		// 检查是否真的到期
		if !task.tickTime.After(now) {
			task.tickable.ProcessTick(now)

			// 重新调度
			w.mu.Lock()
			if _, exists := w.registry[task.name]; exists {
				w.scheduleTask(task.name, task.tickable)
			}
			w.mu.Unlock()
		} else {
			// 还没到期，重新加入槽
			w.mu.Lock()
			slot := w.calculateSlot(task.tickTime)
			w.wheel[slot] = append(w.wheel[slot], task)
			w.mu.Unlock()
		}
	}

	// 检查是否应该暂停（所有槽都为空）
	w.mu.Lock()
	if !w.hasAnyTask() {
		w.paused = true
		select {
		case w.pauseChan <- struct{}{}:
		default:
		}
	}
	w.mu.Unlock()
}

// scheduleTask 调度任务到时间轮（需要持有锁）
func (w *TimeWheelTicker) scheduleTask(name string, tickable Tickable) {
	nextTime := tickable.NextTickTime()
	if nextTime.IsZero() {
		return
	}

	slot := w.calculateSlot(nextTime)
	w.wheel[slot] = append(w.wheel[slot], &tickTask{
		name:     name,
		tickable: tickable,
		tickTime: nextTime,
	})
}

// removeTask 从时间轮中移除任务（需要持有锁）
func (w *TimeWheelTicker) removeTask(name string) {
	for i := range w.wheel {
		newTasks := make([]*tickTask, 0)
		for _, task := range w.wheel[i] {
			if task.name != name {
				newTasks = append(newTasks, task)
			}
		}
		w.wheel[i] = newTasks
	}
}

// hasAnyTask 检查时间轮中是否有任何任务（需要持有锁）
func (w *TimeWheelTicker) hasAnyTask() bool {
	for i := range w.wheel {
		if len(w.wheel[i]) > 0 {
			return true
		}
	}
	return false
}

// checkAndPause 检查是否应该暂停（需要持有锁）
func (w *TimeWheelTicker) checkAndPause() {
	if !w.hasAnyTask() && !w.paused {
		w.paused = true
		select {
		case w.pauseChan <- struct{}{}:
		default:
		}
	}
}

// calculateSlot 计算任务应该放在哪个槽
func (w *TimeWheelTicker) calculateSlot(tickTime time.Time) int {
	now := time.Now()
	duration := tickTime.Sub(now)

	if duration <= 0 {
		// 已经过期，放在当前槽
		return w.currentSlot
	}

	// 计算需要多少个 tick
	ticks := int(duration / w.tickInterval)
	if duration%w.tickInterval > 0 {
		ticks++
	}

	// 计算槽位（循环）
	slot := (w.currentSlot + ticks) % w.slots

	return slot
}
