package delaygo

import (
	"time"
)

// Tickable 可被 tick 的对象接口
// Topic 实现此接口以接收 tick 通知
type Tickable interface {
	// ProcessTick 处理 tick 通知
	ProcessTick(now time.Time)

	// NextTickTime 返回下一个需要 tick 的时间
	// 返回 zero time 表示不需要 tick
	NextTickTime() time.Time

	// NeedsTick 是否需要 tick
	NeedsTick() bool
}

// Ticker 定时器接口
// 负责定时触发已注册对象的 ProcessTick 方法
type Ticker interface {
	// Start 启动定时器
	Start()

	// Stop 停止定时器
	Stop()

	// Register 注册需要 tick 的对象
	Register(name string, tickable Tickable)

	// Unregister 取消注册
	Unregister(name string)

	// Wakeup 唤醒定时器（新任务加入时）
	Wakeup()

	// Stats 返回统计信息
	Stats() *TickerStats
}

// TickerStats 定时器统计信息
// @Description 定时器统计信息
type TickerStats struct {
	// @Description 注册的对象数量
	RegisteredCount int `json:"registered_count"`
	// @Description 下一个 tick 时间
	NextTickTime time.Time `json:"next_tick_time"`
	// @Description 距离下一个 tick 的时间
	TimeUntilTick time.Duration `json:"time_until_tick"`
	// @Description 模式：dynamic 或 timewheel
	Mode string `json:"mode"`
}
