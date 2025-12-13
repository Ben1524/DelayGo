package delaygo

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

// === delayJobMetaHeapByReadyAt 延迟任务堆（按 ReadyAt 排序） ===

type delayJobMetaHeapByReadyAt struct {
	items []*DelayJobMeta
	index map[uint64]int // job ID -> index
}

func newDelayJobHeap() *delayJobMetaHeapByReadyAt {
	h := &delayJobMetaHeapByReadyAt{
		items: make([]*DelayJobMeta, 0),
		index: make(map[uint64]int),
	}
	heap.Init(h)
	return h
}

// Len 实现 heap.Interface
func (h *delayJobMetaHeapByReadyAt) Len() int {
	return len(h.items)
}

// Less 实现 heap.Interface
// 按就绪时间排序，时间越早优先级越高
func (h *delayJobMetaHeapByReadyAt) Less(i, j int) bool {
	return h.items[i].ReadyAt.Before(h.items[j].ReadyAt)
}

// Swap 实现 heap.Interface
func (h *delayJobMetaHeapByReadyAt) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.index[h.items[i].ID] = i
	h.index[h.items[j].ID] = j
}

// Push 实现 heap.Interface
func (h *delayJobMetaHeapByReadyAt) Push(x any) {
	meta := x.(*DelayJobMeta)
	h.index[meta.ID] = len(h.items)
	h.items = append(h.items, meta)
}

// Pop 实现 heap.Interface
func (h *delayJobMetaHeapByReadyAt) Pop() any {
	old := h.items
	n := len(old)
	meta := old[n-1]
	old[n-1] = nil
	h.items = old[0 : n-1]
	delete(h.index, meta.ID)
	return meta
}

// find 查找指定任务
func (h *delayJobMetaHeapByReadyAt) find(id uint64) *DelayJobMeta {
	idx, ok := h.index[id]
	if !ok {
		return nil
	}
	return h.items[idx]
}

// remove 移除指定任务
func (h *delayJobMetaHeapByReadyAt) remove(id uint64) bool {
	idx, ok := h.index[id]
	if !ok {
		return false
	}
	heap.Remove(h, idx)
	return true
}

// peek 查看堆顶元素
func (h *delayJobMetaHeapByReadyAt) Peek() *DelayJobMeta {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

// === delayJobMetaHeapByPriority 实现任务元数据优先级堆（最小堆，按 Priority 排序） ===

type delayJobMetaHeapByPriority struct {
	items []*DelayJobMeta
	index map[uint64]int // delayJob ID -> index
}

func newDelayJobMetaHeap() *delayJobMetaHeapByPriority {
	h := &delayJobMetaHeapByPriority{
		items: make([]*DelayJobMeta, 0),
		index: make(map[uint64]int),
	}
	heap.Init(h)
	return h
}

// Len 实现 heap.Interface
func (h *delayJobMetaHeapByPriority) Len() int {
	return len(h.items)
}

// Less 实现 heap.Interface
// 优先级数字越小优先级越高，相同优先级按 ID 排序（FIFO）
func (h *delayJobMetaHeapByPriority) Less(i, j int) bool {
	if h.items[i].Priority != h.items[j].Priority {
		return h.items[i].Priority < h.items[j].Priority
	}
	return h.items[i].ID < h.items[j].ID
}

// Swap 实现 heap.Interface
func (h *delayJobMetaHeapByPriority) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.index[h.items[i].ID] = i
	h.index[h.items[j].ID] = j
}

// Push 实现 heap.Interface
func (h *delayJobMetaHeapByPriority) Push(x any) {
	meta := x.(*DelayJobMeta)
	h.index[meta.ID] = len(h.items)
	h.items = append(h.items, meta)
}

// Pop 实现 heap.Interface
func (h *delayJobMetaHeapByPriority) Pop() any {
	old := h.items
	n := len(old)
	meta := old[n-1]
	old[n-1] = nil
	h.items = old[0 : n-1]
	delete(h.index, meta.ID)
	return meta
}

// remove 移除指定任务
func (h *delayJobMetaHeapByPriority) remove(id uint64) bool {
	idx, ok := h.index[id]
	if !ok {
		return false
	}
	heap.Remove(h, idx)
	return true
}

// find 查找指定任务
func (h *delayJobMetaHeapByPriority) find(id uint64) *DelayJobMeta {
	idx, ok := h.index[id]
	if !ok {
		return nil
	}
	return h.items[idx]
}

// DelayJobMeta 对象池，减少 GC 压力
// 在高并发场景下（每秒数千任务），对象池可显著降低 GC 压力
var delayJobMetaPool *sync.Pool

// var delayJobMetaPool *pool.ObjectPool

func init() {
	delayJobMetaPool = &sync.Pool{
		New: func() any {
			return &DelayJobMeta{}
		},
	}
}

// DelayState 任务状态
// @Description 任务状态
type DelayState int

const (
	// DelayStateEnqueued 已入队，等待加载到内存
	// 临时状态，任务刚创建时的初始状态
	DelayStateEnqueued DelayState = iota

	// DelayStateReady 就绪，等待 Reserve
	// 任务在 Topic.ReadyHeap 中，等待 Worker 拉取
	DelayStateReady

	// DelayStateDelayed 延迟中，等待到期
	// 任务在 Topic.DelayedHeap 中，到期后转为 Ready
	DelayStateDelayed

	// DelayStateReserved 已保留，Worker 处理中
	// 任务在 Topic.ReservedMap 中，受 TTR 保护
	DelayStateReserved

	// DelayStateBuried 已埋葬，暂时搁置
	// 任务在 Topic.BuriedHeap 中，需要 Kick 才能恢复
	DelayStateBuried
)

type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// String 返回状态的字符串表示
func (s DelayState) String() string {
	switch s {
	case DelayStateEnqueued:
		return "enqueued"
	case DelayStateReady:
		return "ready"
	case DelayStateDelayed:
		return "delayed"
	case DelayStateReserved:
		return "reserved"
	case DelayStateBuried:
		return "buried"
	default:
		return "unknown"
	}
}

// DelayJobMeta 任务元数据
// 轻量级结构，常驻内存用于调度
// 大小约 200 字节
// @Description 任务元数据
type DelayJobMeta struct {
	// === 基本信息 ===
	// @Description 任务 ID
	ID uint64 `json:"id"`
	// @Description 所属 topic
	Topic string `json:"topic"`
	// @Description 优先级（数字越小优先级越高）
	Priority uint32 `json:"priority"`
	// @Description 当前状态
	DelayState DelayState `json:"delay_state"`

	// === 时间参数 ===
	// @Description 延迟时间（创建时指定）
	Delay time.Duration `json:"delay"`
	// @Description Time To Run - 执行超时时间
	TTR time.Duration `json:"ttr"`

	// === 时间戳 ===
	// @Description 创建时间
	CreatedAt time.Time `json:"created_at"`
	// @Description 就绪时间（延迟任务的到期时间）
	ReadyAt time.Time `json:"ready_at"`
	// @Description 被保留时间
	ReservedAt time.Time `json:"reserved_at"`
	// @Description 最后一次 Touch 的时间
	LastTouchAt time.Time `json:"last_touch_at"`
	// @Description 被埋葬时间
	BuriedAt time.Time `json:"buried_at"`
	// @Description 删除时间（软删除时使用）
	DeletedAt time.Time `json:"deleted_at"`

	// === 统计信息 ===
	// @Description 被保留次数
	Reserves int `json:"reserves"`
	// @Description 超时次数
	Timeouts int `json:"timeouts"`
	// @Description 被释放次数
	Releases int `json:"releases"`
	// @Description 被埋葬次数
	Buries int `json:"buries"`
	// @Description 被踢出次数
	Kicks int `json:"kicks"`
	// @Description Touch 次数（TTR 延长）
	Touches int `json:"touches"`
	// @Description 累计延长的总时间（用于 MaxTouchDuration 检查）
	TotalTouchTime time.Duration `json:"total_touch_time"`
}

// DelayJob会持久化存储在后端存储中
// DelayJob 完整任务
// 包含元数据和 Body
// Reserve 时才组装完整的 DelayJob 返回给 Worker
// @Description 完整任务
type DelayJob struct {
	_    noCopy
	Meta *DelayJobMeta // 元数据
	body []byte        // 任务数据（可能很大，按需加载）私有字段

	// 延迟加载支持
	storage  Storage   // 存储后端引用
	bodyOnce sync.Once // 确保 Body 只加载一次（并发安全）
	bodyErr  error     // Body 加载错误

	// DelayQueue 引用（用于操作方法）
	queue *DelayQueue // 所属队列引用
}

// GetBody 获取任务 Body（延迟加载，并发安全）
func (j *DelayJob) GetBody() ([]byte, error) {
	j.bodyOnce.Do(func() {
		// 如果没有 storage，说明是内存模式或已经传入了 body
		if j.storage == nil {
			return
		}

		// 从 Storage 加载
		j.body, j.bodyErr = j.storage.GetDelayJobBody(context.Background(), j.Meta.ID)
		if j.bodyErr != nil {
			j.bodyErr = fmt.Errorf("load delayJob body: %w", j.bodyErr)
		}
	})
	return j.body, j.bodyErr
}

// Body 返回任务 Body（便捷方法，忽略错误）
// 如果加载失败，返回 nil
// 建议使用 GetBody() 以获得错误信息
func (j *DelayJob) Body() []byte {
	body, _ := j.GetBody()
	return body
}

// NewDelayJobMeta 创建新的任务元数据（从对象池获取）
func NewDelayJobMeta(id uint64, topic string, priority uint32, delay, ttr time.Duration) *DelayJobMeta {
	now := time.Now()

	// 从对象池获取（已在 ReleaseDelayJobMeta 中重置为零值）
	meta := delayJobMetaPool.Get().(*DelayJobMeta)

	// 设置字段
	meta.ID = id
	meta.Topic = topic
	meta.Priority = priority
	meta.DelayState = DelayStateEnqueued
	meta.Delay = delay
	meta.TTR = ttr
	meta.CreatedAt = now

	// 显式重置状态字段，防止对象池污染
	meta.Touches = 0
	meta.TotalTouchTime = 0
	meta.LastTouchAt = time.Time{}
	meta.ReservedAt = time.Time{}
	meta.Reserves = 0
	meta.Timeouts = 0
	meta.Releases = 0
	meta.Buries = 0
	meta.Kicks = 0

	// 设置就绪时间
	if delay > 0 {
		meta.ReadyAt = now.Add(delay)
	} else {
		meta.ReadyAt = time.Time{} // 0 表示立即就绪  time.Now()存在误差！！！
	}

	return meta
}



// ReleaseDelayJobMeta 释放任务元数据回对象池
// 注意：释放后不应再使用该对象
func ReleaseDelayJobMeta(meta *DelayJobMeta) {
	if meta != nil {
		// 重置为零值，避免对象池污染
		// 设计选择：在 Put 时重置而非 Get 时重置
		// 优势：
		//   1. 防御性更强：即使忘记在 NewDelayJobMeta 中重置某字段也不会出 bug
		//   2. 单一职责：ReleaseDelayJobMeta 负责清理，NewDelayJobMeta 只负责初始化
		//   3. 易维护：新增字段时无需手动维护重置列表
		// 性能：
		//   虽然看起来是"分配新结构体"，但编译器会优化为逐字段清零（类似 memclr）
		//   实测开销约 8ns，远小于一次堆分配（58ns），且不产生实际的堆分配
		*meta = DelayJobMeta{}
		delayJobMetaPool.Put(meta)
	}
}

// NewDelayJob 创建完整任务（立即加载模式）
func NewDelayJob(meta *DelayJobMeta, body []byte, queue *DelayQueue) *DelayJob {
	delayJob := &DelayJob{
		Meta:  meta,
		body:  body,
		queue: queue,
	}
	// 标记为已加载（通过调用一次 bodyOnce）
	delayJob.bodyOnce.Do(func() {})
	return delayJob
}

// NewDelayJobWithStorage 创建任务（延迟加载模式）
func NewDelayJobWithStorage(meta *DelayJobMeta, storage Storage, queue *DelayQueue) *DelayJob {
	return &DelayJob{
		Meta:    meta.Clone(), // 克隆 meta,避免外部修改影响
		storage: storage,
		queue:   queue,
	}
}

// Clone 克隆元数据（用于返回副本，避免外部修改）
func (m *DelayJobMeta) Clone() *DelayJobMeta {
	clone := *m
	return &clone
}

// ShouldBeReady 判断延迟任务是否应该转为 Ready 状态
// 只对 DelayStateDelayed 状态有效
func (m *DelayJobMeta) ShouldBeReady(now time.Time) bool {
	return m.DelayState == DelayStateDelayed && !m.ReadyAt.After(now)
}

// ShouldTimeout 判断保留任务是否应该超时转回 Ready 状态
// 只对 DelayStateReserved 状态有效
func (m *DelayJobMeta) ShouldTimeout(now time.Time) bool {
	if m.DelayState != DelayStateReserved {
		return false
	}
	deadline := m.ReservedAt.Add(m.TTR)
	return !deadline.After(now)
}

// ReserveDeadline 返回保留任务的截止时间
// 只对 DelayStateReserved 状态有效
func (m *DelayJobMeta) ReserveDeadline() time.Time {
	if m.DelayState == DelayStateReserved {
		return m.ReservedAt.Add(m.TTR)
	}
	return time.Time{}
}

// TimeUntilReady 返回延迟任务距离就绪的时间
// 只对 DelayStateDelayed 状态有效
func (m *DelayJobMeta) TimeUntilReady(now time.Time) time.Duration {
	if m.DelayState != DelayStateDelayed {
		return 0
	}
	if m.ReadyAt.Before(now) {
		return 0
	}
	return m.ReadyAt.Sub(now)
}

// TimeUntilTimeout 返回保留任务距离超时的时间
// 只对 DelayStateReserved 状态有效
func (m *DelayJobMeta) TimeUntilTimeout(now time.Time) time.Duration {
	if m.DelayState != DelayStateReserved {
		return 0
	}
	deadline := m.ReserveDeadline()
	if deadline.Before(now) {
		return 0
	}
	return deadline.Sub(now)
}

// === DelayJob 操作方法 ===

// Delete 删除任务（必须是已保留状态）
func (j *DelayJob) Delete() error {
	if j.queue == nil {
		return fmt.Errorf("delayJob has no queue reference")
	}
	return j.queue.Delete(j.Meta.ID)
}

// Release 释放已保留的任务
// priority: 新的优先级
// delay: 延迟时间
func (j *DelayJob) Release(priority uint32, delay time.Duration) error {
	if j.queue == nil {
		return fmt.Errorf("delayJob has no queue reference")
	}
	return j.queue.Release(j.Meta.ID, priority, delay)
}

// Bury 埋葬已保留的任务
// priority: 新的优先级
func (j *DelayJob) Bury(priority uint32) error {
	if j.queue == nil {
		return fmt.Errorf("delayJob has no queue reference")
	}
	return j.queue.Bury(j.Meta.ID, priority)
}

// Kick 踢出埋葬任务（将此任务从埋葬状态恢复）
// 注意：此方法只能用于已埋葬的任务
func (j *DelayJob) Kick() error {
	if j.queue == nil {
		return fmt.Errorf("delayJob has no queue reference")
	}
	return j.queue.KickDelayJob(j.Meta.ID)
}

// Touch 延长任务的 TTR
// 支持两种模式：
// - Touch() - 重置为原始 TTR
// - Touch(duration) - 延长指定时间
func (j *DelayJob) Touch(duration ...time.Duration) error {
	if j.queue == nil {
		return fmt.Errorf("delayJob has no queue reference")
	}
	return j.queue.Touch(j.Meta.ID, duration...)
}
