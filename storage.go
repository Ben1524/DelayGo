package delaygo

import (
	"context"
	"errors"
)

var (
	// ErrStorageClosed 存储已关闭
	ErrStorageClosed = errors.New("delaygo: storage closed")
	// ErrDelayJobExists 任务已存在
	ErrDelayJobExists = errors.New("delaygo: delayJob already exists")
)

// DelayJobMetaFilter 任务元数据过滤条件
type DelayJobMetaFilter struct {
	Topic      string      // 按 topic 过滤，空表示不过滤
	DelayState *DelayState // 按状态过滤，nil 表示不过滤
	Limit      int         // 返回数量限制，0 表示无限制
	Offset     int         // 偏移量，用于分页
	Cursor     uint64      // 游标（任务 ID），用于游标分页，0 表示从头开始
}

// DelayJobMetaList 任务元数据列表结果
type DelayJobMetaList struct {
	Metas      []*DelayJobMeta // 任务元数据列表
	Total      int             // 总数（可选，某些实现可能不支持）
	HasMore    bool            // 是否还有更多数据
	NextCursor uint64          // 下一页游标（最后一个任务的 ID）
}

// Storage 持久化存储接口
// 设计原则：元数据与 Body 分离存储
// - 元数据：轻量级（~200B），常驻内存，用于调度，会更新
// - Body：可能很大（KB~MB），按需加载，不可变
type Storage interface {
	// === 任务创建（原子操作） ===

	// SaveDelayJob 保存完整任务（元数据 + Body）
	// 只在 Put 时调用，同时保存 meta 和 body
	// Body 不可变，一旦保存就不会修改
	// 如果任务已存在则返回 ErrDelayJobExists
	SaveDelayJob(ctx context.Context, meta *DelayJobMeta, body []byte) error

	// === 元数据操作 ===

	// UpdateDelayJobMeta 更新任务元数据
	// 只更新元数据（状态、统计等），不涉及 Body
	// 如果任务不存在则返回 ErrNotFound
	UpdateDelayJobMeta(ctx context.Context, meta *DelayJobMeta) error

	// GetDelayJobMeta 获取任务元数据
	// 如果任务不存在则返回 ErrNotFound
	GetDelayJobMeta(ctx context.Context, id uint64) (*DelayJobMeta, error)

	// ScanDelayJobMeta 扫描任务元数据
	// 支持过滤、分页和游标
	// filter 为 nil 时返回所有任务元数据（用于启动恢复）
	// 启动恢复时：ScanDelayJobMeta(ctx, nil) 加载所有元数据，不加载 Body
	// 性能关键：100 万任务时只需 200MB 内存而不是 10GB
	ScanDelayJobMeta(ctx context.Context, filter *DelayJobMetaFilter) (*DelayJobMetaList, error)

	// === Body 操作 ===

	// GetDelayJobBody 获取任务 Body
	// 如果任务不存在则返回 ErrNotFound
	// Reserve 时才调用，按需加载
	GetDelayJobBody(ctx context.Context, id uint64) ([]byte, error)

	// === 任务删除 ===

	// DeleteDelayJob 删除任务（元数据 + Body）
	// 如果任务不存在则返回 ErrNotFound
	DeleteDelayJob(ctx context.Context, id uint64) error

	// 批量删除任务（元数据 + Body）
	// 如果某个任务不存在则忽略
	BatchDeleteDelayJobs(ctx context.Context, ids []uint64) error

	// === 统计查询 ===

	// CountDelayJobs 统计任务数量
	// filter 为 nil 时统计所有任务
	CountDelayJobs(ctx context.Context, filter *DelayJobMetaFilter) (int, error)

	// === 资源管理 ===

	// Close 关闭存储
	Close() error
}

// StorageStats 存储统计信息
// @Description 存储统计信息
type StorageStats struct {
	// @Description 总任务数
	TotalDelayJobs int64 `json:"total_delay_jobs"`
	// @Description 总 topic 数
	TotalTopics int `json:"total_topics"`
	// @Description 元数据存储大小（字节）
	MetaSize int64 `json:"meta_size"`
	// @Description Body 存储大小（字节）
	BodySize int64 `json:"body_size"`
	// @Description 总存储大小（字节）
	TotalSize int64 `json:"total_size"`
	// @Description 最后保存时间（Unix 时间戳）
	LastSaveTime int64 `json:"last_save_time"`
	// @Description 最后加载时间（Unix 时间戳）
	LastLoadTime int64 `json:"last_load_time"`
	// @Description 平均元数据大小（字节）
	AvgMetaSize int64 `json:"avg_meta_size"`
	// @Description 平均 Body 大小（字节）
	AvgBodySize int64 `json:"avg_body_size"`
	// @Description 已加载元数据大小（字节）
	LoadedMetaSize int64 `json:"loaded_meta_size"`
	// @Description 已加载 Body 大小（字节）
	LoadedBodySize int64 `json:"loaded_body_size"`
}

// StorageWithStats 支持统计信息的存储接口（可选）
type StorageWithStats interface {
	Storage
	// Stats 返回存储统计信息
	Stats(ctx context.Context) (*StorageStats, error)
}
