package delaygo

import (
	"context"
	"sync"
)

// MemoryStorage 内存存储实现
// 用于测试和不需要持久化的场景
type MemoryStorage struct {
	mu     sync.RWMutex
	metas  map[uint64]*DelayJobMeta // 任务元数据
	bodies map[uint64][]byte        // 任务 Body
}

// NewMemoryStorage 创建内存存储
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		metas:  make(map[uint64]*DelayJobMeta),
		bodies: make(map[uint64][]byte),
	}
}

// SaveDelayJob 保存任务（元数据 + Body）
func (s *MemoryStorage) SaveDelayJob(ctx context.Context, meta *DelayJobMeta, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metas[meta.ID]; exists {
		return ErrDelayJobExists
	}

	// 保存元数据（克隆）
	s.metas[meta.ID] = meta.Clone()

	// 保存 Body（复制）
	if len(body) > 0 {
		bodyCopy := make([]byte, len(body))
		copy(bodyCopy, body)
		s.bodies[meta.ID] = bodyCopy
	}

	return nil
}

// UpdateDelayJobMeta 更新任务元数据
func (s *MemoryStorage) UpdateDelayJobMeta(ctx context.Context, meta *DelayJobMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metas[meta.ID]; !exists {
		return ErrNotFound
	}

	// 更新元数据（克隆）
	s.metas[meta.ID] = meta.Clone()

	return nil
}

// GetDelayJobMeta 获取任务元数据
func (s *MemoryStorage) GetDelayJobMeta(ctx context.Context, id uint64) (*DelayJobMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, ok := s.metas[id]
	if !ok {
		return nil, ErrNotFound
	}

	return meta.Clone(), nil
}

// GetDelayJobBody 获取任务 Body
func (s *MemoryStorage) GetDelayJobBody(ctx context.Context, id uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	body, ok := s.bodies[id]
	if !ok {
		return nil, ErrNotFound
	}

	// 返回副本
	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)
	return bodyCopy, nil
}

// DeleteDelayJob 删除任务（元数据 + Body）
func (s *MemoryStorage) DeleteDelayJob(ctx context.Context, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metas[id]; !exists {
		return ErrNotFound
	}

	delete(s.metas, id)
	delete(s.bodies, id)

	return nil
}

// BatchDeleteDelayJobs 批量删除任务（元数据 + Body）
func (s *MemoryStorage) BatchDeleteDelayJobs(ctx context.Context, ids []uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range ids {
		delete(s.metas, id)
		delete(s.bodies, id)
	}

	return nil
}

// ScanDelayJobMeta 扫描任务元数据
func (s *MemoryStorage) ScanDelayJobMeta(ctx context.Context, filter *DelayJobMetaFilter) (*DelayJobMetaList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 收集所有符合条件的元数据
	var allMetas []*DelayJobMeta

	for _, meta := range s.metas {
		// 过滤 Topic
		if filter != nil && filter.Topic != "" && meta.Topic != filter.Topic {
			continue
		}

		// 过滤 DelayState
		if filter != nil && filter.DelayState != nil && meta.DelayState != *filter.DelayState {
			continue
		}

		allMetas = append(allMetas, meta.Clone())
	}

	total := len(allMetas)

	// 应用 Offset 和 Limit
	var resultMetas []*DelayJobMeta
	hasMore := false
	nextCursor := uint64(0)

	if filter != nil {
		// Offset 分页
		if filter.Offset > 0 {
			if filter.Offset >= len(allMetas) {
				allMetas = nil
			} else {
				allMetas = allMetas[filter.Offset:]
			}
		}

		// Limit
		if filter.Limit > 0 && len(allMetas) > filter.Limit {
			resultMetas = allMetas[:filter.Limit]
			hasMore = true
			if len(resultMetas) > 0 {
				nextCursor = resultMetas[len(resultMetas)-1].ID
			}
		} else {
			resultMetas = allMetas
		}
	} else {
		resultMetas = allMetas
	}

	return &DelayJobMetaList{
		Metas:      resultMetas,
		Total:      total,
		HasMore:    hasMore,
		NextCursor: nextCursor,
	}, nil
}

// CountDelayJobs 统计任务数量
func (s *MemoryStorage) CountDelayJobs(ctx context.Context, filter *DelayJobMetaFilter) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0

	for _, meta := range s.metas {
		// 过滤 Topic
		if filter != nil && filter.Topic != "" && meta.Topic != filter.Topic {
			continue
		}

		// 过滤 DelayState
		if filter != nil && filter.DelayState != nil && meta.DelayState != *filter.DelayState {
			continue
		}

		count++
	}

	return count, nil
}

// Close 关闭存储
func (s *MemoryStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 清空数据
	s.metas = nil
	s.bodies = nil

	return nil
}

// Stats 返回存储统计信息
func (s *MemoryStorage) Stats(ctx context.Context) (*StorageStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := &StorageStats{
		TotalDelayJobs: int64(len(s.metas)),
	}

	// 统计不同 topic 数量
	topics := make(map[string]bool)
	var metaSize int64
	var bodySize int64

	for id, meta := range s.metas {
		topics[meta.Topic] = true
		metaSize += 200 // 估算每个 DelayJobMeta 约 200 字节

		if body, ok := s.bodies[id]; ok {
			bodySize += int64(len(body))
		}
	}

	stats.TotalTopics = len(topics)
	stats.MetaSize = metaSize
	stats.BodySize = bodySize
	stats.TotalSize = metaSize + bodySize

	if stats.TotalDelayJobs > 0 {
		stats.AvgMetaSize = metaSize / stats.TotalDelayJobs
		stats.AvgBodySize = bodySize / stats.TotalDelayJobs
	}

	stats.LoadedMetaSize = metaSize
	stats.LoadedBodySize = bodySize

	return stats, nil
}
