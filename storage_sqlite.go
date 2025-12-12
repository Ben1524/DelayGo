package delaygo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// saveDelayJobRequest SaveDelayJob 请求
type saveDelayJobRequest struct {
	meta *DelayJobMeta
	body []byte
	done chan error // 用于通知调用者操作完成
}

// SQLiteStorage SQLite 存储实现
// 内置批量更新和批量保存缓冲机制，自动合并并发调用
type SQLiteStorage struct {
	db     *sql.DB
	dbPath string
	mu     sync.RWMutex

	// 批量更新缓冲机制（用于 UpdateDelayJobMeta）
	updateBuffer   map[uint64]*DelayJobMeta // 待更新的任务元数据缓冲
	updateBufferMu sync.Mutex
	updateChan     chan *DelayJobMeta // 更新通道

	// 批量保存缓冲机制（用于 SaveDelayJob）
	// SaveDelayJob 是同步的，但内部会自动合并并发请求
	saveChan chan *saveDelayJobRequest // 保存请求通道

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closed  bool       // 标记是否已关闭
	closeMu sync.Mutex // 保护 closed 字段

	// 批量操作配置
	maxBatchSize  int // 批量操作最大数量，默认 1000
	maxBatchBytes int // 批量保存最大字节数，默认 16MB
}

// SQLiteStorageOption SQLite 存储配置选项
type SQLiteStorageOption func(*SQLiteStorage)

// WithMaxBatchSize 设置批量操作最大数量
func WithMaxBatchSize(size int) SQLiteStorageOption {
	return func(s *SQLiteStorage) {
		if size > 0 {
			s.maxBatchSize = size
		}
	}
}

// WithMaxBatchBytes 设置批量保存最大字节数
func WithMaxBatchBytes(bytes int) SQLiteStorageOption {
	return func(s *SQLiteStorage) {
		if bytes > 0 {
			s.maxBatchBytes = bytes
		}
	}
}

// NewSQLiteStorage 创建 SQLite 存储
func NewSQLiteStorage(dbPath string, opts ...SQLiteStorageOption) (*SQLiteStorage, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// 启用 WAL 模式以提高并发性能
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}
	// 设置 busy_timeout
	if _, err := db.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set busy_timeout: %w", err)
	}
	// 启用外键约束
	if _, err := db.Exec("PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set foreign_keys: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	storage := &SQLiteStorage{
		db:            db,
		dbPath:        dbPath,
		updateBuffer:  make(map[uint64]*DelayJobMeta),
		updateChan:    make(chan *DelayJobMeta, 1000),        // 缓冲 1000 个更新请求
		saveChan:      make(chan *saveDelayJobRequest, 1000), // 缓冲 1000 个保存请求
		ctx:           ctx,
		cancel:        cancel,
		maxBatchSize:  1000,             // 默认 1000
		maxBatchBytes: 16 * 1024 * 1024, // 默认 16MB
	}

	// 应用选项
	for _, opt := range opts {
		opt(storage)
	}

	// 初始化数据库表
	if err := storage.initTables(); err != nil {
		_ = db.Close()
		cancel()
		return nil, fmt.Errorf("init tables: %w", err)
	}

	// 启用 WAL 模式（Write-Ahead Logging）
	// WAL 模式允许读写并发，显著提升并发性能
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		cancel()
		return nil, fmt.Errorf("enable WAL mode: %w", err)
	}

	// 降低同步级别，提升写入性能
	// NORMAL: 在关键时刻同步，平衡性能和安全性
	if _, err := db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		_ = db.Close()
		cancel()
		return nil, fmt.Errorf("set synchronous mode: %w", err)
	}

	// WAL 模式下可以提高并发连接数
	db.SetMaxOpenConns(10) // 允许多个读连接
	db.SetMaxIdleConns(2)

	// 启动批量更新后台任务
	storage.wg.Add(1)
	go storage.batchUpdateLoop()

	// 启动批量保存后台任务
	storage.wg.Add(1)
	go storage.batchSaveLoop()

	return storage, nil
}

// initTables 初始化数据库表
func (s *SQLiteStorage) initTables() error {
	// 创建 delayJob_meta 表
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS delayJob_meta (
			id INTEGER PRIMARY KEY,
			topic TEXT NOT NULL,
			priority INTEGER NOT NULL,
			state INTEGER NOT NULL,
			delay INTEGER NOT NULL,
			ttr INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			ready_at INTEGER NOT NULL,
			reserved_at INTEGER,
			buried_at INTEGER,
			deleted_at INTEGER,
			reserves INTEGER DEFAULT 0,
			timeouts INTEGER DEFAULT 0,
			releases INTEGER DEFAULT 0,
			buries INTEGER DEFAULT 0,
			kicks INTEGER DEFAULT 0,
			touches INTEGER DEFAULT 0
		)
	`)
	if err != nil {
		return fmt.Errorf("create delayJob_meta table: %w", err)
	}

	// 创建索引
	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_delayJob_meta_topic ON delayJob_meta(topic)`)
	if err != nil {
		return fmt.Errorf("create topic index: %w", err)
	}

	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_delayJob_meta_state ON delayJob_meta(state)`)
	if err != nil {
		return fmt.Errorf("create state index: %w", err)
	}

	// 创建 delayJob_body 表
	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS delayJob_body (
			id INTEGER PRIMARY KEY,
			body BLOB NOT NULL,
			FOREIGN KEY (id) REFERENCES delayJob_meta(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return fmt.Errorf("create delayJob_body table: %w", err)
	}

	return nil
}

// SaveDelayJob 保存任务（元数据 + Body）
// 内部会自动合并并发请求，提升批量写入性能
func (s *SQLiteStorage) SaveDelayJob(ctx context.Context, meta *DelayJobMeta, body []byte) error {
	// 检查是否已关闭
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return ErrStorageClosed
	}
	s.closeMu.Unlock()

	// 克隆元数据和 body，避免外部修改
	metaCopy := meta.Clone()
	var bodyCopy []byte
	if len(body) > 0 {
		bodyCopy = make([]byte, len(body))
		copy(bodyCopy, body)
	}

	// 创建请求
	req := &saveDelayJobRequest{
		meta: metaCopy,
		body: bodyCopy,
		done: make(chan error, 1),
	}

	// 发送到 saveChan，由 batchSaveLoop 批量处理
	select {
	case s.saveChan <- req:
		return <-req.done // 等待结果
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// batchSaveDelayJobsInternal 批量保存任务（在单个事务中）
// 内部方法，由 SaveDelayJob 调用
func (s *SQLiteStorage) batchSaveDelayJobsInternal(requests []*saveDelayJobRequest) error {
	if len(requests) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		// 通知所有请求失败
		for _, req := range requests {
			req.done <- fmt.Errorf("begin transaction: %w", err)
		}
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 准备插入元数据的语句
	metaStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO delayJob_meta (
			id, topic, priority, state, delay, ttr,
			created_at, ready_at, reserved_at, buried_at, deleted_at,
			reserves, timeouts, releases, buries, kicks, touches
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		for _, req := range requests {
			req.done <- fmt.Errorf("prepare meta statement: %w", err)
		}
		return fmt.Errorf("prepare meta statement: %w", err)
	}
	defer func() { _ = metaStmt.Close() }()

	// 准备插入 Body 的语句
	bodyStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO delayJob_body (id, body) VALUES (?, ?)
	`)
	if err != nil {
		for _, req := range requests {
			req.done <- fmt.Errorf("prepare body statement: %w", err)
		}
		return fmt.Errorf("prepare body statement: %w", err)
	}
	defer func() { _ = bodyStmt.Close() }()

	// 批量执行插入
	var successReqs []*saveDelayJobRequest
	for _, req := range requests {
		meta := req.meta
		_, err := metaStmt.ExecContext(ctx,
			meta.ID,
			meta.Topic,
			meta.Priority,
			meta.DelayState,
			int64(meta.Delay),
			int64(meta.TTR),
			meta.CreatedAt.Unix(),
			meta.ReadyAt.Unix(),
			nullableTime(meta.ReservedAt),
			nullableTime(meta.BuriedAt),
			nullableTime(meta.DeletedAt),
			meta.Reserves,
			meta.Timeouts,
			meta.Releases,
			meta.Buries,
			meta.Kicks,
			meta.Touches,
		)
		if err != nil {
			// 检查是否是重复键错误
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				req.done <- ErrDelayJobExists
			} else {
				req.done <- err
			}
			continue
		}

		// 插入 Body
		if len(req.body) > 0 {
			_, err = bodyStmt.ExecContext(ctx, meta.ID, req.body)
			if err != nil {
				req.done <- err
				continue
			}
		}

		// 标记为成功（等待 commit）
		successReqs = append(successReqs, req)
	}

	// Commit 事务
	if err := tx.Commit(); err != nil {
		// Commit 失败，通知所有成功的请求
		commitErr := fmt.Errorf("commit transaction: %w", err)
		for _, req := range successReqs {
			req.done <- commitErr
		}
		return commitErr
	}

	// Commit 成功，通知所有成功的请求
	for _, req := range successReqs {
		req.done <- nil
	}

	return nil
}

// UpdateDelayJobMeta 更新任务元数据（异步批量缓冲）
// 自动将高频更新操作缓冲后批量写入，大幅降低 I/O 压力
func (s *SQLiteStorage) UpdateDelayJobMeta(ctx context.Context, meta *DelayJobMeta) error {
	// 克隆元数据，避免外部修改
	metaCopy := meta.Clone()

	// 发送到更新通道（非阻塞）
	select {
	case s.updateChan <- metaCopy:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	default:
		// 通道满了，阻塞等待（利用 Channel 的背压机制）
		// 注意：不要在这里调用 flushUpdates()，因为 flushUpdates() 需要获取锁，
		// 如果 batchUpdateLoop 正在持有锁（例如正在写入 DB），会导致死锁或长时间阻塞。
		// 正确的做法是让调用者阻塞，直到 batchUpdateLoop 处理完当前批次并腾出空间。
		select {
		case s.updateChan <- metaCopy:
			return nil
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// UpdateDelayJobMetaSync 同步更新任务元数据（立即写入，不缓冲）
// 用于需要立即持久化的关键操作（如删除前的最终更新）
func (s *SQLiteStorage) UpdateDelayJobMetaSync(ctx context.Context, meta *DelayJobMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.db.ExecContext(ctx, `
		UPDATE delayJob_meta SET
			topic = ?, priority = ?, state = ?,
			delay = ?, ttr = ?,
			created_at = ?, ready_at = ?,
			reserved_at = ?, buried_at = ?, deleted_at = ?,
			reserves = ?, timeouts = ?, releases = ?,
			buries = ?, kicks = ?, touches = ?
		WHERE id = ?
	`,
		meta.Topic, meta.Priority, meta.DelayState,
		int64(meta.Delay), int64(meta.TTR),
		meta.CreatedAt.Unix(), meta.ReadyAt.Unix(),
		nullableTime(meta.ReservedAt),
		nullableTime(meta.BuriedAt),
		nullableTime(meta.DeletedAt),
		meta.Reserves, meta.Timeouts, meta.Releases,
		meta.Buries, meta.Kicks, meta.Touches,
		meta.ID,
	)
	if err != nil {
		return fmt.Errorf("update delayJob_meta: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}

	return nil
}

// GetDelayJobMeta 获取任务元数据
func (s *SQLiteStorage) GetDelayJobMeta(ctx context.Context, id uint64) (*DelayJobMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var meta DelayJobMeta
	var delay, ttr, createdAt, readyAt int64
	var reservedAt, buriedAt, deletedAt sql.NullInt64

	err := s.db.QueryRowContext(ctx, `
		SELECT
			id, topic, priority, state, delay, ttr,
			created_at, ready_at, reserved_at, buried_at, deleted_at,
			reserves, timeouts, releases, buries, kicks, touches
		FROM delayJob_meta WHERE id = ?
	`, id).Scan(
		&meta.ID,
		&meta.Topic,
		&meta.Priority,
		&meta.DelayState,
		&delay,
		&ttr,
		&createdAt,
		&readyAt,
		&reservedAt,
		&buriedAt,
		&deletedAt,
		&meta.Reserves,
		&meta.Timeouts,
		&meta.Releases,
		&meta.Buries,
		&meta.Kicks,
		&meta.Touches,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query delayJob_meta: %w", err)
	}

	// 转换时间
	meta.Delay = time.Duration(delay)
	meta.TTR = time.Duration(ttr)
	meta.CreatedAt = time.Unix(createdAt, 0)
	meta.ReadyAt = time.Unix(readyAt, 0)
	if reservedAt.Valid {
		meta.ReservedAt = time.Unix(reservedAt.Int64, 0)
	}
	if buriedAt.Valid {
		meta.BuriedAt = time.Unix(buriedAt.Int64, 0)
	}
	if deletedAt.Valid {
		meta.DeletedAt = time.Unix(deletedAt.Int64, 0)
	}

	return &meta, nil
}

// GetDelayJobBody 获取任务 Body
func (s *SQLiteStorage) GetDelayJobBody(ctx context.Context, id uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var body []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT body FROM delayJob_body WHERE id = ?
	`, id).Scan(&body)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query delayJob_body: %w", err)
	}

	return body, nil
}

// DeleteDelayJob 删除任务（元数据 + Body）
func (s *SQLiteStorage) DeleteDelayJob(ctx context.Context, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 删除元数据（CASCADE 会自动删除 body）
	result, err := tx.ExecContext(ctx, "DELETE FROM delayJob_meta WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete delayJob_meta: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// ScanDelayJobMeta 扫描任务元数据
func (s *SQLiteStorage) ScanDelayJobMeta(ctx context.Context, filter *DelayJobMetaFilter) (*DelayJobMetaList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 构建查询
	query := "SELECT id, topic, priority, state, delay, ttr, created_at, ready_at, reserved_at, buried_at, deleted_at, reserves, timeouts, releases, buries, kicks, touches FROM delayJob_meta WHERE 1=1"
	args := make([]any, 0)

	if filter != nil {
		if filter.Topic != "" {
			query += " AND topic = ?"
			args = append(args, filter.Topic)
		}
		if filter.DelayState != nil {
			query += " AND state = ?"
			args = append(args, *filter.DelayState)
		}
	}

	// 应用 Offset 和 Limit
	if filter != nil && filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit+1) // 多查一个判断 HasMore
		if filter.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, filter.Offset)
		}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query delayJob_meta: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var metas []*DelayJobMeta
	for rows.Next() {
		var meta DelayJobMeta
		var delay, ttr, createdAt, readyAt int64
		var reservedAt, buriedAt, deletedAt sql.NullInt64

		err := rows.Scan(
			&meta.ID,
			&meta.Topic,
			&meta.Priority,
			&meta.DelayState,
			&delay,
			&ttr,
			&createdAt,
			&readyAt,
			&reservedAt,
			&buriedAt,
			&deletedAt,
			&meta.Reserves,
			&meta.Timeouts,
			&meta.Releases,
			&meta.Buries,
			&meta.Kicks,
			&meta.Touches,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		// 转换时间
		meta.Delay = time.Duration(delay)
		meta.TTR = time.Duration(ttr)
		meta.CreatedAt = time.Unix(createdAt, 0)
		meta.ReadyAt = time.Unix(readyAt, 0)
		if reservedAt.Valid {
			meta.ReservedAt = time.Unix(reservedAt.Int64, 0)
		}
		if buriedAt.Valid {
			meta.BuriedAt = time.Unix(buriedAt.Int64, 0)
		}
		if deletedAt.Valid {
			meta.DeletedAt = time.Unix(deletedAt.Int64, 0)
		}

		metas = append(metas, &meta)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	// 判断 HasMore
	hasMore := false
	var nextCursor uint64
	if filter != nil && filter.Limit > 0 && len(metas) > filter.Limit {
		hasMore = true
		nextCursor = metas[filter.Limit-1].ID
		metas = metas[:filter.Limit]
	}

	// 统计总数（可选）
	total := len(metas)

	return &DelayJobMetaList{
		Metas:      metas,
		Total:      total,
		HasMore:    hasMore,
		NextCursor: nextCursor,
	}, nil
}

// CountDelayJobs 统计任务数量
func (s *SQLiteStorage) CountDelayJobs(ctx context.Context, filter *DelayJobMetaFilter) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := "SELECT COUNT(*) FROM delayJob_meta WHERE 1=1"
	args := make([]any, 0)

	if filter != nil {
		if filter.Topic != "" {
			query += " AND topic = ?"
			args = append(args, filter.Topic)
		}
		if filter.DelayState != nil {
			query += " AND state = ?"
			args = append(args, *filter.DelayState)
		}
	}

	var count int
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count delayJobs: %w", err)
	}

	return count, nil
}

// 批量删除接口
func (s *SQLiteStorage) BatchDeleteDelayJobs(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 构建占位符
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf("DELETE FROM delayJob_meta WHERE id IN (%s)", strings.Join(placeholders, ","))

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("batch delete delayJob_meta: %w", err)
	}

	_, err = result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	return nil
}

// Close 关闭存储
func (s *SQLiteStorage) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil // 已经关闭，直接返回
	}
	s.closed = true
	s.closeMu.Unlock()

	// 停止批量循环
	s.cancel()

	// 关闭 channel，让 goroutines 退出
	close(s.updateChan)
	close(s.saveChan)

	// 等待 goroutines 完成
	s.wg.Wait()

	// 最后刷新一次更新缓冲区（batchSaveLoop 已经处理完所有保存请求）
	s.flushUpdates()

	return s.db.Close()
}

// batchUpdateLoop 批量更新后台循环
// 优化策略：收到请求后，快速收集 channel 中所有可用的更新，然后立即刷新
func (s *SQLiteStorage) batchUpdateLoop() {
	defer s.wg.Done()

	for {
		// 等待第一个更新请求
		meta, ok := <-s.updateChan
		if !ok {
			return
		}

		// 检查 context 是否已取消
		select {
		case <-s.ctx.Done():
			// 处理这个请求后退出
			s.updateBufferMu.Lock()
			s.updateBuffer[meta.ID] = meta
			s.updateBufferMu.Unlock()
			s.flushUpdates()
			return
		default:
		}

		// 加入缓冲区
		s.updateBufferMu.Lock()
		s.updateBuffer[meta.ID] = meta
		s.updateBufferMu.Unlock()

		// 快速收集 channel 中所有可用的更新
		numAvailable := len(s.updateChan)
		if numAvailable > 0 {
			limit := numAvailable
			if len(s.updateBuffer)+numAvailable > s.maxBatchSize {
				limit = s.maxBatchSize - len(s.updateBuffer)
			}

			s.updateBufferMu.Lock()
			for range limit {
				meta2, ok := <-s.updateChan
				if !ok {
					break
				}
				s.updateBuffer[meta2.ID] = meta2
			}
			s.updateBufferMu.Unlock()
		}

		// 非阻塞继续收集（捕获在 len() 调用后新到达的请求）
	collectLoop:
		for {
			s.updateBufferMu.Lock()
			bufferSize := len(s.updateBuffer)
			s.updateBufferMu.Unlock()

			// 检查是否达到限制
			if bufferSize >= s.maxBatchSize {
				break collectLoop
			}

			select {
			case meta2, ok := <-s.updateChan:
				if !ok {
					break collectLoop
				}
				s.updateBufferMu.Lock()
				s.updateBuffer[meta2.ID] = meta2
				s.updateBufferMu.Unlock()

			case <-s.ctx.Done():
				break collectLoop

			default:
				// Channel 为空，立即刷新
				break collectLoop
			}
		}

		// 立即刷新所有收集到的更新
		s.flushUpdates()
	}
}

// batchSaveLoop 批量保存后台循环
// 优化策略：收到第一个请求后，用非阻塞方式快速收集 channel 中的所有请求，然后立即批量保存
// 这样可以在高并发时实现真正的批量，在低并发时也不会有延迟
func (s *SQLiteStorage) batchSaveLoop() {
	defer s.wg.Done()

	for {
		// 等待第一个请求（阻塞）
		req, ok := <-s.saveChan
		if !ok {
			return
		}

		// 检查 context 是否已取消
		select {
		case <-s.ctx.Done():
			// 处理这个请求后退出
			err := s.batchSaveDelayJobsInternal([]*saveDelayJobRequest{req})
			req.done <- err
			close(req.done)
			return
		default:
		}

		// 收集 channel 中所有可用的请求（非阻塞）
		buffer := []*saveDelayJobRequest{req}

		// 策略：收集 channel 中当前所有可用的请求
		// 但同时限制数量和总大小，避免：
		// 1. 单次事务过大，导致长时间锁表
		// 2. buffer 占用过多内存
		// 3. 数据库写入超时

		// 计算当前 buffer 的总大小
		totalBytes := len(req.body)

		// 先用 len() 快速读取已知的请求
		numAvailable := len(s.saveChan)
		if numAvailable > 0 {
			for range numAvailable {
				// 检查是否达到限制
				if len(buffer) >= s.maxBatchSize {
					break // 达到数量限制
				}
				if totalBytes >= s.maxBatchBytes {
					break // 达到大小限制
				}

				req2, ok := <-s.saveChan
				if !ok {
					break
				}

				totalBytes += len(req2.body)
				buffer = append(buffer, req2)
			}
		}

		// 用非阻塞方式继续收集（捕获在 len() 调用后新到达的请求）
	collectLoop:
		for {
			// 检查是否达到限制
			if len(buffer) >= s.maxBatchSize || totalBytes >= s.maxBatchBytes {
				break collectLoop
			}

			select {
			case req2, ok := <-s.saveChan:
				if !ok {
					// Channel 已关闭
					break collectLoop
				}
				totalBytes += len(req2.body)
				buffer = append(buffer, req2)

			case <-s.ctx.Done():
				// Context 取消，立即保存
				break collectLoop

			default:
				// Channel 为空，立即保存当前批次
				break collectLoop
			}
		}

		// 批量保存所有收集到的请求
		err := s.batchSaveDelayJobsInternal(buffer)
		for _, r := range buffer {
			r.done <- err
			close(r.done)
		}
	}
}

// flushUpdates 刷新缓冲区中的更新到数据库
func (s *SQLiteStorage) flushUpdates() {
	// 获取待更新的任务
	s.updateBufferMu.Lock()
	if len(s.updateBuffer) == 0 {
		s.updateBufferMu.Unlock()
		return
	}

	// 复制并清空缓冲区
	updates := make([]*DelayJobMeta, 0, len(s.updateBuffer))
	for _, meta := range s.updateBuffer {
		updates = append(updates, meta)
	}
	s.updateBuffer = make(map[uint64]*DelayJobMeta)
	s.updateBufferMu.Unlock()

	// 批量写入数据库
	if err := s.batchUpdateDelayJobMeta(updates); err != nil {
		// 记录错误但不中断（可以添加日志）
		_ = err
	}
}

// batchUpdateDelayJobMeta 批量更新任务元数据到数据库
func (s *SQLiteStorage) batchUpdateDelayJobMeta(metas []*DelayJobMeta) error {
	if len(metas) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 准备批量更新语句
	stmt, err := tx.Prepare(`
		UPDATE delayJob_meta SET
			topic = ?, priority = ?, state = ?,
			delay = ?, ttr = ?,
			created_at = ?, ready_at = ?,
			reserved_at = ?, buried_at = ?, deleted_at = ?,
			reserves = ?, timeouts = ?, releases = ?,
			buries = ?, kicks = ?, touches = ?
		WHERE id = ?
	`)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	// 批量执行
	for _, meta := range metas {
		_, err := stmt.Exec(
			meta.Topic, meta.Priority, meta.DelayState,
			int64(meta.Delay), int64(meta.TTR),
			meta.CreatedAt.Unix(), meta.ReadyAt.Unix(),
			nullableTime(meta.ReservedAt),
			nullableTime(meta.BuriedAt),
			nullableTime(meta.DeletedAt),
			meta.Reserves, meta.Timeouts, meta.Releases,
			meta.Buries, meta.Kicks, meta.Touches,
			meta.ID,
		)
		if err != nil {
			// 单个更新失败不影响其他，继续执行
			_ = err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// Stats 返回存储统计信息
func (s *SQLiteStorage) Stats(ctx context.Context) (*StorageStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := &StorageStats{}

	// 统计任务数
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM delayJob_meta").Scan(&stats.TotalDelayJobs)
	if err != nil {
		return nil, fmt.Errorf("count delayJobs: %w", err)
	}

	// 统计 topic 数
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(DISTINCT topic) FROM delayJob_meta").Scan(&stats.TotalTopics)
	if err != nil {
		return nil, fmt.Errorf("count topics: %w", err)
	}

	// 统计存储大小（估算）
	stats.MetaSize = stats.TotalDelayJobs * 200 // 每个 meta 约 200 字节

	var totalBodySize sql.NullInt64
	err = s.db.QueryRowContext(ctx, "SELECT SUM(LENGTH(body)) FROM delayJob_body").Scan(&totalBodySize)
	if err != nil {
		return nil, fmt.Errorf("sum body size: %w", err)
	}
	if totalBodySize.Valid {
		stats.BodySize = totalBodySize.Int64
	}

	stats.TotalSize = stats.MetaSize + stats.BodySize

	if stats.TotalDelayJobs > 0 {
		stats.AvgMetaSize = stats.MetaSize / stats.TotalDelayJobs
		stats.AvgBodySize = stats.BodySize / stats.TotalDelayJobs
	}

	// 当前加载的数据大小（SQLite 不在内存中）
	stats.LoadedMetaSize = 0
	stats.LoadedBodySize = 0

	return stats, nil
}

// nullableTime 转换 time.Time 为 sql.NullInt64
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.Unix()
}

// Vacuum 优化数据库（定期维护）
func (s *SQLiteStorage) Vacuum() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("VACUUM")
	return err
}

// ExportJSON 导出数据为 JSON（用于备份/调试）
func (s *SQLiteStorage) ExportJSON(ctx context.Context) ([]byte, error) {
	list, err := s.ScanDelayJobMeta(ctx, nil)
	if err != nil {
		return nil, err
	}

	return json.MarshalIndent(list.Metas, "", "  ")
}
