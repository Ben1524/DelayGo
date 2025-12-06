package delaygo

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// mysqlSaveDelayJobRequest SaveDelayJob 请求
type mysqlSaveDelayJobRequest struct {
	meta *DelayJobMeta
	body []byte
	done chan error // 用于通知调用者操作完成
}

// MySQLStorage MySQL 存储实现
// 内置批量更新和批量保存缓冲机制，自动合并并发调用
type MySQLStorage struct {
	db  *sql.DB
	dsn string
	mu  sync.RWMutex

	// 批量更新缓冲机制（用于 UpdateDelayJobMeta）
	updateBuffer   map[uint64]*DelayJobMeta // 待更新的任务元数据缓冲
	updateBufferMu sync.Mutex
	updateChan     chan *DelayJobMeta // 更新通道

	// 批量保存缓冲机制（用于 SaveDelayJob）
	// SaveDelayJob 是同步的，但内部会自动合并并发请求
	saveChan chan *mysqlSaveDelayJobRequest // 保存请求通道

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closed  bool       // 标记是否已关闭
	closeMu sync.Mutex // 保护 closed 字段

	// 批量操作配置
	maxBatchSize  int // 批量操作最大数量，默认 1000
	maxBatchBytes int // 批量保存最大字节数，默认 16MB
}

// MySQLStorageOption MySQL 存储配置选项
type MySQLStorageOption func(*MySQLStorage)

// WithMySQLMaxBatchSize 设置批量操作最大数量
func WithMySQLMaxBatchSize(size int) MySQLStorageOption {
	return func(s *MySQLStorage) {
		if size > 0 {
			s.maxBatchSize = size
		}
	}
}

// WithMySQLMaxBatchBytes 设置批量保存最大字节数
func WithMySQLMaxBatchBytes(bytes int) MySQLStorageOption {
	return func(s *MySQLStorage) {
		if bytes > 0 {
			s.maxBatchBytes = bytes
		}
	}
}

// NewMySQLStorage 创建 MySQL 存储
// dsn 格式: user:password@tcp(host:port)/dbname?charset=utf8mb4&parseTime=True&loc=Local
func NewMySQLStorage(dsn string, opts ...MySQLStorageOption) (*MySQLStorage, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	storage := &MySQLStorage{
		db:            db,
		dsn:           dsn,
		updateBuffer:  make(map[uint64]*DelayJobMeta),
		updateChan:    make(chan *DelayJobMeta, 1000),             // 缓冲 1000 个更新请求
		saveChan:      make(chan *mysqlSaveDelayJobRequest, 1000), // 缓冲 1000 个保存请求
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

	// 设置连接池
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	// 启动批量更新后台任务
	storage.wg.Add(1)
	go storage.batchUpdateLoop()

	// 启动批量保存后台任务
	storage.wg.Add(1)
	go storage.batchSaveLoop()

	return storage, nil
}

// initTables 初始化数据库表
func (s *MySQLStorage) initTables() error {
	// 创建 delay_job_meta 表
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS delay_job_meta (
			id BIGINT UNSIGNED PRIMARY KEY,
			topic VARCHAR(255) NOT NULL,
			priority INT UNSIGNED NOT NULL,
			state INT NOT NULL,
			delay BIGINT NOT NULL,
			ttr BIGINT NOT NULL,
			created_at BIGINT NOT NULL,
			ready_at BIGINT NOT NULL,
			reserved_at BIGINT,
			buried_at BIGINT,
			deleted_at BIGINT,
			reserves INT DEFAULT 0,
			timeouts INT DEFAULT 0,
			releases INT DEFAULT 0,
			buries INT DEFAULT 0,
			kicks INT DEFAULT 0,
			touches INT DEFAULT 0,
			total_touch_time BIGINT DEFAULT 0,
			INDEX idx_topic (topic),
			INDEX idx_state (state)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	if err != nil {
		return fmt.Errorf("create delay_job_meta table: %w", err)
	}

	// 创建 delay_job_body 表
	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS delay_job_body (
			id BIGINT UNSIGNED PRIMARY KEY,
			body LONGBLOB NOT NULL,
			CONSTRAINT fk_delay_job_body_meta FOREIGN KEY (id) REFERENCES delay_job_meta(id) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	if err != nil {
		return fmt.Errorf("create delay_job_body table: %w", err)
	}

	return nil
}

// SaveDelayJob 保存任务（元数据 + Body）
// 内部会自动合并并发请求，提升批量写入性能
func (s *MySQLStorage) SaveDelayJob(ctx context.Context, meta *DelayJobMeta, body []byte) error {
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

	req := &mysqlSaveDelayJobRequest{
		meta: metaCopy,
		body: bodyCopy,
		done: make(chan error, 1),
	}

	// 发送到保存通道
	select {
	case s.saveChan <- req:
		// 等待处理完成
		select {
		case err := <-req.done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// UpdateDelayJobMeta 更新任务元数据
// 异步批量更新，高吞吐量
func (s *MySQLStorage) UpdateDelayJobMeta(ctx context.Context, meta *DelayJobMeta) error {
	// 检查是否已关闭
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return ErrStorageClosed
	}
	s.closeMu.Unlock()

	// 克隆元数据，避免外部修改
	metaCopy := meta.Clone()

	// 发送到更新通道
	select {
	case s.updateChan <- metaCopy:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// GetDelayJobMeta 获取任务元数据
func (s *MySQLStorage) GetDelayJobMeta(ctx context.Context, id uint64) (*DelayJobMeta, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, topic, priority, state, delay, ttr, 
		       created_at, ready_at, reserved_at, buried_at, deleted_at,
		       reserves, timeouts, releases, buries, kicks, touches, total_touch_time
		FROM delay_job_meta WHERE id = ?
	`, id)

	meta := &DelayJobMeta{}
	var createdAt, readyAt, reservedAt, buriedAt, deletedAt sql.NullInt64
	var delay, ttr, totalTouchTime int64

	err := row.Scan(
		&meta.ID, &meta.Topic, &meta.Priority, &meta.DelayState, &delay, &ttr,
		&createdAt, &readyAt, &reservedAt, &buriedAt, &deletedAt,
		&meta.Reserves, &meta.Timeouts, &meta.Releases, &meta.Buries, &meta.Kicks, &meta.Touches, &totalTouchTime,
	)

	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan delay_job_meta: %w", err)
	}

	// 转换时间类型
	meta.Delay = time.Duration(delay)
	meta.TTR = time.Duration(ttr)
	meta.TotalTouchTime = time.Duration(totalTouchTime)

	if createdAt.Valid {
		meta.CreatedAt = time.Unix(0, createdAt.Int64)
	}
	if readyAt.Valid {
		meta.ReadyAt = time.Unix(0, readyAt.Int64)
	}
	if reservedAt.Valid {
		meta.ReservedAt = time.Unix(0, reservedAt.Int64)
	}
	if buriedAt.Valid {
		meta.BuriedAt = time.Unix(0, buriedAt.Int64)
	}
	if deletedAt.Valid {
		meta.DeletedAt = time.Unix(0, deletedAt.Int64)
	}

	return meta, nil
}

// ScanDelayJobMeta 扫描任务元数据
func (s *MySQLStorage) ScanDelayJobMeta(ctx context.Context, filter *DelayJobMetaFilter) (*DelayJobMetaList, error) {
	query := `
		SELECT id, topic, priority, state, delay, ttr, 
		       created_at, ready_at, reserved_at, buried_at, deleted_at,
		       reserves, timeouts, releases, buries, kicks, touches, total_touch_time
		FROM delay_job_meta
	`
	var args []interface{}
	var conditions []string

	if filter != nil {
		if filter.Topic != "" {
			conditions = append(conditions, "topic = ?")
			args = append(args, filter.Topic)
		}
		if filter.DelayState != nil {
			conditions = append(conditions, "state = ?")
			args = append(args, *filter.DelayState)
		}
		if filter.Cursor > 0 {
			conditions = append(conditions, "id > ?")
			args = append(args, filter.Cursor)
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY id ASC"

	if filter != nil && filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query delay_job_meta: %w", err)
	}
	defer rows.Close()

	var metas []*DelayJobMeta
	var lastID uint64

	for rows.Next() {
		meta := &DelayJobMeta{}
		var createdAt, readyAt, reservedAt, buriedAt, deletedAt sql.NullInt64
		var delay, ttr, totalTouchTime int64

		err := rows.Scan(
			&meta.ID, &meta.Topic, &meta.Priority, &meta.DelayState, &delay, &ttr,
			&createdAt, &readyAt, &reservedAt, &buriedAt, &deletedAt,
			&meta.Reserves, &meta.Timeouts, &meta.Releases, &meta.Buries, &meta.Kicks, &meta.Touches, &totalTouchTime,
		)
		if err != nil {
			return nil, fmt.Errorf("scan delay_job_meta row: %w", err)
		}

		// 转换时间类型
		meta.Delay = time.Duration(delay)
		meta.TTR = time.Duration(ttr)
		meta.TotalTouchTime = time.Duration(totalTouchTime)

		if createdAt.Valid {
			meta.CreatedAt = time.Unix(0, createdAt.Int64)
		}
		if readyAt.Valid {
			meta.ReadyAt = time.Unix(0, readyAt.Int64)
		}
		if reservedAt.Valid {
			meta.ReservedAt = time.Unix(0, reservedAt.Int64)
		}
		if buriedAt.Valid {
			meta.BuriedAt = time.Unix(0, buriedAt.Int64)
		}
		if deletedAt.Valid {
			meta.DeletedAt = time.Unix(0, deletedAt.Int64)
		}

		metas = append(metas, meta)
		lastID = meta.ID
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delay_job_meta rows: %w", err)
	}

	return &DelayJobMetaList{
		Metas:      metas,
		HasMore:    filter != nil && filter.Limit > 0 && len(metas) == filter.Limit,
		NextCursor: lastID,
	}, nil
}

// GetDelayJobBody 获取任务 Body
func (s *MySQLStorage) GetDelayJobBody(ctx context.Context, id uint64) ([]byte, error) {
	var body []byte
	err := s.db.QueryRowContext(ctx, "SELECT body FROM delay_job_body WHERE id = ?", id).Scan(&body)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query delay_job_body: %w", err)
	}
	return body, nil
}

// DeleteDelayJob 删除任务（元数据 + Body）
func (s *MySQLStorage) DeleteDelayJob(ctx context.Context, id uint64) error {
	// 由于设置了 ON DELETE CASCADE，删除 meta 会自动删除 body
	res, err := s.db.ExecContext(ctx, "DELETE FROM delay_job_meta WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete delay_job_meta: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	if rows == 0 {
		return ErrNotFound
	}

	return nil
}

// CountDelayJobs 统计任务数量
func (s *MySQLStorage) CountDelayJobs(ctx context.Context, filter *DelayJobMetaFilter) (int, error) {
	query := "SELECT COUNT(*) FROM delay_job_meta"
	var args []interface{}
	var conditions []string

	if filter != nil {
		if filter.Topic != "" {
			conditions = append(conditions, "topic = ?")
			args = append(args, filter.Topic)
		}
		if filter.DelayState != nil {
			conditions = append(conditions, "state = ?")
			args = append(args, *filter.DelayState)
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	var count int
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count delay_job_meta: %w", err)
	}

	return count, nil
}

// GetMaxDelayJobID 获取最大任务 ID
func (s *MySQLStorage) GetMaxDelayJobID(ctx context.Context) (uint64, error) {
	var maxID sql.NullInt64
	err := s.db.QueryRowContext(ctx, "SELECT MAX(id) FROM delay_job_meta").Scan(&maxID)
	if err != nil {
		return 0, fmt.Errorf("query max id: %w", err)
	}

	if !maxID.Valid {
		return 0, nil
	}

	return uint64(maxID.Int64), nil
}

// Close 关闭存储
func (s *MySQLStorage) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	s.closeMu.Unlock()

	// 取消上下文，通知后台任务退出
	s.cancel()

	// 等待后台任务结束
	s.wg.Wait()

	// 关闭数据库连接
	return s.db.Close()
}

// batchUpdateLoop 批量更新循环
func (s *MySQLStorage) batchUpdateLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case meta := <-s.updateChan:
			s.addToUpdateBuffer(meta)
			if len(s.updateBuffer) >= s.maxBatchSize {
				s.flushUpdateBuffer()
			}
		case <-ticker.C:
			s.flushUpdateBuffer()
		case <-s.ctx.Done():
			// 处理剩余的更新
			s.flushUpdateBuffer()
			return
		}
	}
}

// addToUpdateBuffer 添加到更新缓冲
func (s *MySQLStorage) addToUpdateBuffer(meta *DelayJobMeta) {
	s.updateBufferMu.Lock()
	defer s.updateBufferMu.Unlock()
	s.updateBuffer[meta.ID] = meta
}

// flushUpdateBuffer 刷新更新缓冲
func (s *MySQLStorage) flushUpdateBuffer() {
	s.updateBufferMu.Lock()
	if len(s.updateBuffer) == 0 {
		s.updateBufferMu.Unlock()
		return
	}

	// 取出所有待更新的任务
	metas := make([]*DelayJobMeta, 0, len(s.updateBuffer))
	for _, meta := range s.updateBuffer {
		metas = append(metas, meta)
	}
	// 清空缓冲
	s.updateBuffer = make(map[uint64]*DelayJobMeta)
	s.updateBufferMu.Unlock()

	// 执行批量更新
	if err := s.doBatchUpdate(metas); err != nil {
		// 记录错误，实际生产中可能需要重试或记录日志
		fmt.Printf("batch update failed: %v\n", err)
	}
}

// doBatchUpdate 执行批量更新
func (s *MySQLStorage) doBatchUpdate(metas []*DelayJobMeta) error {
	if len(metas) == 0 {
		return nil
	}

	// 使用 INSERT ... ON DUPLICATE KEY UPDATE 语法进行批量更新
	// 这种方式比 CASE WHEN 更高效，且语法更简洁
	query := `
		INSERT INTO delay_job_meta (
			id, topic, priority, state, delay, ttr, 
			created_at, ready_at, reserved_at, buried_at, deleted_at,
			reserves, timeouts, releases, buries, kicks, touches, total_touch_time
		) VALUES 
	`

	vals := []interface{}{}
	placeholders := []string{}

	for _, meta := range metas {
		placeholders = append(placeholders, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

		vals = append(vals,
			meta.ID, meta.Topic, meta.Priority, meta.DelayState, int64(meta.Delay), int64(meta.TTR),
			meta.CreatedAt.UnixNano(), meta.ReadyAt.UnixNano(),
			sqlNullInt64(meta.ReservedAt), sqlNullInt64(meta.BuriedAt), sqlNullInt64(meta.DeletedAt),
			meta.Reserves, meta.Timeouts, meta.Releases, meta.Buries, meta.Kicks, meta.Touches, int64(meta.TotalTouchTime),
		)
	}

	query += strings.Join(placeholders, ",")
	query += `
		ON DUPLICATE KEY UPDATE
			state = VALUES(state),
			ready_at = VALUES(ready_at),
			reserved_at = VALUES(reserved_at),
			buried_at = VALUES(buried_at),
			deleted_at = VALUES(deleted_at),
			reserves = VALUES(reserves),
			timeouts = VALUES(timeouts),
			releases = VALUES(releases),
			buries = VALUES(buries),
			kicks = VALUES(kicks),
			touches = VALUES(touches),
			total_touch_time = VALUES(total_touch_time)
	`

	_, err := s.db.ExecContext(s.ctx, query, vals...)
	return err
}

// batchSaveLoop 批量保存循环
func (s *MySQLStorage) batchSaveLoop() {
	defer s.wg.Done()

	// 缓冲队列
	var batch []*mysqlSaveDelayJobRequest
	var batchBytes int

	// 刷新函数
	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.doBatchSave(batch)
		batch = make([]*mysqlSaveDelayJobRequest, 0, s.maxBatchSize)
		batchBytes = 0
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case req := <-s.saveChan:
			batch = append(batch, req)
			batchBytes += len(req.body)

			// 达到阈值则刷新
			if len(batch) >= s.maxBatchSize || batchBytes >= s.maxBatchBytes {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.ctx.Done():
			flush()
			return
		}
	}
}

// doBatchSave 执行批量保存
func (s *MySQLStorage) doBatchSave(reqs []*mysqlSaveDelayJobRequest) {
	if len(reqs) == 0 {
		return
	}

	// 开启事务
	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		for _, req := range reqs {
			req.done <- err
		}
		return
	}

	// 准备批量插入 meta
	metaQuery := `
		INSERT IGNORE INTO delay_job_meta (
			id, topic, priority, state, delay, ttr, 
			created_at, ready_at, reserved_at, buried_at, deleted_at,
			reserves, timeouts, releases, buries, kicks, touches, total_touch_time
		) VALUES 
	`
	metaVals := []interface{}{}
	metaPlaceholders := []string{}

	// 准备批量插入 body
	bodyQuery := `INSERT IGNORE INTO delay_job_body (id, body) VALUES `
	bodyVals := []interface{}{}
	bodyPlaceholders := []string{}

	for _, req := range reqs {
		meta := req.meta
		metaPlaceholders = append(metaPlaceholders, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		metaVals = append(metaVals,
			meta.ID, meta.Topic, meta.Priority, meta.DelayState, int64(meta.Delay), int64(meta.TTR),
			meta.CreatedAt.UnixNano(), meta.ReadyAt.UnixNano(),
			sqlNullInt64(meta.ReservedAt), sqlNullInt64(meta.BuriedAt), sqlNullInt64(meta.DeletedAt),
			meta.Reserves, meta.Timeouts, meta.Releases, meta.Buries, meta.Kicks, meta.Touches, int64(meta.TotalTouchTime),
		)

		if len(req.body) > 0 {
			bodyPlaceholders = append(bodyPlaceholders, "(?, ?)")
			bodyVals = append(bodyVals, meta.ID, req.body)
		}
	}

	// 执行 meta 插入
	metaQuery += strings.Join(metaPlaceholders, ",")
	if _, err := tx.ExecContext(s.ctx, metaQuery, metaVals...); err != nil {
		_ = tx.Rollback()
		for _, req := range reqs {
			req.done <- err
		}
		return
	}

	// 执行 body 插入
	if len(bodyPlaceholders) > 0 {
		bodyQuery += strings.Join(bodyPlaceholders, ",")
		if _, err := tx.ExecContext(s.ctx, bodyQuery, bodyVals...); err != nil {
			_ = tx.Rollback()
			for _, req := range reqs {
				req.done <- err
			}
			return
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		for _, req := range reqs {
			req.done <- err
		}
		return
	}

	// 通知成功
	for _, req := range reqs {
		req.done <- nil
	}
}

// sqlNullInt64 辅助函数：将 time.Time 转换为 sql.NullInt64
func sqlNullInt64(t time.Time) sql.NullInt64 {
	if t.IsZero() {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: t.UnixNano(), Valid: true}
}
