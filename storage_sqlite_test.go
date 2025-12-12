package delaygo

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNewSQLiteStorage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	if storage.db == nil {
		t.Error("db should not be nil")
	}

	// 验证默认配置
	if storage.maxBatchSize != 1000 {
		t.Errorf("Expected maxBatchSize=1000, got %d", storage.maxBatchSize)
	}
	if storage.maxBatchBytes != 16*1024*1024 {
		t.Errorf("Expected maxBatchBytes=16MB, got %d", storage.maxBatchBytes)
	}
}

func TestNewSQLiteStorageWithOptions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath,
		WithMaxBatchSize(500),
		WithMaxBatchBytes(8*1024*1024),
	)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	// 验证自定义配置
	if storage.maxBatchSize != 500 {
		t.Errorf("Expected maxBatchSize=500, got %d", storage.maxBatchSize)
	}
	if storage.maxBatchBytes != 8*1024*1024 {
		t.Errorf("Expected maxBatchBytes=8MB, got %d", storage.maxBatchBytes)
	}
}

func TestSQLiteStorageSaveDelayJob(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 5*time.Second, 30*time.Second)
	body := []byte("test body")

	// Save delayJob
	err = storage.SaveDelayJob(ctx, meta, body)
	if err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	// Try to save duplicate
	err = storage.SaveDelayJob(ctx, meta, body)
	if err != ErrDelayJobExists {
		t.Errorf("SaveDelayJob duplicate = %v, want ErrDelayJobExists", err)
	}

	ReleaseDelayJobMeta(meta)
}

// TestSQLiteStorage_ClosedOperation 测试关闭后操作
func TestSQLiteStorage_ClosedOperation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)

	// 保存任务
	err = storage.SaveDelayJob(ctx, meta, []byte("body"))
	if err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	// 关闭存储
	err = storage.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// 关闭后的操作应该返回错误（context canceled）
	err = storage.SaveDelayJob(ctx, meta, []byte("body"))
	if err == nil {
		t.Error("SaveDelayJob after Close should return error")
	}

	ReleaseDelayJobMeta(meta)
}

// TestSQLiteStorage_ConcurrentWrites 测试并发写入
func TestSQLiteStorage_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	const numGoroutines = 10
	const delayJobsPerGoroutine = 100

	// 并发写入任务
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < delayJobsPerGoroutine; j++ {
				id := uint64(goroutineID*delayJobsPerGoroutine + j + 1)
				meta := NewDelayJobMeta(id, "test", 10, 0, 30*time.Second)
				err := storage.SaveDelayJob(ctx, meta, []byte(fmt.Sprintf("body-%d", id)))
				if err != nil {
					t.Errorf("SaveDelayJob error: %v", err)
				}
				ReleaseDelayJobMeta(meta)
			}
		}(i)
	}

	wg.Wait()

	// 验证所有任务都已保存
	result, err := storage.ScanDelayJobMeta(ctx, nil)
	if err != nil {
		t.Fatalf("ScanDelayJobMeta error: %v", err)
	}

	expectedCount := numGoroutines * delayJobsPerGoroutine
	if len(result.Metas) != expectedCount {
		t.Errorf("Saved delayJobs = %d, want %d", len(result.Metas), expectedCount)
	}
}

// TestSQLiteStorage_LargeBody 测试大任务体
func TestSQLiteStorage_LargeBody(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)

	// 创建 1MB 的任务体
	largeBody := make([]byte, 1024*1024)
	for i := range largeBody {
		largeBody[i] = byte(i % 256)
	}

	// 保存大任务
	err = storage.SaveDelayJob(ctx, meta, largeBody)
	if err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	// 读取并验证
	gotBody, err := storage.GetDelayJobBody(ctx, 1)
	if err != nil {
		t.Fatalf("GetDelayJobBody error: %v", err)
	}

	if len(gotBody) != len(largeBody) {
		t.Errorf("Body length = %d, want %d", len(gotBody), len(largeBody))
	}

	// 验证内容
	for i := range largeBody {
		if gotBody[i] != largeBody[i] {
			t.Errorf("Body mismatch at index %d: got %d, want %d", i, gotBody[i], largeBody[i])
			break
		}
	}

	ReleaseDelayJobMeta(meta)
}

// TestSQLiteStorage_BatchOperations 测试批量操作
func TestSQLiteStorage_BatchOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath,
		WithMaxBatchSize(100),
		WithMaxBatchBytes(1024*1024),
	)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// 快速保存大量任务（触发批量写入）
	const numDelayJobs = 1000
	for i := 1; i <= numDelayJobs; i++ {
		meta := NewDelayJobMeta(uint64(i), "test", 10, 0, 30*time.Second)
		err := storage.SaveDelayJob(ctx, meta, []byte(fmt.Sprintf("body-%d", i)))
		if err != nil {
			t.Fatalf("SaveDelayJob %d error: %v", i, err)
		}
		ReleaseDelayJobMeta(meta)
	}

	// 等待批量写入完成
	time.Sleep(200 * time.Millisecond)

	// 验证所有任务都已保存
	result, err := storage.ScanDelayJobMeta(ctx, nil)
	if err != nil {
		t.Fatalf("ScanDelayJobMeta error: %v", err)
	}

	if len(result.Metas) != numDelayJobs {
		t.Errorf("Saved delayJobs = %d, want %d", len(result.Metas), numDelayJobs)
	}
}

// TestSQLiteStorage_InvalidID 测试无效 ID
func TestSQLiteStorage_InvalidID(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// 获取不存在的任务元数据
	_, err = storage.GetDelayJobMeta(ctx, 99999)
	if err == nil {
		t.Error("GetDelayJobMeta with invalid ID should return error")
	}

	// 获取不存在的任务体
	_, err = storage.GetDelayJobBody(ctx, 99999)
	if err == nil {
		t.Error("GetDelayJobBody with invalid ID should return error")
	}

	// 删除不存在的任务（应该返回 ErrNotFound）
	err = storage.DeleteDelayJob(ctx, 99999)
	if err != ErrNotFound {
		t.Errorf("DeleteDelayJob with invalid ID = %v, want ErrNotFound", err)
	}
}

func TestSQLiteStorageUpdateDelayJobMetaSync(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save delayJob first
	if err := storage.SaveDelayJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	// Update meta synchronously
	meta.DelayState = DelayStateReady
	meta.Reserves = 5

	err = storage.UpdateDelayJobMetaSync(ctx, meta)
	if err != nil {
		t.Fatalf("UpdateDelayJobMetaSync error: %v", err)
	}

	// Verify update
	gotMeta, err := storage.GetDelayJobMeta(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetDelayJobMeta error: %v", err)
	}

	if gotMeta.DelayState != DelayStateReady {
		t.Errorf("DelayState = %v, want %v", gotMeta.DelayState, DelayStateReady)
	}

	if gotMeta.Reserves != 5 {
		t.Errorf("Reserves = %d, want %d", gotMeta.Reserves, 5)
	}

	// Update non-existent delayJob
	nonExistent := NewDelayJobMeta(999, "test", 10, 0, 30*time.Second)
	err = storage.UpdateDelayJobMetaSync(ctx, nonExistent)
	if err != ErrNotFound {
		t.Errorf("UpdateDelayJobMetaSync non-existent = %v, want ErrNotFound", err)
	}

	ReleaseDelayJobMeta(meta)
	ReleaseDelayJobMeta(nonExistent)
}

func TestSQLiteStorageGetDelayJobMeta(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 5*time.Second, 30*time.Second)
	body := []byte("test body")

	// Save delayJob
	if err := storage.SaveDelayJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	// Get meta
	gotMeta, err := storage.GetDelayJobMeta(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetDelayJobMeta error: %v", err)
	}

	if gotMeta.ID != meta.ID {
		t.Errorf("ID = %d, want %d", gotMeta.ID, meta.ID)
	}

	if gotMeta.Topic != meta.Topic {
		t.Errorf("Topic = %s, want %s", gotMeta.Topic, meta.Topic)
	}

	if gotMeta.Priority != meta.Priority {
		t.Errorf("Priority = %d, want %d", gotMeta.Priority, meta.Priority)
	}

	// Get non-existent
	_, err = storage.GetDelayJobMeta(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("GetDelayJobMeta non-existent = %v, want ErrNotFound", err)
	}

	ReleaseDelayJobMeta(meta)
}

// 测试批量删除任务
func TestSQLiteStorageBatchDeleteDelayJobs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
		defer func() { _ = os.RemoveAll(tmpDir) }()
	}
	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()
	ctx := context.Background()
	// Save multiple delayJobs
	var ids []uint64
	for i := uint64(1); i <= 500; i++ {
		meta := NewDelayJobMeta(i, "test", 10, 0, 30*time.Second)
		if err := storage.SaveDelayJob(ctx, meta, []byte("body")); err != nil {
			t.Fatalf("SaveDelayJob error: %v", err)
		}
		ids = append(ids, i)
	}

	err = storage.BatchDeleteDelayJobs(ctx, ids)
	if err != nil {
		t.Fatalf("BatchDeleteDelayJobs error: %v", err)
	}

	// Verify deletion
	for _, id := range ids {
		_, err := storage.GetDelayJobMeta(ctx, id)
		if err != ErrNotFound {
			t.Errorf("GetDelayJobMeta after batch delete = %v, want ErrNotFound", err)
		}
	}
}

func TestSQLiteStorageGetDelayJobBody(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save delayJob
	if err := storage.SaveDelayJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	// Get body
	gotBody, err := storage.GetDelayJobBody(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetDelayJobBody error: %v", err)
	}

	if string(gotBody) != string(body) {
		t.Errorf("Body = %s, want %s", gotBody, body)
	}

	// Get non-existent
	_, err = storage.GetDelayJobBody(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("GetDelayJobBody non-existent = %v, want ErrNotFound", err)
	}

	ReleaseDelayJobMeta(meta)
}

func TestSQLiteStorageDeleteDelayJob(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save delayJob
	if err := storage.SaveDelayJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	// Delete delayJob
	err = storage.DeleteDelayJob(ctx, meta.ID)
	if err != nil {
		t.Fatalf("DeleteDelayJob error: %v", err)
	}

	// Verify deletion
	_, err = storage.GetDelayJobMeta(ctx, meta.ID)
	if err != ErrNotFound {
		t.Errorf("GetDelayJobMeta after delete = %v, want ErrNotFound", err)
	}

	// Delete non-existent
	err = storage.DeleteDelayJob(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("DeleteDelayJob non-existent = %v, want ErrNotFound", err)
	}

	ReleaseDelayJobMeta(meta)
}

func TestSQLiteStorageScanDelayJobMeta(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save multiple delayJobs
	for i := uint64(1); i <= 5; i++ {
		meta := NewDelayJobMeta(i, "test", uint32(i), 0, 30*time.Second)
		meta.DelayState = DelayStateReady
		if err := storage.SaveDelayJob(ctx, meta, []byte("body")); err != nil {
			t.Fatalf("SaveDelayJob error: %v", err)
		}
	}

	// Scan all
	result, err := storage.ScanDelayJobMeta(ctx, nil)
	if err != nil {
		t.Fatalf("ScanDelayJobMeta error: %v", err)
	}

	if len(result.Metas) != 5 {
		t.Errorf("Metas count = %d, want %d", len(result.Metas), 5)
	}

	// Scan with filter by topic
	filter := &DelayJobMetaFilter{Topic: "test"}
	result, err = storage.ScanDelayJobMeta(ctx, filter)
	if err != nil {
		t.Fatalf("ScanDelayJobMeta with filter error: %v", err)
	}

	if len(result.Metas) != 5 {
		t.Errorf("Metas count with topic filter = %d, want %d", len(result.Metas), 5)
	}

	// Scan with filter by state
	state := DelayStateReady
	filter = &DelayJobMetaFilter{DelayState: &state}
	result, err = storage.ScanDelayJobMeta(ctx, filter)
	if err != nil {
		t.Fatalf("ScanDelayJobMeta with state filter error: %v", err)
	}

	if len(result.Metas) != 5 {
		t.Errorf("Metas count with state filter = %d, want %d", len(result.Metas), 5)
	}

	// Scan with limit
	filter = &DelayJobMetaFilter{Limit: 2}
	result, err = storage.ScanDelayJobMeta(ctx, filter)
	if err != nil {
		t.Fatalf("ScanDelayJobMeta with limit error: %v", err)
	}

	if len(result.Metas) != 2 {
		t.Errorf("Metas count with limit = %d, want %d", len(result.Metas), 2)
	}

	if !result.HasMore {
		t.Error("HasMore should be true")
	}
}

func TestSQLiteStorageCountDelayJobs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save multiple delayJobs with different states
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateReady
	meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.DelayState = DelayStateDelayed
	meta3 := NewDelayJobMeta(3, "other", 10, 0, 30*time.Second)
	meta3.DelayState = DelayStateReady

	_ = storage.SaveDelayJob(ctx, meta1, []byte("body"))
	_ = storage.SaveDelayJob(ctx, meta2, []byte("body"))
	_ = storage.SaveDelayJob(ctx, meta3, []byte("body"))

	// Count all
	count, err := storage.CountDelayJobs(ctx, nil)
	if err != nil {
		t.Fatalf("CountDelayJobs error: %v", err)
	}

	if count != 3 {
		t.Errorf("Count = %d, want %d", count, 3)
	}

	// Count by topic
	filter := &DelayJobMetaFilter{Topic: "test"}
	count, err = storage.CountDelayJobs(ctx, filter)
	if err != nil {
		t.Fatalf("CountDelayJobs with topic filter error: %v", err)
	}

	if count != 2 {
		t.Errorf("Count with topic filter = %d, want %d", count, 2)
	}

	// Count by state
	state := DelayStateReady
	filter = &DelayJobMetaFilter{DelayState: &state}
	count, err = storage.CountDelayJobs(ctx, filter)
	if err != nil {
		t.Fatalf("CountDelayJobs with state filter error: %v", err)
	}

	if count != 2 {
		t.Errorf("Count with state filter = %d, want %d", count, 2)
	}
}

func TestSQLiteStorageStats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save delayJobs in different topics
	meta1 := NewDelayJobMeta(1, "topic1", 10, 0, 30*time.Second)
	meta2 := NewDelayJobMeta(2, "topic2", 10, 0, 30*time.Second)

	_ = storage.SaveDelayJob(ctx, meta1, []byte("body1"))
	_ = storage.SaveDelayJob(ctx, meta2, []byte("longer body 2"))

	stats, err := storage.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats error: %v", err)
	}

	if stats.TotalDelayJobs != 2 {
		t.Errorf("TotalDelayJobs = %d, want %d", stats.TotalDelayJobs, 2)
	}

	if stats.TotalTopics != 2 {
		t.Errorf("TotalTopics = %d, want %d", stats.TotalTopics, 2)
	}
}

func TestSQLiteStorageVacuum(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	// Should not error
	err = storage.Vacuum()
	if err != nil {
		t.Errorf("Vacuum error: %v", err)
	}
}

func TestSQLiteStorageExportJSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save a delayJob
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	_ = storage.SaveDelayJob(ctx, meta, []byte("body"))

	// Export
	data, err := storage.ExportJSON(ctx)
	if err != nil {
		t.Fatalf("ExportJSON error: %v", err)
	}

	if len(data) == 0 {
		t.Error("ExportJSON should return data")
	}
}

func TestSQLiteStorageAsyncUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delaygo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save delayJob first
	if err := storage.SaveDelayJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	// Update meta asynchronously
	meta.DelayState = DelayStateReserved
	meta.Reserves = 1

	err = storage.UpdateDelayJobMeta(ctx, meta)
	if err != nil {
		t.Fatalf("UpdateDelayJobMeta error: %v", err)
	}

	// Wait for async flush
	time.Sleep(200 * time.Millisecond)

	// Verify update
	gotMeta, err := storage.GetDelayJobMeta(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetDelayJobMeta error: %v", err)
	}

	if gotMeta.DelayState != DelayStateReserved {
		t.Errorf("DelayState = %v, want %v", gotMeta.DelayState, DelayStateReserved)
	}

	ReleaseDelayJobMeta(meta)
}
