package delaygo

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// StorageType 定义存储类型
type StorageType int

const (
	StorageTypeMemory StorageType = iota
	StorageTypeSQLite
)

// String 返回存储类型的字符串表示
func (st StorageType) String() string {
	switch st {
	case StorageTypeMemory:
		return "Memory"
	case StorageTypeSQLite:
		return "SQLite"
	default:
		return "Unknown"
	}
}

// TestStorage 测试存储的包装器
type TestStorage struct {
	Storage Storage
	Type    StorageType
	cleanup func()
}

// NewTestStorage 创建测试存储（根据类型）
func NewTestStorage(t *testing.T, storageType StorageType) *TestStorage {
	switch storageType {
	case StorageTypeMemory:
		return &TestStorage{
			Storage: NewMemoryStorage(),
			Type:    StorageTypeMemory,
			cleanup: func() {},
		}
	case StorageTypeSQLite:
		tmpDir, err := os.MkdirTemp("", "sdq-test-*")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}

		dbPath := filepath.Join(tmpDir, "test.db")
		t.Logf("Creating SQLite storage at: %s", dbPath)
		storage, err := NewSQLiteStorage(dbPath)
		if err != nil {
			_ = os.RemoveAll(tmpDir)
			t.Fatalf("Failed to create SQLite storage: %v", err)
		}

		return &TestStorage{
			Storage: storage,
			Type:    StorageTypeSQLite,
			cleanup: func() {
				_ = storage.Close()
				_ = os.RemoveAll(tmpDir)
			},
		}
	default:
		t.Fatalf("Unknown storage type: %v", storageType)
		return nil
	}
}

// Close 关闭存储并清理资源
func (ts *TestStorage) Close() {
	if ts.Storage != nil {
		_ = ts.Storage.Close()
	}
	if ts.cleanup != nil {
		ts.cleanup()
	}
}

// AllStorageTypes 返回所有需要测试的存储类型
func AllStorageTypes() []StorageType {
	return []StorageType{StorageTypeMemory, StorageTypeSQLite}
}

// RunWithAllStorages 使用所有存储类型运行测试函数
func RunWithAllStorages(t *testing.T, testFunc func(t *testing.T, storage *TestStorage)) {
	for _, storageType := range AllStorageTypes() {
		t.Run(storageType.String(), func(t *testing.T) {
			storage := NewTestStorage(t, storageType)
			defer storage.Close()
			testFunc(t, storage)
		})
	}
}

// NoOpTicker 是一个无操作的 Ticker 实现，用于测试
// 它不会实际执行任何定时任务，避免了 ticker 的并发死锁问题
type NoOpTicker struct{}

// NewNoOpTicker 创建一个无操作的 Ticker
func NewNoOpTicker() *NoOpTicker {
	return &NoOpTicker{}
}

// Start 启动定时器（无操作）
func (t *NoOpTicker) Start() {}

// Stop 停止定时器（无操作）
func (t *NoOpTicker) Stop() {}

// Register 注册需要 tick 的对象（无操作）
func (t *NoOpTicker) Register(name string, tickable Tickable) {}

// Unregister 取消注册（无操作）
func (t *NoOpTicker) Unregister(name string) {}

// Wakeup 唤醒定时器（无操作）
func (t *NoOpTicker) Wakeup() {}

// Stats 返回统计信息（空统计）
func (t *NoOpTicker) Stats() *TickerStats {
	return &TickerStats{
		RegisteredCount: 0,
		NextTickTime:    time.Time{},
		TimeUntilTick:   0,
		Mode:            "noop",
	}
}
