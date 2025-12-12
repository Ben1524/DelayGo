package delaygo

import (
	"context"
	"testing"
	"time"
)

func TestNewMemoryStorage(t *testing.T) {
	storage := NewMemoryStorage()
	if storage == nil {
		t.Fatal("NewMemoryStorage returned nil")
	}
	defer func() { _ = storage.Close() }()

	if storage.metas == nil {
		t.Error("metas map should be initialized")
	}

	if storage.bodies == nil {
		t.Error("bodies map should be initialized")
	}
}

func TestMemoryStorageSaveDelayJob(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save delayJob
	err := storage.SaveDelayJob(ctx, meta, body)
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

func TestMemoryStorageUpdateDelayJobMeta(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save delayJob first
	if err := storage.SaveDelayJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	// Update meta
	meta.DelayState = DelayStateReady
	meta.Reserves = 5

	err := storage.UpdateDelayJobMeta(ctx, meta)
	if err != nil {
		t.Fatalf("UpdateDelayJobMeta error: %v", err)
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
	err = storage.UpdateDelayJobMeta(ctx, nonExistent)
	if err != ErrNotFound {
		t.Errorf("UpdateDelayJobMeta non-existent = %v, want ErrNotFound", err)
	}

	ReleaseDelayJobMeta(meta)
	ReleaseDelayJobMeta(nonExistent)
}

func TestMemoryStorageGetDelayJobMeta(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
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

	// Should return a clone
	if gotMeta == meta {
		t.Error("GetDelayJobMeta should return a clone")
	}

	// Get non-existent
	_, err = storage.GetDelayJobMeta(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("GetDelayJobMeta non-existent = %v, want ErrNotFound", err)
	}

	ReleaseDelayJobMeta(meta)
}

func TestMemoryStorageGetDelayJobBody(t *testing.T) {
	storage := NewMemoryStorage()
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

func TestMemoryStorageDeleteDelayJob(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save delayJob
	if err := storage.SaveDelayJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	// Delete delayJob
	err := storage.DeleteDelayJob(ctx, meta.ID)
	if err != nil {
		t.Fatalf("DeleteDelayJob error: %v", err)
	}

	// Verify deletion
	_, err = storage.GetDelayJobMeta(ctx, meta.ID)
	if err != ErrNotFound {
		t.Errorf("GetDelayJobMeta after delete = %v, want ErrNotFound", err)
	}

	_, err = storage.GetDelayJobBody(ctx, meta.ID)
	if err != ErrNotFound {
		t.Errorf("GetDelayJobBody after delete = %v, want ErrNotFound", err)
	}

	// Delete non-existent
	err = storage.DeleteDelayJob(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("DeleteDelayJob non-existent = %v, want ErrNotFound", err)
	}

	ReleaseDelayJobMeta(meta)
}

func TestMemoryStorageScanDelayJobMeta(t *testing.T) {
	storage := NewMemoryStorage()
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

	if result.Total != 5 {
		t.Errorf("Total = %d, want %d", result.Total, 5)
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

func TestMemoryStorageCountDelayJobs(t *testing.T) {
	storage := NewMemoryStorage()
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

func TestMemoryStorageClose(t *testing.T) {
	storage := NewMemoryStorage()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	_ = storage.SaveDelayJob(ctx, meta, []byte("body"))

	err := storage.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// After close, maps should be nil
	if storage.metas != nil {
		t.Error("metas should be nil after close")
	}

	if storage.bodies != nil {
		t.Error("bodies should be nil after close")
	}
}

func TestMemoryStorageStats(t *testing.T) {
	storage := NewMemoryStorage()
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

	if stats.MetaSize == 0 {
		t.Error("MetaSize should be > 0")
	}

	if stats.BodySize == 0 {
		t.Error("BodySize should be > 0")
	}
}

func TestMemoryStorageConcurrency(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	done := make(chan bool)

	// Concurrent writes
	for i := range 10 {
		go func(id uint64) {
			meta := NewDelayJobMeta(id, "test", 10, 0, 30*time.Second)
			_ = storage.SaveDelayJob(ctx, meta, []byte("body"))
			done <- true
		}(uint64(i + 1))
	}

	// Wait for all writes
	for range 10 {
		<-done
	}

	// Verify count
	count, _ := storage.CountDelayJobs(ctx, nil)
	if count != 10 {
		t.Errorf("Count = %d, want %d", count, 10)
	}
}

func TestMemoryStorageBatchDeleteDelayJobs(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Create multiple jobs
	ids := []uint64{1, 2, 3, 4, 5}
	for _, id := range ids {
		meta := NewDelayJobMeta(id, "test", 10, 0, 30*time.Second)
		body := []byte("test body")
		if err := storage.SaveDelayJob(ctx, meta, body); err != nil {
			t.Fatalf("SaveDelayJob %d error: %v", id, err)
		}
		ReleaseDelayJobMeta(meta)
	}

	// Batch delete some
	deleteIDs := []uint64{1, 3, 5}
	if err := storage.BatchDeleteDelayJobs(ctx, deleteIDs); err != nil {
		t.Fatalf("BatchDeleteDelayJobs error: %v", err)
	}

	// Verify deletion
	for _, id := range deleteIDs {
		if _, err := storage.GetDelayJobMeta(ctx, id); err != ErrNotFound {
			t.Errorf("GetDelayJobMeta %d after delete = %v, want ErrNotFound", id, err)
		}
	}

	// Verify others exist
	keepIDs := []uint64{2, 4}
	for _, id := range keepIDs {
		if _, err := storage.GetDelayJobMeta(ctx, id); err != nil {
			t.Errorf("GetDelayJobMeta %d error: %v", id, err)
		}
	}
}
