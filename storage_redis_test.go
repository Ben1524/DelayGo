package delaygo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupRedis(t *testing.T) (*RedisStorage, func()) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Use a random prefix for isolation
	prefix := fmt.Sprintf("delaygo_test_%d", time.Now().UnixNano())
	storage := NewRedisStorage(client, WithRedisPrefix(prefix))

	cleanup := func() {
		storage.Close()
		mr.Close()
	}

	return storage, cleanup
}

func TestNewRedisStorage(t *testing.T) {
	storage, cleanup := setupRedis(t)
	if storage == nil {
		return
	}
	defer cleanup()

	if storage.client == nil {
		t.Error("client should be initialized")
	}
}

func TestRedisStorageSaveDelayJob(t *testing.T) {
	storage, cleanup := setupRedis(t)
	if storage == nil {
		return
	}
	defer cleanup()

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

func TestRedisStorageUpdateDelayJobMeta(t *testing.T) {
	storage, cleanup := setupRedis(t)
	if storage == nil {
		return
	}
	defer cleanup()

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

func TestRedisStorageGetDelayJobMeta(t *testing.T) {
	storage, cleanup := setupRedis(t)
	if storage == nil {
		return
	}
	defer cleanup()

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

	// Get non-existent meta
	_, err = storage.GetDelayJobMeta(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("GetDelayJobMeta non-existent = %v, want ErrNotFound", err)
	}

	ReleaseDelayJobMeta(meta)
}

func TestRedisStorageGetDelayJobBody(t *testing.T) {
	storage, cleanup := setupRedis(t)
	if storage == nil {
		return
	}
	defer cleanup()

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

	// Get non-existent body
	_, err = storage.GetDelayJobBody(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("GetDelayJobBody non-existent = %v, want ErrNotFound", err)
	}

	ReleaseDelayJobMeta(meta)
}

func TestRedisStorageDeleteDelayJob(t *testing.T) {
	storage, cleanup := setupRedis(t)
	if storage == nil {
		return
	}
	defer cleanup()

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

	// Delete non-existent delayJob
	err = storage.DeleteDelayJob(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("DeleteDelayJob non-existent = %v, want ErrNotFound", err)
	}

	ReleaseDelayJobMeta(meta)
}

func TestRedisStorageScanDelayJobMeta(t *testing.T) {
	storage, cleanup := setupRedis(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()

	// Create multiple jobs
	for i := 1; i <= 10; i++ {
		meta := NewDelayJobMeta(uint64(i), "test", 10, 0, 30*time.Second)
		if i%2 == 0 {
			meta.DelayState = DelayStateReady
		}
		body := []byte(fmt.Sprintf("body %d", i))
		if err := storage.SaveDelayJob(ctx, meta, body); err != nil {
			t.Fatalf("SaveDelayJob %d error: %v", i, err)
		}
		ReleaseDelayJobMeta(meta)
	}

	// Scan all
	list, err := storage.ScanDelayJobMeta(ctx, nil)
	if err != nil {
		t.Fatalf("ScanDelayJobMeta all error: %v", err)
	}
	if len(list.Metas) != 10 {
		t.Errorf("Scan all count = %d, want 10", len(list.Metas))
	}

	// Scan with limit
	filter := &DelayJobMetaFilter{Limit: 5}
	list, err = storage.ScanDelayJobMeta(ctx, filter)
	if err != nil {
		t.Fatalf("ScanDelayJobMeta limit error: %v", err)
	}
	if len(list.Metas) != 5 {
		t.Errorf("Scan limit count = %d, want 5", len(list.Metas))
	}
	if !list.HasMore {
		t.Error("Scan limit HasMore should be true")
	}

	// Scan with state filter
	state := DelayStateReady
	filter = &DelayJobMetaFilter{DelayState: &state}
	list, err = storage.ScanDelayJobMeta(ctx, filter)
	if err != nil {
		t.Fatalf("ScanDelayJobMeta state error: %v", err)
	}
	if len(list.Metas) != 5 {
		t.Errorf("Scan state count = %d, want 5", len(list.Metas))
	}
	for _, m := range list.Metas {
		if m.DelayState != DelayStateReady {
			t.Errorf("Scan state got %v, want %v", m.DelayState, DelayStateReady)
		}
	}
}

func TestRedisStorageCountDelayJobs(t *testing.T) {
	storage, cleanup := setupRedis(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()

	// Create jobs with different states
	// 3 Ready, 2 Delayed, 1 Buried
	states := []DelayState{
		DelayStateReady, DelayStateReady, DelayStateReady,
		DelayStateDelayed, DelayStateDelayed,
		DelayStateBuried,
	}

	for i, state := range states {
		meta := NewDelayJobMeta(uint64(i+1), "test", 10, 0, 30*time.Second)
		meta.DelayState = state
		body := []byte(fmt.Sprintf("body %d", i+1))
		if err := storage.SaveDelayJob(ctx, meta, body); err != nil {
			t.Fatalf("SaveDelayJob %d error: %v", i+1, err)
		}
		ReleaseDelayJobMeta(meta)
	}

	// Count all
	count, err := storage.CountDelayJobs(ctx, nil)
	if err != nil {
		t.Fatalf("CountDelayJobs all error: %v", err)
	}
	if count != 6 {
		t.Errorf("Count all = %d, want 6", count)
	}

	// Count Ready
	ready := DelayStateReady
	count, err = storage.CountDelayJobs(ctx, &DelayJobMetaFilter{DelayState: &ready})
	if err != nil {
		t.Fatalf("CountDelayJobs Ready error: %v", err)
	}
	if count != 3 {
		t.Errorf("Count Ready = %d, want 3", count)
	}

	// Count Delayed
	delayed := DelayStateDelayed
	count, err = storage.CountDelayJobs(ctx, &DelayJobMetaFilter{DelayState: &delayed})
	if err != nil {
		t.Fatalf("CountDelayJobs Delayed error: %v", err)
	}
	if count != 2 {
		t.Errorf("Count Delayed = %d, want 2", count)
	}

	// Count Buried
	buried := DelayStateBuried
	count, err = storage.CountDelayJobs(ctx, &DelayJobMetaFilter{DelayState: &buried})
	if err != nil {
		t.Fatalf("CountDelayJobs Buried error: %v", err)
	}
	if count != 1 {
		t.Errorf("Count Buried = %d, want 1", count)
	}
}

func TestRedisStorageBatchDeleteDelayJobs(t *testing.T) {
	storage, cleanup := setupRedis(t)
	if storage == nil {
		return
	}
	defer cleanup()

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

func TestRedisStorageSaveDelayJobs(t *testing.T) {
	storage, cleanup := setupRedis(t)
	if storage == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()

	var metas []*DelayJobMeta
	var bodies [][]byte
	for i := 1; i <= 5; i++ {
		meta := NewDelayJobMeta(uint64(i), "test", 10, 0, 30*time.Second)
		metas = append(metas, meta)
		bodies = append(bodies, []byte(fmt.Sprintf("body %d", i)))
	}

	// Save delayJobs
	err := storage.SaveDelayJobs(ctx, metas, bodies)
	if err != nil {
		t.Fatalf("SaveDelayJobs error: %v", err)
	}

	// Verify
	for i, meta := range metas {
		gotMeta, err := storage.GetDelayJobMeta(ctx, meta.ID)
		if err != nil {
			t.Errorf("GetDelayJobMeta %d error: %v", meta.ID, err)
		}
		if gotMeta.ID != meta.ID {
			t.Errorf("ID = %d, want %d", gotMeta.ID, meta.ID)
		}

		gotBody, err := storage.GetDelayJobBody(ctx, meta.ID)
		if err != nil {
			t.Errorf("GetDelayJobBody %d error: %v", meta.ID, err)
		}
		if string(gotBody) != string(bodies[i]) {
			t.Errorf("Body = %s, want %s", gotBody, bodies[i])
		}
	}

	// Test duplicate
	err = storage.SaveDelayJobs(ctx, metas, bodies)
	if err != ErrDelayJobExists {
		t.Errorf("SaveDelayJobs duplicate = %v, want ErrDelayJobExists", err)
	}
}
