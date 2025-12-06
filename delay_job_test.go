package delaygo

import (
	"testing"
	"time"
)

func TestDelayStateString(t *testing.T) {
	tests := []struct {
		state DelayState
		want  string
	}{
		{DelayStateEnqueued, "enqueued"},
		{DelayStateReady, "ready"},
		{DelayStateDelayed, "delayed"},
		{DelayStateReserved, "reserved"},
		{DelayStateBuried, "buried"},
		{DelayState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("DelayState(%d).String() = %s, want %s", tt.state, got, tt.want)
		}
	}
}

func TestNewDelayJobMeta(t *testing.T) {
	id := uint64(1)
	topic := "test-topic"
	priority := uint32(10)
	delay := 5 * time.Second
	ttr := 30 * time.Second

	meta := NewDelayJobMeta(id, topic, priority, delay, ttr)

	if meta.ID != id {
		t.Errorf("ID = %d, want %d", meta.ID, id)
	}

	if meta.Topic != topic {
		t.Errorf("Topic = %s, want %s", meta.Topic, topic)
	}

	if meta.Priority != priority {
		t.Errorf("Priority = %d, want %d", meta.Priority, priority)
	}

	if meta.DelayState != DelayStateEnqueued {
		t.Errorf("DelayState = %v, want %v", meta.DelayState, DelayStateEnqueued)
	}

	if meta.Delay != delay {
		t.Errorf("Delay = %v, want %v", meta.Delay, delay)
	}

	if meta.TTR != ttr {
		t.Errorf("TTR = %v, want %v", meta.TTR, ttr)
	}

	if meta.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}

	// With delay, ReadyAt should be in the future
	if !meta.ReadyAt.After(meta.CreatedAt) {
		t.Error("ReadyAt should be after CreatedAt when delay > 0")
	}

	// Counters should be zero
	if meta.Reserves != 0 || meta.Timeouts != 0 || meta.Releases != 0 ||
		meta.Buries != 0 || meta.Kicks != 0 || meta.Touches != 0 {
		t.Error("Counters should be zero")
	}

	ReleaseDelayJobMeta(meta)
}

func TestNewDelayJobMetaNoDelay(t *testing.T) {
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)

	// Without delay, ReadyAt should be zero (indicating immediate ready)
	if !meta.ReadyAt.IsZero() {
		t.Errorf("ReadyAt should be zero when delay = 0, got %v", meta.ReadyAt)
	}

	ReleaseDelayJobMeta(meta)
}

func TestDelayJobMetaClone(t *testing.T) {
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateReady
	meta.Reserves = 5

	clone := meta.Clone()

	if clone == meta {
		t.Error("Clone should return a different pointer")
	}

	if clone.ID != meta.ID {
		t.Errorf("Clone.ID = %d, want %d", clone.ID, meta.ID)
	}

	if clone.DelayState != meta.DelayState {
		t.Errorf("Clone.DelayState = %v, want %v", clone.DelayState, meta.DelayState)
	}

	if clone.Reserves != meta.Reserves {
		t.Errorf("Clone.Reserves = %d, want %d", clone.Reserves, meta.Reserves)
	}

	// Modifying clone should not affect original
	clone.Reserves = 10
	if meta.Reserves == 10 {
		t.Error("Modifying clone should not affect original")
	}

	ReleaseDelayJobMeta(meta)
}

func TestDelayJobMetaShouldBeReady(t *testing.T) {
	now := time.Now()

	// Test delayed delayJob that should be ready
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateDelayed
	meta.ReadyAt = now.Add(-1 * time.Second) // Past

	if !meta.ShouldBeReady(now) {
		t.Error("Should be ready when ReadyAt is in the past")
	}

	// Test delayed delayJob that should not be ready
	meta.ReadyAt = now.Add(1 * time.Second) // Future
	if meta.ShouldBeReady(now) {
		t.Error("Should not be ready when ReadyAt is in the future")
	}

	// Test non-delayed delayJob
	meta.DelayState = DelayStateReady
	if meta.ShouldBeReady(now) {
		t.Error("ShouldBeReady should return false for non-delayed state")
	}

	ReleaseDelayJobMeta(meta)
}

func TestDelayJobMetaShouldTimeout(t *testing.T) {
	now := time.Now()

	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateReserved
	meta.ReservedAt = now.Add(-31 * time.Second) // Reserved 31 seconds ago, TTR is 30s

	if !meta.ShouldTimeout(now) {
		t.Error("Should timeout when past TTR deadline")
	}

	// Not yet timeout
	meta.ReservedAt = now.Add(-10 * time.Second) // Reserved 10 seconds ago
	if meta.ShouldTimeout(now) {
		t.Error("Should not timeout when within TTR")
	}

	// Not reserved state
	meta.DelayState = DelayStateReady
	if meta.ShouldTimeout(now) {
		t.Error("ShouldTimeout should return false for non-reserved state")
	}

	ReleaseDelayJobMeta(meta)
}

func TestDelayJobMetaReserveDeadline(t *testing.T) {
	now := time.Now()

	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateReserved
	meta.ReservedAt = now

	deadline := meta.ReserveDeadline()
	expected := now.Add(30 * time.Second)

	if !deadline.Equal(expected) {
		t.Errorf("ReserveDeadline = %v, want %v", deadline, expected)
	}

	// Non-reserved state
	meta.DelayState = DelayStateReady
	deadline = meta.ReserveDeadline()
	if !deadline.IsZero() {
		t.Error("ReserveDeadline should return zero time for non-reserved state")
	}

	ReleaseDelayJobMeta(meta)
}

func TestDelayJobMetaTimeUntilReady(t *testing.T) {
	now := time.Now()

	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateDelayed
	meta.ReadyAt = now.Add(10 * time.Second)

	duration := meta.TimeUntilReady(now)
	if duration < 9*time.Second || duration > 11*time.Second {
		t.Errorf("TimeUntilReady = %v, want ~10s", duration)
	}

	// Already ready
	meta.ReadyAt = now.Add(-1 * time.Second)
	duration = meta.TimeUntilReady(now)
	if duration != 0 {
		t.Errorf("TimeUntilReady = %v, want 0 when already past", duration)
	}

	// Non-delayed state
	meta.DelayState = DelayStateReady
	duration = meta.TimeUntilReady(now)
	if duration != 0 {
		t.Errorf("TimeUntilReady = %v, want 0 for non-delayed state", duration)
	}

	ReleaseDelayJobMeta(meta)
}

func TestDelayJobMetaTimeUntilTimeout(t *testing.T) {
	now := time.Now()

	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateReserved
	meta.ReservedAt = now

	duration := meta.TimeUntilTimeout(now)
	if duration < 29*time.Second || duration > 31*time.Second {
		t.Errorf("TimeUntilTimeout = %v, want ~30s", duration)
	}

	// Already timeout
	meta.ReservedAt = now.Add(-31 * time.Second)
	duration = meta.TimeUntilTimeout(now)
	if duration != 0 {
		t.Errorf("TimeUntilTimeout = %v, want 0 when already past", duration)
	}

	// Non-reserved state
	meta.DelayState = DelayStateReady
	duration = meta.TimeUntilTimeout(now)
	if duration != 0 {
		t.Errorf("TimeUntilTimeout = %v, want 0 for non-reserved state", duration)
	}

	ReleaseDelayJobMeta(meta)
}

func TestNewDelayJob(t *testing.T) {
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	delayJob := NewDelayJob(meta, body, nil)

	if delayJob.Meta != meta {
		t.Error("DelayJob.Meta should be the same as provided")
	}

	gotBody, err := delayJob.GetBody()
	if err != nil {
		t.Errorf("GetBody error: %v", err)
	}

	if string(gotBody) != string(body) {
		t.Errorf("Body = %s, want %s", gotBody, body)
	}

	// Test Body() convenience method
	if string(delayJob.Body()) != string(body) {
		t.Errorf("Body() = %s, want %s", delayJob.Body(), body)
	}

	ReleaseDelayJobMeta(meta)
}

func TestNewDelayJobWithStorage(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, storage *TestStorage) {
		meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)

		delayJob := NewDelayJobWithStorage(meta, storage.Storage, nil)

		// NewDelayJobWithStorage 会克隆 meta，所以比较内容而不是指针
		if delayJob.Meta.ID != meta.ID || delayJob.Meta.Topic != meta.Topic {
			t.Error("DelayJob.Meta should have the same content as provided")
		}

		// Body should be nil initially (not loaded)
		// GetBody will try to load from storage but fail since nothing was saved
		_, err := delayJob.GetBody()
		if err == nil {
			t.Error("GetBody should fail when delayJob not in storage")
		}

		ReleaseDelayJobMeta(meta)
	})
}

func TestReleaseDelayJobMeta(t *testing.T) {
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)

	// Should not panic
	ReleaseDelayJobMeta(meta)
	ReleaseDelayJobMeta(nil) // nil should be safe
}

// TestDelayJob_OperationMethods 测试 DelayJob 的操作方法
func TestDelayJob_OperationMethods(t *testing.T) {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	//config.Storage, _ = NewSQLiteStorage("D:\\GolandProjects\\delaygo\\delaygo-test-1847605274\\test.db")
	config.Ticker = NewNoOpTicker()

	q, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	// defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	// 等待恢复完成，避免竞态条件
	<-q.recoveryDone

	// 测试 Delete
	t.Run("Delete", func(t *testing.T) {
		id, _ := q.Put("test-delete", []byte("test delete"), 1, 0, 60*time.Second)
		delayJob, err := q.Reserve([]string{"test-delete"}, 1*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		if err := delayJob.Delete(); err != nil {
			t.Errorf("delayJob.Delete() failed: %v", err)
		}

		// 验证任务已删除
		_, err = q.Peek(id)
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}
	})

	// 测试 Release
	t.Run("Release", func(t *testing.T) {
		id, _ := q.Put("test-release", []byte("test release"), 1, 0, 60*time.Second)
		delayJob, err := q.Reserve([]string{"test-release"}, 1*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		if err := delayJob.Release(2, 0); err != nil {
			t.Errorf("delayJob.Release() failed: %v", err)
		}

		// 验证任务已重新入队
		meta, err := q.StatsDelayJob(id)
		if err != nil {
			t.Fatal(err)
		}
		if meta.DelayState != DelayStateReady {
			t.Errorf("expected DelayStateReady after release, got %v", meta.DelayState)
		}
		if meta.Priority != 2 {
			t.Errorf("expected priority 2, got %d", meta.Priority)
		}
		time.Sleep(1000 * time.Millisecond) // 等待异步更新完成
		if meta.Releases != 1 {
			t.Errorf("expected Releases = 1 after release, got %d", meta.Releases)
		}
		// 清理
		_ = q.Delete(id)
	})

	// 测试 Bury
	t.Run("Bury", func(t *testing.T) {
		id, _ := q.Put("test-bury", []byte("test bury"), 1, 0, 60*time.Second)
		delayJob, err := q.Reserve([]string{"test-bury"}, 1*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		if err := delayJob.Bury(5); err != nil {
			t.Errorf("delayJob.Bury() failed: %v", err)
		}

		// 验证任务已埋葬
		meta, err := q.StatsDelayJob(id)
		if err != nil {
			t.Fatal(err)
		}
		if meta.DelayState != DelayStateBuried {
			t.Errorf("expected DelayStateBuried after bury, got %v", meta.DelayState)
		}
		if meta.Priority != 5 {
			t.Errorf("expected priority 5, got %d", meta.Priority)
		}

		time.Sleep(100 * time.Millisecond) // 等待异步更新完成
		if meta.Buries != 1 {
			t.Errorf("expected Buries = 1 after bury, got %d", meta.Buries)
		}

		// 清理
		_ = q.Delete(id)
	})

	// 测试 Kick
	t.Run("Kick", func(t *testing.T) {
		id, _ := q.Put("test-kick", []byte("test kick"), 1, 0, 60*time.Second)
		delayJob, err := q.Reserve([]string{"test-kick"}, 1*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		// 先 bury
		if err := delayJob.Bury(5); err != nil {
			t.Fatal(err)
		}

		// 验证已埋葬
		meta, err := q.StatsDelayJob(id)
		if err != nil {
			t.Fatal(err)
		}
		if meta.DelayState != DelayStateBuried {
			t.Fatalf("expected DelayStateBuried after bury, got %v", meta.DelayState)
		}

		// 测试 kick（使用原来的 delayJob 对象，它持有正确的 ID）
		if err := delayJob.Kick(); err != nil {
			t.Errorf("delayJob.Kick() failed: %v", err)
		}

		// 验证任务已恢复
		meta, err = q.StatsDelayJob(id)
		if err != nil {
			t.Fatal(err)
		}
		if meta.DelayState != DelayStateReady {
			t.Errorf("expected DelayStateReady after kick, got %v", meta.DelayState)
		}

		time.Sleep(100 * time.Millisecond) // 等待异步更新完成
		if meta.Kicks != 1 {
			t.Errorf("expected Kicks = 1 after kick, got %d", meta.Kicks)
		}

		// 清理
		_ = q.Delete(id)
	})

	// 测试 Touch
	t.Run("Touch", func(t *testing.T) {
		_, _ = q.Put("test-touch", []byte("test touch"), 1, 0, 5*time.Second)
		delayJob, err := q.Reserve([]string{"test-touch"}, 1*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		oldDeadline := delayJob.Meta.ReserveDeadline()

		// 延长 TTR
		time.Sleep(100 * time.Millisecond)
		if err := delayJob.Touch(10 * time.Second); err != nil {
			t.Errorf("delayJob.Touch() failed: %v", err)
		}

		// 获取新的 deadline
		meta, _ := q.StatsDelayJob(delayJob.Meta.ID)
		newDeadline := meta.ReserveDeadline()

		if !newDeadline.After(oldDeadline) {
			t.Errorf("expected deadline to be extended, old: %v, new: %v", oldDeadline, newDeadline)
		}

		time.Sleep(100 * time.Millisecond) // 等待异步更新完成
		if meta.Touches != 1 {
			t.Errorf("expected Touches = 1 after touch, got %d", meta.Touches)
		}

		// 清理
		_ = delayJob.Delete()
	})
}
