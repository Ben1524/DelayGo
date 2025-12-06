package delaygo

import (
	"context"
	"testing"
	"time"
)

func TestNewRecoveryManager(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		if rm == nil {
			t.Fatal("newRecoveryRunner returned nil")
		}

		if rm.storage != testStorage.Storage {
			t.Error("storage should be set")
		}
	})
}

func TestRecoveryManagerRecoverEmpty(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		rm := newRecoveryRunner(ctx, testStorage.Storage)

		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		if result.TotalDelayJobs != 0 {
			t.Errorf("TotalDelayJobs = %d, want 0", result.TotalDelayJobs)
		}

		if result.MaxID != 0 {
			t.Errorf("MaxID = %d, want 0", result.MaxID)
		}

		if len(result.TopicDelayJobs) != 0 {
			t.Errorf("TopicDelayJobs = %v, want empty", result.TopicDelayJobs)
		}
	})
}

// TestRecoveryManagerRecoverNilStorage 已删除
// 现在 Storage 是必需的，不再支持 nil storage

func TestRecoveryManagerRecoverReadyDelayJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save ready delayJobs
		meta1 := NewDelayJobMeta(1, "topic1", 10, 0, 30*time.Second)
		meta1.DelayState = DelayStateReady
		_ = testStorage.Storage.SaveDelayJob(ctx, meta1, []byte("body1"))

		meta2 := NewDelayJobMeta(2, "topic1", 5, 0, 30*time.Second)
		meta2.DelayState = DelayStateReady
		_ = testStorage.Storage.SaveDelayJob(ctx, meta2, []byte("body2"))

		meta3 := NewDelayJobMeta(3, "topic2", 10, 0, 30*time.Second)
		meta3.DelayState = DelayStateReady
		_ = testStorage.Storage.SaveDelayJob(ctx, meta3, []byte("body3"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		if result.TotalDelayJobs != 3 {
			t.Errorf("TotalDelayJobs = %d, want 3", result.TotalDelayJobs)
		}

		if result.MaxID != 3 {
			t.Errorf("MaxID = %d, want 3", result.MaxID)
		}

		if len(result.TopicDelayJobs) != 2 {
			t.Errorf("TopicDelayJobs count = %d, want 2", len(result.TopicDelayJobs))
		}

		if len(result.TopicDelayJobs["topic1"]) != 2 {
			t.Errorf("topic1 delayJobs = %d, want 2", len(result.TopicDelayJobs["topic1"]))
		}

		if len(result.TopicDelayJobs["topic2"]) != 1 {
			t.Errorf("topic2 delayJobs = %d, want 1", len(result.TopicDelayJobs["topic2"]))
		}
	})
}

func TestRecoveryManagerRecoverDelayedDelayJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save delayed delayJob
		meta := NewDelayJobMeta(1, "test", 10, 5*time.Second, 30*time.Second)
		meta.DelayState = DelayStateDelayed
		meta.ReadyAt = time.Now().Add(5 * time.Second)
		_ = testStorage.Storage.SaveDelayJob(ctx, meta, []byte("body"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		if result.TotalDelayJobs != 1 {
			t.Errorf("TotalDelayJobs = %d, want 1", result.TotalDelayJobs)
		}

		delayJobs := result.TopicDelayJobs["test"]
		if len(delayJobs) != 1 {
			t.Fatalf("test delayJobs = %d, want 1", len(delayJobs))
		}

		if delayJobs[0].DelayState != DelayStateDelayed {
			t.Errorf("DelayState = %v, want DelayStateDelayed", delayJobs[0].DelayState)
		}
	})
}

func TestRecoveryManagerRecoverBuriedDelayJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save buried delayJob
		meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.DelayState = DelayStateBuried
		meta.BuriedAt = time.Now()
		_ = testStorage.Storage.SaveDelayJob(ctx, meta, []byte("body"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		delayJobs := result.TopicDelayJobs["test"]
		if len(delayJobs) != 1 {
			t.Fatalf("test delayJobs = %d, want 1", len(delayJobs))
		}

		if delayJobs[0].DelayState != DelayStateBuried {
			t.Errorf("DelayState = %v, want DelayStateBuried", delayJobs[0].DelayState)
		}
	})
}

func TestRecoveryManagerRecoverEnqueuedDelayJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save enqueued delayJob (no delay)
		meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
		meta1.DelayState = DelayStateEnqueued
		_ = testStorage.Storage.SaveDelayJob(ctx, meta1, []byte("body1"))

		// Save enqueued delayJob (with delay)
		meta2 := NewDelayJobMeta(2, "test", 10, 5*time.Second, 30*time.Second)
		meta2.DelayState = DelayStateEnqueued
		meta2.Delay = 5 * time.Second
		_ = testStorage.Storage.SaveDelayJob(ctx, meta2, []byte("body2"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		delayJobs := result.TopicDelayJobs["test"]
		if len(delayJobs) != 2 {
			t.Fatalf("test delayJobs = %d, want 2", len(delayJobs))
		}

		// Enqueued without delay should become Ready
		var readyDelayJob, delayedDelayJob *DelayJobMeta
		for _, delayJob := range delayJobs {
			if delayJob.ID == 1 {
				readyDelayJob = delayJob
			} else {
				delayedDelayJob = delayJob
			}
		}

		if readyDelayJob == nil || readyDelayJob.DelayState != DelayStateReady {
			t.Errorf("DelayJob 1 DelayState = %v, want DelayStateReady", readyDelayJob.DelayState)
		}

		// Enqueued with delay should become Delayed
		if delayedDelayJob == nil || delayedDelayJob.DelayState != DelayStateDelayed {
			t.Errorf("DelayJob 2 DelayState = %v, want DelayStateDelayed", delayedDelayJob.DelayState)
		}
	})
}

func TestRecoveryManagerRecoverReservedDelayJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save reserved delayJob (simulating crash during processing)
		meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.DelayState = DelayStateReserved
		meta.ReservedAt = time.Now().Add(-1 * time.Minute)
		meta.Reserves = 1
		_ = testStorage.Storage.SaveDelayJob(ctx, meta, []byte("body"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		delayJobs := result.TopicDelayJobs["test"]
		if len(delayJobs) != 1 {
			t.Fatalf("test delayJobs = %d, want 1", len(delayJobs))
		}

		// Reserved delayJobs should become Ready (for re-processing)
		if delayJobs[0].DelayState != DelayStateReady {
			t.Errorf("DelayState = %v, want DelayStateReady", delayJobs[0].DelayState)
		}

		// Timeouts should be incremented
		if delayJobs[0].Timeouts != 1 {
			t.Errorf("Timeouts = %d, want 1", delayJobs[0].Timeouts)
		}

		// ReservedAt should be cleared
		if !delayJobs[0].ReservedAt.IsZero() {
			t.Error("ReservedAt should be zero")
		}
	})
}

func TestRecoveryManagerMaxID(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save delayJobs with various IDs
		for _, id := range []uint64{5, 100, 50, 1, 75} {
			meta := NewDelayJobMeta(id, "test", 10, 0, 30*time.Second)
			meta.DelayState = DelayStateReady
			_ = testStorage.Storage.SaveDelayJob(ctx, meta, []byte("body"))
		}

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		if result.MaxID != 100 {
			t.Errorf("MaxID = %d, want 100", result.MaxID)
		}
	})
}

func TestRecoveryManagerFailedDelayJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save delayJob with unknown state (should be skipped)
		meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.DelayState = DelayState(99) // Unknown state
		_ = testStorage.Storage.SaveDelayJob(ctx, meta, []byte("body"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		if result.TotalDelayJobs != 1 {
			t.Errorf("TotalDelayJobs = %d, want 1", result.TotalDelayJobs)
		}

		if result.FailedDelayJobs != 1 {
			t.Errorf("FailedDelayJobs = %d, want 1", result.FailedDelayJobs)
		}

		// Unknown state delayJobs should not be in TopicDelayJobs
		if len(result.TopicDelayJobs["test"]) != 0 {
			t.Errorf("TopicDelayJobs should not contain failed delayJobs")
		}
	})
}

func TestRecoveryManagerPreprocessDelayJobMeta(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		rm := newRecoveryRunner(ctx, testStorage.Storage)

		tests := []struct {
			name            string
			inputDelayState DelayState
			inputDelay      time.Duration
			wantDelayState  DelayState
			shouldBeNil     bool
		}{
			{"Ready stays Ready", DelayStateReady, 0, DelayStateReady, false},
			{"Delayed stays Delayed", DelayStateDelayed, 5 * time.Second, DelayStateDelayed, false},
			{"Buried stays Buried", DelayStateBuried, 0, DelayStateBuried, false},
			{"Enqueued no delay becomes Ready", DelayStateEnqueued, 0, DelayStateReady, false},
			{"Enqueued with delay becomes Delayed", DelayStateEnqueued, 5 * time.Second, DelayStateDelayed, false},
			{"Reserved becomes Ready", DelayStateReserved, 0, DelayStateReady, false},
			{"Unknown state returns nil", DelayState(99), 0, 0, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				meta := NewDelayJobMeta(1, "test", 10, tt.inputDelay, 30*time.Second)
				meta.DelayState = tt.inputDelayState
				if tt.inputDelayState == DelayStateReserved {
					meta.ReservedAt = time.Now()
				}

				result := rm.preprocessDelayJobMeta(meta)

				if tt.shouldBeNil {
					if result != nil {
						t.Errorf("preprocessDelayJobMeta should return nil for unknown state")
					}
				} else {
					if result == nil {
						t.Errorf("preprocessDelayJobMeta should not return nil")
					} else if result.DelayState != tt.wantDelayState {
						t.Errorf("DelayState = %v, want %v", result.DelayState, tt.wantDelayState)
					}
				}
			})
		}
	})
}

func TestRecoveryManagerHandleEnqueuedDelayJob(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		rm := newRecoveryRunner(ctx, testStorage.Storage)

		// Without delay
		meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
		meta1.DelayState = DelayStateEnqueued
		result := rm.handleEnqueuedDelayJob(meta1)

		if result.DelayState != DelayStateReady {
			t.Errorf("DelayState = %v, want DelayStateReady", result.DelayState)
		}

		// With delay
		meta2 := NewDelayJobMeta(2, "test", 10, 5*time.Second, 30*time.Second)
		meta2.DelayState = DelayStateEnqueued
		result = rm.handleEnqueuedDelayJob(meta2)

		if result.DelayState != DelayStateDelayed {
			t.Errorf("DelayState = %v, want DelayStateDelayed", result.DelayState)
		}
	})
}

func TestRecoveryManagerHandleReservedDelayJob(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		rm := newRecoveryRunner(ctx, testStorage.Storage)

		meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.DelayState = DelayStateReserved
		meta.ReservedAt = time.Now()
		meta.Timeouts = 0

		result := rm.handleReservedDelayJob(meta)

		if result.DelayState != DelayStateReady {
			t.Errorf("DelayState = %v, want DelayStateReady", result.DelayState)
		}

		if !result.ReservedAt.IsZero() {
			t.Error("ReservedAt should be cleared")
		}

		if result.Timeouts != 1 {
			t.Errorf("Timeouts = %d, want 1", result.Timeouts)
		}

		if result.ReadyAt.IsZero() {
			t.Error("ReadyAt should be set")
		}
	})
}

func TestRecoveryResultApply(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		// Test that RecoveryResult can be applied to TopicManager
		ctx := context.Background()

		// Create delayJobs in storage
		meta1 := NewDelayJobMeta(1, "topic1", 10, 0, 30*time.Second)
		meta1.DelayState = DelayStateReady
		_ = testStorage.Storage.SaveDelayJob(ctx, meta1, []byte("body1"))

		meta2 := NewDelayJobMeta(2, "topic1", 5, 0, 30*time.Second)
		meta2.DelayState = DelayStateDelayed
		meta2.ReadyAt = time.Now().Add(10 * time.Second)
		_ = testStorage.Storage.SaveDelayJob(ctx, meta2, []byte("body2"))

		meta3 := NewDelayJobMeta(3, "topic2", 10, 0, 30*time.Second)
		meta3.DelayState = DelayStateBuried
		_ = testStorage.Storage.SaveDelayJob(ctx, meta3, []byte("body3"))

		// Recover
		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		// Apply to TopicManager
		config := DefaultConfig()
		ticker := NewNoOpTicker() // 使用 NoOpTicker 避免死锁
		hub := newTopicManager(&config, testStorage.Storage, ticker)

		err = hub.ApplyRecovery(result)
		if err != nil {
			t.Fatalf("ApplyRecovery error: %v", err)
		}

		// Verify
		topics := hub.ListTopics()
		if len(topics) != 2 {
			t.Errorf("topics count = %d, want 2", len(topics))
		}

		stats1 := hub.TopicStats("topic1")
		if stats1 == nil {
			t.Fatal("topic1 stats should not be nil")
		}

		// Note: one delayJob is delayed, so it went to delayed queue
		if stats1.ReadyDelayJobs != 1 {
			t.Errorf("topic1 ReadyDelayJobs = %d, want 1", stats1.ReadyDelayJobs)
		}

		stats2 := hub.TopicStats("topic2")
		if stats2 == nil {
			t.Fatal("topic2 stats should not be nil")
		}

		if stats2.BuriedDelayJobs != 1 {
			t.Errorf("topic2 BuriedDelayJobs = %d, want 1", stats2.BuriedDelayJobs)
		}
	})
}
