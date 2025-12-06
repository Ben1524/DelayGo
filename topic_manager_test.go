package delaygo

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// noopTicker is a no-op implementation of Ticker for testing
// It avoids the deadlock issues in DynamicSleepTicker and TimeWheelTicker
type noopTicker struct{}

func (t *noopTicker) Start()                                  {}
func (t *noopTicker) Stop()                                   {}
func (t *noopTicker) Register(name string, tickable Tickable) {}
func (t *noopTicker) Unregister(name string)                  {}
func (t *noopTicker) Wakeup()                                 {}
func (t *noopTicker) Stats() *TickerStats {
	return &TickerStats{Mode: "noop"}
}

func newNoopTicker() Ticker {
	return &noopTicker{}
}

func TestNewTopicManager(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		ticker := newNoopTicker()

		hub := newTopicManager(&config, testStorage.Storage, ticker)
		if hub == nil {
			t.Fatal("newTopicManager returned nil")
		}

		if hub.topics == nil {
			t.Error("topics map should be initialized")
		}

		if hub.topicWrappers == nil {
			t.Error("topicWrappers map should be initialized")
		}
	})
}

func TestTopicManagerGetOrCreateTopic(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Create new topic
	hub.Lock()
	topic, err := hub.GetOrCreateTopic("test")
	hub.Unlock()

	if err != nil {
		t.Fatalf("GetOrCreateTopic error: %v", err)
	}

	if topic == nil {
		t.Fatal("GetOrCreateTopic returned nil")
	}

	if topic.name != "test" {
		t.Errorf("topic name = %s, want test", topic.name)
	}

	// Get existing topic
	hub.Lock()
	topic2, err := hub.GetOrCreateTopic("test")
	hub.Unlock()

	if err != nil {
		t.Fatalf("GetOrCreateTopic second call error: %v", err)
	}

	if topic2 != topic {
		t.Error("Should return the same topic instance")
	}
}

func TestTopicManagerMaxTopics(t *testing.T) {
	config := DefaultConfig()
	config.MaxTopics = 2
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Create topics up to limit
	hub.Lock()
	_, err := hub.GetOrCreateTopic("topic1")
	hub.Unlock()
	if err != nil {
		t.Fatalf("GetOrCreateTopic topic1 error: %v", err)
	}

	hub.Lock()
	_, err = hub.GetOrCreateTopic("topic2")
	hub.Unlock()
	if err != nil {
		t.Fatalf("GetOrCreateTopic topic2 error: %v", err)
	}

	// Try to create one more
	hub.Lock()
	_, err = hub.GetOrCreateTopic("topic3")
	hub.Unlock()
	if err != ErrMaxTopicsReached {
		t.Errorf("GetOrCreateTopic topic3 = %v, want ErrMaxTopicsReached", err)
	}
}

func TestTopicManagerGetTopic(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Get non-existent topic
	hub.Lock()
	topic := hub.GetTopic("test")
	hub.Unlock()

	if topic != nil {
		t.Error("GetTopic should return nil for non-existent topic")
	}

	// Create and get topic
	hub.Lock()
	_, _ = hub.GetOrCreateTopic("test")
	topic = hub.GetTopic("test")
	hub.Unlock()

	if topic == nil {
		t.Error("GetTopic should return existing topic")
	}
}

func TestTopicManagerListTopics(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Empty hub
	topics := hub.ListTopics()
	if len(topics) != 0 {
		t.Errorf("ListTopics = %v, want empty", topics)
	}

	// Add topics
	hub.Lock()
	_, _ = hub.GetOrCreateTopic("beta")
	_, _ = hub.GetOrCreateTopic("alpha")
	_, _ = hub.GetOrCreateTopic("gamma")
	hub.Unlock()

	topics = hub.ListTopics()
	if len(topics) != 3 {
		t.Errorf("ListTopics count = %d, want 3", len(topics))
	}

	// Should be sorted
	if topics[0] != "alpha" || topics[1] != "beta" || topics[2] != "gamma" {
		t.Errorf("ListTopics not sorted: %v", topics)
	}
}

func TestTopicManagerTopicStats(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Non-existent topic
	stats := hub.TopicStats("test")
	if stats != nil {
		t.Error("TopicStats should return nil for non-existent topic")
	}

	// Create topic and add delayJobs
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateReady
	topic.pushReady(meta)
	hub.Unlock()

	stats = hub.TopicStats("test")
	if stats == nil {
		t.Fatal("TopicStats should return stats for existing topic")
	}

	if stats.ReadyDelayJobs != 1 {
		t.Errorf("ReadyDelayJobs = %d, want 1", stats.ReadyDelayJobs)
	}
}

func TestTopicManagerAllTopicStats(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Empty hub
	allStats := hub.AllTopicStats()
	if len(allStats) != 0 {
		t.Errorf("AllTopicStats = %v, want empty", allStats)
	}

	// Add topics with delayJobs
	hub.Lock()
	topic1, _ := hub.GetOrCreateTopic("topic1")
	meta1 := NewDelayJobMeta(1, "topic1", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateReady
	topic1.pushReady(meta1)

	topic2, _ := hub.GetOrCreateTopic("topic2")
	meta2 := NewDelayJobMeta(2, "topic2", 10, 0, 30*time.Second)
	meta2.DelayState = DelayStateDelayed
	topic2.pushDelayed(meta2)
	hub.Unlock()

	allStats = hub.AllTopicStats()
	if len(allStats) != 2 {
		t.Errorf("AllTopicStats count = %d, want 2", len(allStats))
	}
}

func TestTopicManagerTotalDelayJobs(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Empty hub
	total := hub.TotalDelayJobs()
	if total != 0 {
		t.Errorf("TotalDelayJobs = %d, want 0", total)
	}

	// Add delayJobs
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	topic.pushReady(meta1)
	meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
	topic.pushDelayed(meta2)
	hub.Unlock()

	total = hub.TotalDelayJobs()
	if total != 2 {
		t.Errorf("TotalDelayJobs = %d, want 2", total)
	}
}

func TestTopicManagerValidateTopicName(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	tests := []struct {
		name    string
		wantErr error
	}{
		{"", ErrTopicRequired},
		{"valid", nil},
		{"valid_name", nil},
		{"valid-name", nil},
		{"Valid123", nil},
		{"name with space", ErrInvalidTopic},
		{"name!special", ErrInvalidTopic},
		{string(make([]byte, 201)), ErrInvalidTopic}, // Too long
	}

	for _, tt := range tests {
		err := hub.ValidateTopicName(tt.name)
		if err != tt.wantErr {
			t.Errorf("ValidateTopicName(%q) = %v, want %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestTopicManagerPut(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		ticker := newNoopTicker()

		hub := newTopicManager(&config, testStorage.Storage, ticker)

		// Put ready delayJob
		meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
		needsNotify, err := hub.Put("test", meta)
		if err != nil {
			t.Fatalf("Put error: %v", err)
		}

		if !needsNotify {
			t.Error("Put ready delayJob should notify")
		}

		// Put delayed delayJob
		meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
		meta2.ReadyAt = time.Now().Add(10 * time.Second)
		needsNotify, err = hub.Put("test", meta2)
		if err != nil {
			t.Fatalf("Put delayed error: %v", err)
		}

		if needsNotify {
			t.Error("Put delayed delayJob should not notify")
		}
	})
}

func TestTopicManagerPutMaxDelayJobs(t *testing.T) {
	config := DefaultConfig()
	config.MaxDelayJobsPerTopic = 2
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Add delayJobs up to limit
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	_, _ = hub.Put("test", meta1)

	meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
	_, _ = hub.Put("test", meta2)

	// Try to add one more
	meta3 := NewDelayJobMeta(3, "test", 10, 0, 30*time.Second)
	_, err := hub.Put("test", meta3)
	if err != ErrMaxDelayJobsReached {
		t.Errorf("Put over limit = %v, want ErrMaxDelayJobsReached", err)
	}
}

func TestTopicManagerTryReserve(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		ticker := newNoopTicker()

		hub := newTopicManager(&config, testStorage.Storage, ticker)

		// No delayJobs
		meta := hub.TryReserve([]string{"test"})
		if meta != nil {
			t.Error("TryReserve should return nil when no delayJobs")
		}

		// Add ready delayJob
		hub.Lock()
		topic, _ := hub.GetOrCreateTopic("test")
		delayJobMeta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
		delayJobMeta.DelayState = DelayStateReady
		topic.pushReady(delayJobMeta)
		hub.Unlock()

		// Reserve delayJob
		meta = hub.TryReserve([]string{"test"})
		if meta == nil {
			t.Fatal("TryReserve should return delayJob")
		}

		if meta.DelayState != DelayStateReserved {
			t.Errorf("DelayState = %v, want DelayStateReserved", meta.DelayState)
		}

		if meta.Reserves != 1 {
			t.Errorf("Reserves = %d, want 1", meta.Reserves)
		}
	})
}

func TestTopicManagerDelete(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		ticker := newNoopTicker()

		hub := newTopicManager(&config, testStorage.Storage, ticker)

		// Add and reserve delayJob
		hub.Lock()
		topic, _ := hub.GetOrCreateTopic("test")
		meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.DelayState = DelayStateReserved
		topic.addReserved(meta)
		hub.Unlock()

		// Save to storage
		_ = testStorage.Storage.SaveDelayJob(context.Background(), meta, []byte("body"))

		// Delete delayJob
		err := hub.Delete(1)
		if err != nil {
			t.Fatalf("Delete error: %v", err)
		}

		// Verify deletion
		hub.Lock()
		foundMeta, _ := hub.FindDelayJob(1)
		hub.Unlock()

		if foundMeta != nil {
			t.Error("DelayJob should be deleted")
		}
	})
}

func TestTopicManagerDeleteNotReserved(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Add ready delayJob (not reserved)
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateReady
	topic.pushReady(meta)
	hub.Unlock()

	// Try to delete
	err := hub.Delete(1)
	if err != ErrInvalidDelayState {
		t.Errorf("Delete not reserved = %v, want ErrInvalidDelayState", err)
	}
}

func TestTopicManagerRelease(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		ticker := newNoopTicker()

		hub := newTopicManager(&config, testStorage.Storage, ticker)

		// Add reserved delayJob
		hub.Lock()
		topic, _ := hub.GetOrCreateTopic("test")
		meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.DelayState = DelayStateReserved
		topic.addReserved(meta)
		hub.Unlock()

		// Release without delay
		topicName, needsNotify, err := hub.Release(1, 5, 0)
		if err != nil {
			t.Fatalf("Release error: %v", err)
		}

		if topicName != "test" {
			t.Errorf("topicName = %s, want test", topicName)
		}

		if !needsNotify {
			t.Error("Release without delay should notify")
		}

		// Check delayJob is in ready queue
		hub.Lock()
		foundMeta, _ := hub.FindDelayJob(1)
		hub.Unlock()

		if foundMeta.DelayState != DelayStateReady {
			t.Errorf("DelayState = %v, want DelayStateReady", foundMeta.DelayState)
		}

		if foundMeta.Priority != 5 {
			t.Errorf("Priority = %d, want 5", foundMeta.Priority)
		}
	})
}

func TestTopicManagerReleaseWithDelay(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Add reserved delayJob
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateReserved
	topic.addReserved(meta)
	hub.Unlock()

	// Release with delay
	_, needsNotify, err := hub.Release(1, 5, 10*time.Second)
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}

	if needsNotify {
		t.Error("Release with delay should not notify")
	}

	// Check delayJob is in delayed queue
	hub.Lock()
	foundMeta, _ := hub.FindDelayJob(1)
	hub.Unlock()

	if foundMeta.DelayState != DelayStateDelayed {
		t.Errorf("DelayState = %v, want DelayStateDelayed", foundMeta.DelayState)
	}
}

func TestTopicManagerBury(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Add reserved delayJob
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateReserved
	topic.addReserved(meta)
	hub.Unlock()

	// Bury delayJob
	err := hub.Bury(1, 20)
	if err != nil {
		t.Fatalf("Bury error: %v", err)
	}

	// Check delayJob is buried
	hub.Lock()
	foundMeta, _ := hub.FindDelayJob(1)
	hub.Unlock()

	if foundMeta.DelayState != DelayStateBuried {
		t.Errorf("DelayState = %v, want DelayStateBuried", foundMeta.DelayState)
	}

	if foundMeta.Priority != 20 {
		t.Errorf("Priority = %d, want 20", foundMeta.Priority)
	}

	if foundMeta.Buries != 1 {
		t.Errorf("Buries = %d, want 1", foundMeta.Buries)
	}
}

func TestTopicManagerKick(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Add buried delayJobs
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	for i := uint64(1); i <= 5; i++ {
		meta := NewDelayJobMeta(i, "test", uint32(i), 0, 30*time.Second)
		meta.DelayState = DelayStateBuried
		topic.pushBuried(meta)
	}
	hub.Unlock()

	// Kick 3 delayJobs
	kicked, needsNotify, err := hub.Kick("test", 3)
	if err != nil {
		t.Fatalf("Kick error: %v", err)
	}

	if kicked != 3 {
		t.Errorf("kicked = %d, want 3", kicked)
	}

	if !needsNotify {
		t.Error("Kick should notify")
	}

	// Check stats
	stats := hub.TopicStats("test")
	if stats.BuriedDelayJobs != 2 {
		t.Errorf("BuriedDelayJobs = %d, want 2", stats.BuriedDelayJobs)
	}

	if stats.ReadyDelayJobs != 3 {
		t.Errorf("ReadyDelayJobs = %d, want 3", stats.ReadyDelayJobs)
	}
}

func TestTopicManagerKickDelayJob(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Add buried delayJob
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.DelayState = DelayStateBuried
	topic.pushBuried(meta)
	hub.Unlock()

	// Kick specific delayJob
	topicName, err := hub.KickDelayJob(1)
	if err != nil {
		t.Fatalf("KickDelayJob error: %v", err)
	}

	if topicName != "test" {
		t.Errorf("topicName = %s, want test", topicName)
	}

	// Check delayJob is ready
	hub.Lock()
	foundMeta, _ := hub.FindDelayJob(1)
	hub.Unlock()

	if foundMeta.DelayState != DelayStateReady {
		t.Errorf("DelayState = %v, want DelayStateReady", foundMeta.DelayState)
	}

	if foundMeta.Kicks != 1 {
		t.Errorf("Kicks = %d, want 1", foundMeta.Kicks)
	}
}

func TestTopicManagerFindDelayJob(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Add delayJobs in different states
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")

	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateReady
	topic.pushReady(meta1)

	meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.DelayState = DelayStateDelayed
	topic.pushDelayed(meta2)

	meta3 := NewDelayJobMeta(3, "test", 10, 0, 30*time.Second)
	meta3.DelayState = DelayStateReserved
	topic.addReserved(meta3)

	meta4 := NewDelayJobMeta(4, "test", 10, 0, 30*time.Second)
	meta4.DelayState = DelayStateBuried
	topic.pushBuried(meta4)

	// Find each delayJob
	foundMeta, foundTopic := hub.FindDelayJob(1)
	if foundMeta == nil || foundTopic == nil {
		t.Error("Should find ready delayJob")
	}

	foundMeta, foundTopic = hub.FindDelayJob(2)
	if foundMeta == nil || foundTopic == nil {
		t.Error("Should find delayed delayJob")
	}

	foundMeta, foundTopic = hub.FindDelayJob(3)
	if foundMeta == nil || foundTopic == nil {
		t.Error("Should find reserved delayJob")
	}

	foundMeta, foundTopic = hub.FindDelayJob(4)
	if foundMeta == nil || foundTopic == nil {
		t.Error("Should find buried delayJob")
	}

	// Find non-existent
	foundMeta, foundTopic = hub.FindDelayJob(999)
	hub.Unlock()

	if foundMeta != nil || foundTopic != nil {
		t.Error("Should not find non-existent delayJob")
	}
}

func TestTopicManagerCleanupEmptyTopics(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicManager(&config, nil, ticker)

	// Add topics
	hub.Lock()
	_, _ = hub.GetOrCreateTopic("empty1")
	_, _ = hub.GetOrCreateTopic("empty2")
	topic3, _ := hub.GetOrCreateTopic("notempty")
	meta := NewDelayJobMeta(1, "notempty", 10, 0, 30*time.Second)
	topic3.pushReady(meta)
	hub.Unlock()

	// Cleanup
	cleaned := hub.CleanupEmptyTopics()
	if cleaned != 2 {
		t.Errorf("cleaned = %d, want 2", cleaned)
	}

	topics := hub.ListTopics()
	if len(topics) != 1 {
		t.Errorf("topics count = %d, want 1", len(topics))
	}

	if topics[0] != "notempty" {
		t.Errorf("remaining topic = %s, want notempty", topics[0])
	}
}

// TestStress_ManyTopics 大量 Topic 测试
func TestStress_ManyTopics(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁
		config.MaxTopics = 10000

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		const topicCount = 1000

		startTime := time.Now()

		// 创建大量 topic，每个 topic 1 个任务
		var wg sync.WaitGroup
		for i := range topicCount {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				_, _ = q.Put(fmt.Sprintf("topic-%d", id), []byte("delayJob"), 1, 0, 5*time.Second)
			}(i)
		}
		wg.Wait()

		createTime := time.Since(startTime)
		t.Logf("创建 %d 个 topic 耗时: %v", topicCount, createTime)

		// 统计
		stats := q.Stats()
		if stats.Topics != topicCount {
			t.Errorf("期望 %d 个 topic，实际 %d", topicCount, stats.Topics)
		}

		t.Logf("Topic 统计: %d topics, %d delayJobs", stats.Topics, stats.TotalDelayJobs)
	})
}
