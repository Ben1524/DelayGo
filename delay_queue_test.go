package delaygo

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func ExampleDelayQueue_basic() {
	// 创建配置
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

	// 创建队列
	q, err := New(config)
	if err != nil {
		log.Fatal(err)
	}

	// 启动队列
	if err := q.Start(); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	// 添加任务
	delayJobID, err := q.Put(
		"email",                                  // topic
		[]byte("send email to user@example.com"), // body
		1,                                        // priority
		0,                                        // delay
		60*time.Second,                           // ttr
	)
	if err != nil {
		log.Fatal(err)
	}

	if delayJobID > 0 {
		fmt.Println("Created delayJob")
	}

	// Worker 保留任务
	delayJob, err := q.Reserve([]string{"email"}, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Reserved delayJob, body: %s\n", string(delayJob.Body()))

	// 完成任务
	if err := q.Delete(delayJob.Meta.ID); err != nil {
		log.Fatal(err)
	}

	fmt.Println("DelayJob completed")

	// Output:
	// Created delayJob
	// Reserved delayJob, body: send email to user@example.com
	// DelayJob completed
}

func ExampleDelayQueue_delayed() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加延迟任务（500ms 后执行）
	delayJobID, _ := q.Put(
		"notification",
		[]byte("reminder"),
		1,
		500*time.Millisecond, // delay
		60*time.Second,
	)

	if delayJobID > 0 {
		fmt.Println("Created delayed delayJob")
	}

	// 立即尝试 Reserve（应该超时）
	_, err := q.Reserve([]string{"notification"}, 50*time.Millisecond)
	if err == ErrTimeout {
		fmt.Println("No delayJob available yet (delayed)")
	}

	// 等待任务到期
	time.Sleep(600 * time.Millisecond)

	// 现在可以 Reserve
	delayJob, err := q.Reserve([]string{"notification"}, 1*time.Second)
	if err == nil {
		fmt.Println("Reserved delayJob after delay")
		_ = q.Delete(delayJob.Meta.ID)
	}

	// Output:
	// Created delayed delayJob
	// No delayJob available yet (delayed)
	// Reserved delayJob after delay
}

func ExampleDelayQueue_priority() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加不同优先级的任务
	_, _ = q.Put("tasks", []byte("low priority"), 10, 0, 60*time.Second)
	_, _ = q.Put("tasks", []byte("high priority"), 1, 0, 60*time.Second)
	_, _ = q.Put("tasks", []byte("medium priority"), 5, 0, 60*time.Second)

	// Reserve 会按优先级顺序返回
	for range 3 {
		delayJob, _ := q.Reserve([]string{"tasks"}, 1*time.Second)
		fmt.Printf("Priority %d: %s\n", delayJob.Meta.Priority, string(delayJob.Body()))
		_ = q.Delete(delayJob.Meta.ID)
	}

	// Output:
	// Priority 1: high priority
	// Priority 5: medium priority
	// Priority 10: low priority
}

func ExampleDelayQueue_release() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewTimeWheelTicker(10*time.Millisecond, 100) // 使用 TimeWheelTicker 确保及时处理

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加任务
	_, _ = q.Put("retry", []byte("task"), 1, 0, 60*time.Second)

	// Worker 1 保留任务
	delayJob, _ := q.Reserve([]string{"retry"}, 1*time.Second)
	fmt.Println("Worker 1 reserved delayJob")

	// 处理失败，释放任务（延迟 100ms 重试）
	_ = q.Release(delayJob.Meta.ID, 1, 100*time.Millisecond)
	fmt.Println("DelayJob released with 100ms delay")

	// 立即尝试 Reserve（应该没有任务）
	_, err := q.Reserve([]string{"retry"}, 10*time.Millisecond)
	if err == ErrTimeout {
		fmt.Println("No delayJob available (released with delay)")
	}

	// 等待延迟到期（TimeWheelTicker 会自动处理）
	time.Sleep(150 * time.Millisecond)

	delayJob, err = q.Reserve([]string{"retry"}, 1*time.Second)
	if err == nil && delayJob != nil {
		fmt.Println("Worker 2 reserved delayJob (retry)")
		_ = q.Delete(delayJob.Meta.ID)
	}

	// Output:
	// Worker 1 reserved delayJob
	// DelayJob released with 100ms delay
	// No delayJob available (released with delay)
	// Worker 2 reserved delayJob (retry)
}

func ExampleDelayQueue_bury() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加任务
	_, _ = q.Put("process", []byte("task"), 1, 0, 60*time.Second)

	// 保留任务
	delayJob, _ := q.Reserve([]string{"process"}, 1*time.Second)

	// 遇到无法处理的错误，埋葬任务
	_ = q.Bury(delayJob.Meta.ID, 1)
	fmt.Println("DelayJob buried")

	// 无法再 Reserve
	_, err := q.Reserve([]string{"process"}, 100*time.Millisecond)
	if err == ErrTimeout {
		fmt.Println("No delayJob available (buried)")
	}

	// 管理员 Kick 恢复任务
	count, _ := q.Kick("process", 10)
	fmt.Printf("Kicked %d delayJobs\n", count)

	// 现在可以 Reserve
	delayJob, _ = q.Reserve([]string{"process"}, 1*time.Second)
	fmt.Println("Reserved delayJob after kick")
	_ = q.Delete(delayJob.Meta.ID)

	// Output:
	// DelayJob buried
	// No delayJob available (buried)
	// Kicked 1 delayJobs
	// Reserved delayJob after kick
}

func ExampleDelayQueue_stats() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加各种状态的任务
	_, _ = q.Put("email", []byte("task1"), 1, 0, 60*time.Second)             // ready
	_, _ = q.Put("email", []byte("task2"), 1, 5*time.Second, 60*time.Second) // delayed
	delayJob, _ := q.Reserve([]string{"email"}, 1*time.Second)               // reserved

	// 查看整体统计
	stats := q.Stats()
	fmt.Printf("Total delayJobs: %d, Ready: %d, Delayed: %d, Reserved: %d\n",
		stats.TotalDelayJobs, stats.ReadyDelayJobs, stats.DelayedDelayJobs, stats.ReservedDelayJobs)

	// 查看 Topic 统计
	topicStats, _ := q.StatsTopic("email")
	fmt.Printf("Topic 'email': Total: %d, Ready: %d, Delayed: %d, Reserved: %d\n",
		topicStats.TotalDelayJobs, topicStats.ReadyDelayJobs, topicStats.DelayedDelayJobs, topicStats.ReservedDelayJobs)

	_ = q.Delete(delayJob.Meta.ID)

	// Output:
	// Total delayJobs: 2, Ready: 0, Delayed: 1, Reserved: 1
	// Topic 'email': Total: 2, Ready: 0, Delayed: 1, Reserved: 1
}

func ExampleDelayQueue_multipleTopics() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加不同 topic 的任务
	_, _ = q.Put("email", []byte("send email"), 1, 0, 60*time.Second)
	_, _ = q.Put("sms", []byte("send sms"), 1, 0, 60*time.Second)
	_, _ = q.Put("push", []byte("send push"), 1, 0, 60*time.Second)

	// Worker 可以监听多个 topic
	delayJob, _ := q.Reserve([]string{"email", "sms", "push"}, 1*time.Second)
	fmt.Printf("Reserved from topic: %s\n", delayJob.Meta.Topic)
	_ = q.Delete(delayJob.Meta.ID)

	// 列出所有 topic
	topics := q.ListTopics()
	fmt.Printf("Topics: %v\n", topics)

	// Output:
	// Reserved from topic: email
	// Topics: [email push sms]
}

// TestReserve_NoThunderingHerd 测试防止惊群效应
func TestReserve_NoThunderingHerd(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 启动多个 worker 等待任务
		workerCount := 10
		var wg sync.WaitGroup
		var reserveCount atomic.Int32

		for range workerCount {
			wg.Go(func() {
				delayJob, err := q.Reserve([]string{"test"}, 2*time.Second)
				if err == nil && delayJob != nil {
					reserveCount.Add(1)
					_ = q.Delete(delayJob.Meta.ID)
				}
			})
		}

		// 等待 workers 进入等待状态
		time.Sleep(100 * time.Millisecond)

		// 只添加一个任务
		_, err = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		wg.Wait()

		// 只应该有一个 worker 获得任务
		if reserveCount.Load() != 1 {
			t.Errorf("expected 1 worker to reserve delayJob, got %d", reserveCount.Load())
		}
	})
}

// TestTouch_MaxTouches 测试 MaxTouches 限制
func TestTouch_MaxTouches(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()
		config.MaxTouches = 3       // 设置最大 Touch 次数为 3
		config.MinTouchInterval = 0 // 禁用间隔限制
		config.MaxTouchDuration = 0 // 禁用时长限制

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布任务
		_, err = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Reserve 任务
		delayJob, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		// Touch 3 次应该成功
		for i := 0; i < 3; i++ {
			err = q.Touch(delayJob.Meta.ID)
			if err != nil {
				t.Errorf("Touch %d failed: %v", i+1, err)
			}

			// 验证 Touches 计数
			meta, err := q.StatsDelayJob(delayJob.Meta.ID)
			if err != nil {
				t.Fatalf("StatsDelayJob failed: %v", err)
			}
			if meta.Touches != i+1 {
				t.Errorf("Touches = %d, want %d", meta.Touches, i+1)
			}
		}

		// 第 4 次 Touch 应该失败
		err = q.Touch(delayJob.Meta.ID)
		if err == nil {
			t.Error("Touch should fail when exceeding MaxTouches")
		}

		// 清理
		_ = delayJob.Delete()
	})
}

// TestTouch_MinTouchInterval 测试 MinTouchInterval 限制
func TestTouch_MinTouchInterval(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()
		config.MinTouchInterval = 100 * time.Millisecond // 最小间隔 100ms

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布任务
		_, err = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Reserve 任务
		delayJob, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		// 第一次 Touch 应该成功
		err = q.Touch(delayJob.Meta.ID)
		if err != nil {
			t.Errorf("First Touch failed: %v", err)
		}

		// 立即再次 Touch 应该失败（间隔太短）
		err = q.Touch(delayJob.Meta.ID)
		if err == nil {
			t.Error("Touch should fail when interval is too short")
		}

		// 等待足够的间隔
		time.Sleep(150 * time.Millisecond)

		// 现在 Touch 应该成功
		err = q.Touch(delayJob.Meta.ID)
		if err != nil {
			t.Errorf("Touch after interval failed: %v", err)
		}

		// 清理
		_ = delayJob.Delete()
	})
}

// TestTouch_MaxTouchDuration 测试 MaxTouchDuration 限制
func TestTouch_MaxTouchDuration(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		t.Logf("Storage type: %s, actual type: %T", testStorage.Type, testStorage.Storage)

		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()
		config.MaxTouchDuration = 500 * time.Millisecond // 最大延长 500ms
		config.MinTouchInterval = 0                      // 禁用间隔限制

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布任务，TTR 200ms
		_, err = q.Put("test", []byte("task"), 1, 0, 200*time.Millisecond)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Reserve 任务
		delayJob, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		t.Logf("Initial state: Touches=%d, TotalTouchTime=%v, TTR=%v, Meta ptr=%p",
			delayJob.Meta.Touches, delayJob.Meta.TotalTouchTime, delayJob.Meta.TTR, delayJob.Meta)

		// Touch 延长 200ms，应该成功（总延长 200ms）
		err = delayJob.Touch()
		t.Logf("After 1st Touch: err=%v, Touches=%d, TotalTouchTime=%v",
			err, delayJob.Meta.Touches, delayJob.Meta.TotalTouchTime)
		if err != nil {
			t.Errorf("First Touch failed: %v", err)
		}

		// 再 Touch 延长 200ms，应该成功（总延长 400ms）
		err = delayJob.Touch()
		t.Logf("After 2nd Touch: err=%v, Touches=%d, TotalTouchTime=%v",
			err, delayJob.Meta.Touches, delayJob.Meta.TotalTouchTime)
		if err != nil {
			t.Errorf("Second Touch failed: %v", err)
		}

		// 再 Touch 延长 200ms，应该失败（总延长会超过 500ms）
		err = delayJob.Touch()
		t.Logf("After 3rd Touch: err=%v, Touches=%d, TotalTouchTime=%v",
			err, delayJob.Meta.Touches, delayJob.Meta.TotalTouchTime)
		if err == nil {
			t.Error("Touch should fail when exceeding MaxTouchDuration")
		}

		// 清理
		_ = delayJob.Delete()
	})
}

// TestTouch_ResetTTR 测试 Touch 重置 TTR
func TestTouch_ResetTTR(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewTimeWheelTicker(10*time.Millisecond, 100)
		config.MaxTouches = 0 // 禁用次数限制

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布任务，TTR 1s
		delayJobID, err := q.Put("test", []byte("task"), 1, 0, 1*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Reserve 任务
		delayJob, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		// 等待 600ms
		time.Sleep(600 * time.Millisecond)

		// Touch 重置 TTR
		err = delayJob.Touch()
		if err != nil {
			t.Fatalf("Touch failed: %v", err)
		}

		// 再等待 600ms
		time.Sleep(600 * time.Millisecond)

		// 任务应该仍然是 Reserved 状态（因为 Touch 重置了 TTR）
		stats := q.Stats()
		if stats.ReservedDelayJobs != 1 {
			t.Errorf("ReservedDelayJobs = %d, want 1 (Touch should reset TTR)", stats.ReservedDelayJobs)
		}

		// 验证不能再次 Reserve 这个任务
		_, err = q.Reserve([]string{"test"}, 100*time.Millisecond)
		if err != ErrTimeout {
			t.Errorf("Reserve should timeout, got: %v", err)
		}

		// 清理
		_ = q.Delete(delayJobID)
	})
}

// TestReserve_FIFO 测试 FIFO 公平性
func TestReserve_FIFO(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 记录 reserve 顺序
		var mu sync.Mutex
		order := make([]int, 0)

		// 启动 5 个 worker，按顺序等待
		var wg sync.WaitGroup
		for i := range 5 {
			workerID := i
			wg.Go(func() {
				time.Sleep(time.Duration(workerID) * 10 * time.Millisecond) // 确保按顺序启动
				delayJob, err := q.Reserve([]string{"test"}, 3*time.Second)
				if err == nil && delayJob != nil {
					mu.Lock()
					order = append(order, workerID)
					mu.Unlock()
					_ = q.Delete(delayJob.Meta.ID)
				}
			})
		}

		// 等待所有 workers 进入等待状态
		time.Sleep(200 * time.Millisecond)

		// 添加 5 个任务
		for range 5 {
			_, err = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond) // 确保任务逐个到达
		}

		wg.Wait()

		// 检查顺序应该是 0, 1, 2, 3, 4 (FIFO)
		if len(order) != 5 {
			t.Fatalf("expected 5 workers to reserve delayJobs, got %d", len(order))
		}

		for i := range 5 {
			if order[i] != i {
				t.Errorf("expected worker %d at position %d, got worker %d", i, i, order[i])
			}
		}
	})
}

// TestReserve_MultipleTopics 测试多 topic 等待
func TestReserve_MultipleTopics(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// Worker 监听多个 topics
		var wg sync.WaitGroup
		wg.Add(1)
		var receivedTopic string

		go func() {
			defer wg.Done()
			delayJob, err := q.Reserve([]string{"email", "sms", "push"}, 2*time.Second)
			if err == nil && delayJob != nil {
				receivedTopic = delayJob.Meta.Topic
				_ = q.Delete(delayJob.Meta.ID)
			}
		}()

		// 等待 worker 进入等待状态
		time.Sleep(100 * time.Millisecond)

		// 在 sms topic 添加任务
		_, err = q.Put("sms", []byte("message"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		wg.Wait()

		if receivedTopic != "sms" {
			t.Errorf("expected to receive from 'sms', got '%s'", receivedTopic)
		}
	})
}

// TestReserve_Timeout 测试超时机制
func TestReserve_Timeout(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		start := time.Now()
		_, err = q.Reserve([]string{"test"}, 500*time.Millisecond)
		elapsed := time.Since(start)

		if err != ErrTimeout {
			t.Errorf("expected ErrTimeout, got %v", err)
		}

		if elapsed < 400*time.Millisecond || elapsed > 600*time.Millisecond {
			t.Errorf("expected ~500ms timeout, got %v", elapsed)
		}
	})
}

// TestReserve_ImmediateAvailable 测试立即可用的任务
func TestReserve_ImmediateAvailable(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 先添加任务
		_, err = q.Put("test", []byte("immediate"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		// Reserve 应该立即返回
		start := time.Now()
		delayJob, err := q.Reserve([]string{"test"}, 5*time.Second)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("expected success, got error: %v", err)
		}

		if delayJob == nil {
			t.Fatal("expected delayJob, got nil")
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("expected immediate return, took %v", elapsed)
		}

		_ = q.Delete(delayJob.Meta.ID)
	})
}

// TestReserve_WaiterCleanup 测试 waiter 超时后的清理
func TestReserve_WaiterCleanup(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 启动多个会超时的 reserve
		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				_, _ = q.Reserve([]string{"test"}, 100*time.Millisecond)
			})
		}

		wg.Wait()

		// 检查等待队列是否已清理
		stats := q.StatsWaiting()
		if len(stats) > 0 {
			for _, s := range stats {
				if s.WaitingWorkers > 0 {
					t.Errorf("expected no waiting workers, got %d for topic %s", s.WaitingWorkers, s.Topic)
				}
			}
		}
	})
}

// BenchmarkReserve_Concurrent 并发 Reserve 性能测试
func BenchmarkReserve_Concurrent(b *testing.B) {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	defer func() { _ = q.Stop() }()
	_ = q.Start()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 添加任务
			id, _ := q.Put("bench", []byte("data"), 1, 0, 60*time.Second)

			// Reserve 任务
			delayJob, err := q.Reserve([]string{"bench"}, 100*time.Millisecond)
			if err == nil && delayJob != nil {
				_ = q.Delete(delayJob.Meta.ID)
			} else {
				// 如果没拿到，删除自己添加的
				_ = q.Delete(id)
			}
		}
	})
}

// TestStress_HighConcurrency 高并发压力测试
// 测试场景：100 个生产者 + 100 个消费者同时工作
func TestStress_HighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		const (
			producers            = 100
			consumers            = 100
			delayJobsPerProducer = 100
			totalDelayJobs       = producers * delayJobsPerProducer
		)

		var (
			produced atomic.Int64
			consumed atomic.Int64
			failed   atomic.Int64
		)

		startTime := time.Now()

		// 启动生产者
		var producerWg sync.WaitGroup
		for i := range producers {
			producerWg.Add(1)
			go func(id int) {
				defer producerWg.Done()
				for j := range delayJobsPerProducer {
					_, err := q.Put(
						fmt.Sprintf("topic-%d", id%10), // 10 个不同的 topic
						fmt.Appendf(nil, "delayJob-%d-%d", id, j),
						uint32(j%10), // 0-9 优先级
						0,
						5*time.Second,
					)
					if err != nil {
						failed.Add(1)
					} else {
						produced.Add(1)
					}
				}
			}(i)
		}

		// 启动消费者
		var consumerWg sync.WaitGroup
		topics := make([]string, 10)
		for i := range 10 {
			topics[i] = fmt.Sprintf("topic-%d", i)
		}

		for i := range consumers {
			consumerWg.Add(1)
			go func(id int) {
				defer consumerWg.Done()
				for {
					// 先检查是否已完成（使用 > 而不是 >=，因为可能正好等于）
					if consumed.Load() >= totalDelayJobs {
						return
					}

					delayJob, err := q.Reserve(topics, 100*time.Millisecond)
					if err == ErrTimeout {
						// 超时，再次检查是否所有任务都已完成
						if consumed.Load() >= totalDelayJobs {
							return
						}
						continue
					}
					if err != nil {
						failed.Add(1)
						continue
					}

					// 模拟处理
					// time.Sleep(time.Microsecond)

					// 先增加计数，如果超过限制则不 Delete（释放回队列）
					newCount := consumed.Add(1)
					if newCount > totalDelayJobs {
						// 超过限制，释放任务回队列
						_ = delayJob.Release(delayJob.Meta.Priority, 0)
						consumed.Add(-1) // 回退计数
						return
					}

					if err := delayJob.Delete(); err != nil {
						failed.Add(1)
						consumed.Add(-1) // Delete 失败，回退计数
					}
				}
			}(i)
		}

		// 等待生产者完成
		producerWg.Wait()
		t.Logf("所有生产者完成，已生产 %d 个任务", produced.Load())

		// 等待消费者完成
		consumerWg.Wait()
		elapsed := time.Since(startTime)

		t.Logf("高并发测试完成:")
		t.Logf("  - 总任务数: %d", totalDelayJobs)
		t.Logf("  - 已生产: %d", produced.Load())
		t.Logf("  - 已消费: %d", consumed.Load())
		t.Logf("  - 失败: %d", failed.Load())
		t.Logf("  - 耗时: %v", elapsed)
		t.Logf("  - 吞吐量: %.0f delayJobs/sec", float64(totalDelayJobs)/elapsed.Seconds())

		if failed.Load() > 0 {
			t.Errorf("有 %d 个任务失败", failed.Load())
		}

		if consumed.Load() != totalDelayJobs {
			t.Errorf("期望消费 %d 个任务，实际消费 %d", totalDelayJobs, consumed.Load())
		}
	})
}

// TestStress_BurstLoad 突发负载测试
func TestStress_BurstLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		const burstSize = 10000

		startTime := time.Now()

		// 突发写入
		var wg sync.WaitGroup
		for i := range burstSize {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				_, _ = q.Put("burst", fmt.Appendf(nil, "delayJob-%d", id), 1, 0, 5*time.Second)
			}(i)
		}
		wg.Wait()
		writeTime := time.Since(startTime)

		t.Logf("突发写入 %d 个任务耗时: %v (%.0f delayJobs/sec)",
			burstSize, writeTime, float64(burstSize)/writeTime.Seconds())

		// 突发读取
		startTime = time.Now()
		var consumed atomic.Int32
		for range 100 {
			wg.Go(func() {
				for {
					delayJob, err := q.Reserve([]string{"burst"}, 10*time.Millisecond)
					if err == ErrTimeout {
						return
					}
					if err == nil {
						_ = delayJob.Delete()
						consumed.Add(1)
					}
				}
			})
		}
		wg.Wait()
		readTime := time.Since(startTime)

		t.Logf("突发读取 %d 个任务耗时: %v (%.0f delayJobs/sec)",
			consumed.Load(), readTime, float64(consumed.Load())/readTime.Seconds())
	})
}

// TestStress_DelayedDelayJobs 大量延迟任务测试
func TestStress_DelayedDelayJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 等待恢复完成，确保初始状态检查的准确性
		if err := q.WaitForRecovery(5 * time.Second); err != nil {
			t.Fatalf("WaitForRecovery failed: %v", err)
		}

		// 检查初始状态是否为空（确保测试隔离）
		initialStats := q.Stats()
		if initialStats.TotalDelayJobs != 0 {
			t.Fatalf("初始状态不为空: TotalDelayJobs=%d, 可能是测试隔离问题", initialStats.TotalDelayJobs)
		}

		const delayJobCount = 5000

		// 添加大量延迟任务（延迟 100-500ms）
		var wg sync.WaitGroup
		for i := range delayJobCount {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				delay := time.Duration(100+id%400) * time.Millisecond
				_, _ = q.Put("delayed", fmt.Appendf(nil, "delayJob-%d", id), 1, delay, 5*time.Second)
			}(i)
		}
		wg.Wait()

		t.Logf("已添加 %d 个延迟任务", delayJobCount)

		// 等待所有任务就绪
		time.Sleep(600 * time.Millisecond)

		// 统计
		stats := q.Stats()
		t.Logf("统计: Ready=%d, Delayed=%d", stats.ReadyDelayJobs, stats.DelayedDelayJobs)

		if stats.ReadyDelayJobs+stats.DelayedDelayJobs != delayJobCount {
			t.Errorf("任务数量不匹配: Ready=%d, Delayed=%d, Total=%d",
				stats.ReadyDelayJobs, stats.DelayedDelayJobs, delayJobCount)
		}
	})
}

// BenchmarkPut 基准测试：Put 操作
// BenchmarkPut-24    	   31119	     38893 ns/op
func BenchmarkPut(b *testing.B) {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = q.Put("bench", []byte("data"), 1, 0, 60*time.Second)
			i++
		}
	})
}

// BenchmarkPutMySQL-24    	     267	   4582648 ns/op
func BenchmarkPutMySQL(b *testing.B) {
	// 请确保已正确配置 MySQL 存储
	config := DefaultConfig()
	mysqlStorage, err := NewMySQLStorage("root:Root@123456@tcp(100.108.24.7:3306)/delay?charset=utf8mb4&parseTime=true")
	if err != nil {
		b.Fatalf("NewMySQLStorage failed: %v", err)
	}
	defer func() { _ = mysqlStorage.Close() }()

	config.Storage = mysqlStorage

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = q.Put("bench", []byte("data"), 1, 0, 60*time.Second)
			i++
		}
	})
}

// BenchmarkReserve 基准测试：Reserve 操作
// BenchmarkReserve-24    	  994315	      2002 ns/op
func BenchmarkReserve(b *testing.B) {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 预填充任务（在计时前完成）
	for i := 0; i < b.N; i++ {
		_, _ = q.Put("bench", []byte("data"), 1, 0, 60*time.Second)
	}

	// 重新开始计时，执行 Reserve 操作
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = q.Reserve([]string{"bench"}, 1*time.Second)
	}
}

// BenchmarkReserveDelete 基准测试：Reserve + Delete 操作
// BenchmarkReserveDelete-24    	  595802	      2979 ns/op
func BenchmarkReserveDelete(b *testing.B) {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 预填充任务
	for i := 0; i < b.N; i++ {
		_, _ = q.Put("bench", []byte("data"), 1, 0, 60*time.Second)
	}
	b.ResetTimer()
	// 执行 Reserve + Delete 操作
	for i := 0; i < b.N; i++ {
		delayJob, err := q.Reserve([]string{"bench"}, 1*time.Second)
		if err == nil {
			_ = q.Delete(delayJob.Meta.ID)
		}
	}
}

// TestAsyncPut 测试异步 Put 功能
func TestAsyncPut(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}

		// 启动队列
		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		// 快速发布多个任务
		const numDelayJobs = 100
		delayJobIDs := make([]uint64, numDelayJobs)

		start := time.Now()
		for i := range numDelayJobs {
			id, err := q.Put("test", []byte("body"), 10, 0, 30*time.Second)
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}
			delayJobIDs[i] = id
		}
		putDuration := time.Since(start)

		// 异步 Put 应该很快（<100ms for 100 delayJobs）
		// 同步模式每个 Put 需要 1-10ms，100 个任务需要 100-1000ms
		// 异步模式应该 <50ms
		t.Logf("Put %d delayJobs took: %v (avg: %v per delayJob)", numDelayJobs, putDuration, putDuration/numDelayJobs)

		if putDuration > 200*time.Millisecond {
			t.Errorf("Async Put too slow: %v (expected <200ms)", putDuration)
		}

		// 等待异步存储完成
		time.Sleep(500 * time.Millisecond)

		// 验证所有任务都在队列中（使用 >= 因为可能有恢复的任务）
		stats := q.Stats()
		if stats.TotalDelayJobs < numDelayJobs {
			t.Errorf("TotalDelayJobs = %d, want at least %d", stats.TotalDelayJobs, numDelayJobs)
		}

		// 验证可以 Reserve 任务
		delayJob, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Errorf("Reserve failed: %v", err)
		}
		if delayJob == nil {
			t.Error("Reserve returned nil delayJob")
		} else {
			_ = delayJob.Delete()
		}
	})
}

// TestAsyncPutStop 测试停止时等待异步 Put 完成
func TestAsyncPutStop(t *testing.T) {
	// 创建临时 SQLite 数据库
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage failed: %v", err)
	}
	defer func() { _ = storage.Close() }()

	config := DefaultConfig()
	config.Storage = storage

	q, err := New(config)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := q.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// 快速发布任务
	const numDelayJobs = 50
	for range numDelayJobs {
		_, err := q.Put("test", []byte("body"), 10, 0, 30*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// 立即停止（应该等待异步写入完成）
	if err := q.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// 重新打开存储，检查任务是否都被保存
	storage2, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage failed: %v", err)
	}
	defer func() { _ = storage2.Close() }()

	// 扫描所有任务
	result, err := storage2.ScanDelayJobMeta(context.Background(), nil)
	if err != nil {
		t.Fatalf("ScanDelayJobMeta failed: %v", err)
	}

	// 验证所有任务都被保存
	if len(result.Metas) != numDelayJobs {
		t.Errorf("Saved delayJobs = %d, want %d", len(result.Metas), numDelayJobs)
	}
}

// TestAsyncPutChannelFull 测试通道满时 Put 会阻塞等待
func TestAsyncPutChannelFull(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage failed: %v", err)
	}
	defer func() { _ = storage.Close() }()

	config := DefaultConfig()
	config.Storage = storage

	q, err := New(config)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// 启动队列
	if err := q.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = q.Stop() }()

	// 发布大量任务测试批量处理
	const numDelayJobs = 1100
	for i := range numDelayJobs {
		_, err := q.Put("test", []byte("body"), 10, 0, 30*time.Second)
		if err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}

	// 验证所有任务都在内存队列中
	stats := q.Stats()
	if stats.TotalDelayJobs != numDelayJobs {
		t.Errorf("TotalDelayJobs = %d, want %d", stats.TotalDelayJobs, numDelayJobs)
	}
}

// TestReserve_TTRTimeout 测试 TTR 超时自动恢复
func TestReserve_TTRTimeout(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		// 使用 TimeWheelTicker 确保 TTR 超时能被及时处理
		config.Ticker = NewTimeWheelTicker(10*time.Millisecond, 100)

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布一个 TTR 很短的任务
		delayJobID, err := q.Put("test", []byte("timeout-task"), 1, 0, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Worker 1 获取任务但不处理
		delayJob1, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		if delayJob1.Meta.ID != delayJobID {
			t.Errorf("delayJob.ID = %d, want %d", delayJob1.Meta.ID, delayJobID)
		}

		// 验证任务状态为 Reserved
		stats := q.Stats()
		if stats.ReservedDelayJobs != 1 {
			t.Errorf("ReservedDelayJobs = %d, want 1", stats.ReservedDelayJobs)
		}

		// 等待 TTR 超时（100ms + 一些余量）
		time.Sleep(200 * time.Millisecond)

		// 验证任务已经自动转为 Ready
		stats = q.Stats()
		if stats.ReservedDelayJobs != 0 {
			t.Errorf("ReservedDelayJobs = %d, want 0 (should timeout)", stats.ReservedDelayJobs)
		}
		if stats.ReadyDelayJobs != 1 {
			t.Errorf("ReadyDelayJobs = %d, want 1 (should be released)", stats.ReadyDelayJobs)
		}

		// Worker 2 应该能够重新获取这个任务
		delayJob2, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Second Reserve failed: %v", err)
		}

		if delayJob2.Meta.ID != delayJobID {
			t.Errorf("second delayJob.ID = %d, want %d", delayJob2.Meta.ID, delayJobID)
		}

		// 验证 Reserves 计数增加
		if delayJob2.Meta.Reserves != 2 {
			t.Errorf("Reserves = %d, want 2", delayJob2.Meta.Reserves)
		}

		// 清理
		_ = delayJob2.Delete()
	})
}

// TestReserve_TTRTimeout_Multiple 测试多个任务 TTR 超时
func TestReserve_TTRTimeout_Multiple(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewTimeWheelTicker(10*time.Millisecond, 100)

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布多个任务
		const numDelayJobs = 5
		for i := 0; i < numDelayJobs; i++ {
			_, err := q.Put("test", []byte("timeout"), 1, 0, 100*time.Millisecond)
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Reserve 所有任务但不处理
		delayJobs := make([]*DelayJob, numDelayJobs)
		for i := 0; i < numDelayJobs; i++ {
			delayJob, err := q.Reserve([]string{"test"}, 1*time.Second)
			if err != nil {
				t.Fatalf("Reserve %d failed: %v", i, err)
			}
			delayJobs[i] = delayJob
		}

		// 验证所有任务都是 Reserved 状态
		stats := q.Stats()
		if stats.ReservedDelayJobs != numDelayJobs {
			t.Errorf("ReservedDelayJobs = %d, want %d", stats.ReservedDelayJobs, numDelayJobs)
		}

		// 等待所有任务 TTR 超时
		time.Sleep(200 * time.Millisecond)

		// 验证所有任务都转为 Ready
		stats = q.Stats()
		if stats.ReservedDelayJobs != 0 {
			t.Errorf("ReservedDelayJobs = %d, want 0", stats.ReservedDelayJobs)
		}
		if stats.ReadyDelayJobs != numDelayJobs {
			t.Errorf("ReadyDelayJobs = %d, want %d", stats.ReadyDelayJobs, numDelayJobs)
		}

		// 验证可以重新获取所有任务
		for i := 0; i < numDelayJobs; i++ {
			delayJob, err := q.Reserve([]string{"test"}, 1*time.Second)
			if err != nil {
				t.Fatalf("Second Reserve %d failed: %v", i, err)
			}
			if delayJob.Meta.Reserves != 2 {
				t.Errorf("delayJob %d Reserves = %d, want 2", i, delayJob.Meta.Reserves)
			}
			_ = delayJob.Delete()
		}
	})
}
