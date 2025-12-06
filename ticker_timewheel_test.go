package delaygo

import (
	"testing"
	"time"
)

func TestNewTimeWheelTicker(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)
	if ticker == nil {
		t.Fatal("NewTimeWheelTicker returned nil")
	}

	tw, ok := ticker.(*TimeWheelTicker)
	if !ok {
		t.Fatal("NewTimeWheelTicker did not return *TimeWheelTicker")
	}

	if tw.tickInterval != 10*time.Millisecond {
		t.Errorf("tickInterval = %v, want %v", tw.tickInterval, 10*time.Millisecond)
	}

	if tw.slots != 100 {
		t.Errorf("slots = %d, want %d", tw.slots, 100)
	}

	if len(tw.wheel) != 100 {
		t.Errorf("wheel length = %d, want %d", len(tw.wheel), 100)
	}
}

func TestTimeWheelTickerDefaults(t *testing.T) {
	ticker := NewTimeWheelTicker(0, 0)

	tw, ok := ticker.(*TimeWheelTicker)
	if !ok {
		t.Fatal("NewTimeWheelTicker did not return *TimeWheelTicker")
	}

	if tw.tickInterval != 10*time.Millisecond {
		t.Errorf("default tickInterval = %v, want %v", tw.tickInterval, 10*time.Millisecond)
	}

	if tw.slots != 3600 {
		t.Errorf("default slots = %d, want %d", tw.slots, 3600)
	}
}

func TestTimeWheelTickerRegister(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)
	tw := ticker.(*TimeWheelTicker)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)

	ticker.Register("test", mockObj)

	tw.mu.RLock()
	defer tw.mu.RUnlock()

	if len(tw.registry) != 1 {
		t.Errorf("registry size = %d, want 1", len(tw.registry))
	}

	if _, exists := tw.registry["test"]; !exists {
		t.Error("tickable not registered")
	}

	// Should not be paused after registration
	if tw.paused {
		t.Error("ticker should not be paused after registration")
	}
}

func TestTimeWheelTickerUnregister(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)
	tw := ticker.(*TimeWheelTicker)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)

	ticker.Register("test", mockObj)
	ticker.Unregister("test")

	tw.mu.RLock()
	defer tw.mu.RUnlock()

	if len(tw.registry) != 0 {
		t.Errorf("registry size = %d, want 0", len(tw.registry))
	}
}

func TestTimeWheelTickerStartStop(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)

	ticker.Start()

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	ticker.Stop()
}

func TestTimeWheelTickerStats(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)
	ticker.Register("test", mockObj)

	stats := ticker.Stats()

	if stats.RegisteredCount != 1 {
		t.Errorf("RegisteredCount = %d, want 1", stats.RegisteredCount)
	}

	if stats.Mode != "timewheel" {
		t.Errorf("Mode = %s, want timewheel", stats.Mode)
	}

	if stats.TimeUntilTick <= 0 {
		t.Error("TimeUntilTick should be > 0")
	}
}

func TestTimeWheelTickerProcessing(t *testing.T) {
	ticker := NewTimeWheelTicker(20*time.Millisecond, 100)

	nextTime := time.Now().Add(40 * time.Millisecond)
	mockObj := newMockTickable(nextTime)

	ticker.Register("test", mockObj)
	ticker.Start()
	defer ticker.Stop()

	// Wait for tick to be processed
	time.Sleep(100 * time.Millisecond)

	tickCount := mockObj.GetTickCount()
	if tickCount == 0 {
		t.Error("ProcessTick should have been called")
	}
}

func TestTimeWheelTickerWakeup(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)
	ticker.Register("test", mockObj)

	// Wakeup should reschedule tasks
	ticker.Wakeup()

	// Should not panic
}

func TestTimeWheelTickerMultipleObjects(t *testing.T) {
	ticker := NewTimeWheelTicker(20*time.Millisecond, 100)

	now := time.Now()
	mock1 := newMockTickable(now.Add(30 * time.Millisecond))
	mock2 := newMockTickable(now.Add(60 * time.Millisecond))

	ticker.Register("obj1", mock1)
	ticker.Register("obj2", mock2)

	ticker.Start()
	defer ticker.Stop()

	// Wait for both to be processed
	time.Sleep(100 * time.Millisecond)

	if mock1.GetTickCount() == 0 {
		t.Error("obj1 should have been ticked")
	}

	if mock2.GetTickCount() == 0 {
		t.Error("obj2 should have been ticked")
	}
}

func TestTimeWheelTickerPauseResume(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)
	tw := ticker.(*TimeWheelTicker)

	// Should start paused
	tw.mu.RLock()
	if !tw.paused {
		t.Error("ticker should start paused")
	}
	tw.mu.RUnlock()

	// Register should resume
	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)
	ticker.Register("test", mockObj)

	tw.mu.RLock()
	if tw.paused {
		t.Error("ticker should resume after registration")
	}
	tw.mu.RUnlock()
}

func TestTimeWheelTickerNoObjects(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)

	ticker.Start()
	defer ticker.Stop()

	// Should not panic with no registered objects
	time.Sleep(100 * time.Millisecond)

	stats := ticker.Stats()
	if stats.RegisteredCount != 0 {
		t.Errorf("RegisteredCount = %d, want 0", stats.RegisteredCount)
	}
}

func TestTimeWheelTickerCalculateSlot(t *testing.T) {
	ticker := NewTimeWheelTicker(100*time.Millisecond, 10)
	tw := ticker.(*TimeWheelTicker)

	now := time.Now()
	tw.currentSlot = 0

	// Test various tick times
	tests := []struct {
		tickTime time.Time
		wantSlot int
	}{
		{now, 0},                              // Already expired
		{now.Add(100 * time.Millisecond), 1},  // 1 tick away
		{now.Add(200 * time.Millisecond), 2},  // 2 ticks away
		{now.Add(1000 * time.Millisecond), 0}, // 10 ticks away (wraps to 0)
		{now.Add(-100 * time.Millisecond), 0}, // Past time
	}

	for _, tt := range tests {
		got := tw.calculateSlot(tt.tickTime)
		if got != tt.wantSlot {
			t.Errorf("calculateSlot(%v) = %d, want %d", tt.tickTime.Sub(now), got, tt.wantSlot)
		}
	}
}

func TestTimeWheelTickerHasAnyTask(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 10)
	tw := ticker.(*TimeWheelTicker)

	tw.mu.Lock()
	if tw.hasAnyTask() {
		t.Error("hasAnyTask should return false for empty wheel")
	}

	// Add a task to the wheel
	tw.wheel[0] = append(tw.wheel[0], &tickTask{
		name:     "test",
		tickable: newMockTickable(time.Now()),
	})

	if !tw.hasAnyTask() {
		t.Error("hasAnyTask should return true when tasks exist")
	}
	tw.mu.Unlock()
}

func TestTimeWheelTickerRemoveTask(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 10)
	tw := ticker.(*TimeWheelTicker)

	mockObj := newMockTickable(time.Now().Add(50 * time.Millisecond))
	ticker.Register("test", mockObj)

	tw.mu.Lock()
	tw.removeTask("test")
	tw.mu.Unlock()

	// Verify task is removed from all slots
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	for i := range tw.wheel {
		for _, task := range tw.wheel[i] {
			if task.name == "test" {
				t.Error("task should be removed from all slots")
			}
		}
	}
}
