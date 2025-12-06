package delaygo

import (
	"testing"
	"time"
)

func TestNewTopic(t *testing.T) {
	topic := newTopic("test")
	if topic == nil {
		t.Fatal("newTopic returned nil")
	}

	if topic.name != "test" {
		t.Errorf("name = %s, want test", topic.name)
	}

	if topic.ready == nil {
		t.Error("ready heap should be initialized")
	}

	if topic.delayed == nil {
		t.Error("delayed heap should be initialized")
	}

	if topic.reserved == nil {
		t.Error("reserved map should be initialized")
	}

	if topic.buried == nil {
		t.Error("buried heap should be initialized")
	}
}

func TestTopicReadyOperations(t *testing.T) {
	topic := newTopic("test")

	// Push ready delayJobs with different priorities
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateReady
	meta2 := NewDelayJobMeta(2, "test", 5, 0, 30*time.Second)
	meta2.DelayState = DelayStateReady
	meta3 := NewDelayJobMeta(3, "test", 15, 0, 30*time.Second)
	meta3.DelayState = DelayStateReady

	topic.pushReady(meta1)
	topic.pushReady(meta2)
	topic.pushReady(meta3)

	// Peek should return highest priority (lowest number)
	peeked := topic.peekReady()
	if peeked.ID != 2 {
		t.Errorf("peekReady ID = %d, want 2 (highest priority)", peeked.ID)
	}

	// Pop should return in priority order
	popped := topic.popReady()
	if popped.ID != 2 {
		t.Errorf("popReady ID = %d, want 2", popped.ID)
	}

	popped = topic.popReady()
	if popped.ID != 1 {
		t.Errorf("popReady ID = %d, want 1", popped.ID)
	}

	popped = topic.popReady()
	if popped.ID != 3 {
		t.Errorf("popReady ID = %d, want 3", popped.ID)
	}

	// Pop from empty should return nil
	popped = topic.popReady()
	if popped != nil {
		t.Error("popReady from empty should return nil")
	}

	// Peek from empty should return nil
	peeked = topic.peekReady()
	if peeked != nil {
		t.Error("peekReady from empty should return nil")
	}

	// Test removeReady
	meta4 := NewDelayJobMeta(4, "test", 10, 0, 30*time.Second)
	meta5 := NewDelayJobMeta(5, "test", 20, 0, 30*time.Second)
	topic.pushReady(meta4)
	topic.pushReady(meta5)

	removed := topic.removeReady(4)
	if !removed {
		t.Error("removeReady should return true")
	}

	// Only meta5 should remain
	popped = topic.popReady()
	if popped.ID != 5 {
		t.Errorf("popReady after remove ID = %d, want 5", popped.ID)
	}
}

func TestTopicDelayedOperations(t *testing.T) {
	topic := newTopic("test")
	now := time.Now()

	// Push delayed delayJobs with different ready times
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateDelayed
	meta1.ReadyAt = now.Add(10 * time.Second)

	meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.DelayState = DelayStateDelayed
	meta2.ReadyAt = now.Add(5 * time.Second)

	meta3 := NewDelayJobMeta(3, "test", 10, 0, 30*time.Second)
	meta3.DelayState = DelayStateDelayed
	meta3.ReadyAt = now.Add(15 * time.Second)

	topic.pushDelayed(meta1)
	topic.pushDelayed(meta2)
	topic.pushDelayed(meta3)

	// Peek should return earliest ready time
	peeked := topic.peekDelayed()
	if peeked.ID != 2 {
		t.Errorf("peekDelayed ID = %d, want 2 (earliest)", peeked.ID)
	}

	// Pop should return in time order
	popped := topic.popDelayed()
	if popped.ID != 2 {
		t.Errorf("popDelayed ID = %d, want 2", popped.ID)
	}

	// Remove specific delayJob
	removed := topic.removeDelayed(1)
	if !removed {
		t.Error("removeDelayed should return true")
	}

	popped = topic.popDelayed()
	if popped.ID != 3 {
		t.Errorf("popDelayed after remove ID = %d, want 3", popped.ID)
	}

	// Pop from empty should return nil
	popped = topic.popDelayed()
	if popped != nil {
		t.Error("popDelayed from empty should return nil")
	}
}

func TestTopicReservedOperations(t *testing.T) {
	topic := newTopic("test")

	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateReserved

	// Add reserved
	topic.addReserved(meta1)

	// Get reserved
	got := topic.getReserved(1)
	if got != meta1 {
		t.Error("getReserved should return the meta")
	}

	// Get non-existent
	got = topic.getReserved(999)
	if got != nil {
		t.Error("getReserved non-existent should return nil")
	}

	// Remove reserved
	removed := topic.removeReserved(1)
	if removed != meta1 {
		t.Error("removeReserved should return the meta")
	}

	// Verify removal
	got = topic.getReserved(1)
	if got != nil {
		t.Error("getReserved after remove should return nil")
	}
}

func TestTopicBuriedOperations(t *testing.T) {
	topic := newTopic("test")

	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateBuried
	meta2 := NewDelayJobMeta(2, "test", 5, 0, 30*time.Second)
	meta2.DelayState = DelayStateBuried

	topic.pushBuried(meta1)
	topic.pushBuried(meta2)

	// Peek should return highest priority
	peeked := topic.peekBuried()
	if peeked.ID != 2 {
		t.Errorf("peekBuried ID = %d, want 2", peeked.ID)
	}

	// Pop should return in priority order
	popped := topic.popBuried()
	if popped.ID != 2 {
		t.Errorf("popBuried ID = %d, want 2", popped.ID)
	}

	// Remove specific delayJob
	meta3 := NewDelayJobMeta(3, "test", 15, 0, 30*time.Second)
	topic.pushBuried(meta3)

	removed := topic.removeBuried(1)
	if !removed {
		t.Error("removeBuried should return true")
	}

	popped = topic.popBuried()
	if popped.ID != 3 {
		t.Errorf("popBuried after remove ID = %d, want 3", popped.ID)
	}
}

func TestTopicStats(t *testing.T) {
	topic := newTopic("test")

	// Add delayJobs to different queues
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

	stats := topic.stats()

	if stats.Name != "test" {
		t.Errorf("Name = %s, want test", stats.Name)
	}

	if stats.ReadyDelayJobs != 1 {
		t.Errorf("ReadyDelayJobs = %d, want 1", stats.ReadyDelayJobs)
	}

	if stats.DelayedDelayJobs != 1 {
		t.Errorf("DelayedDelayJobs = %d, want 1", stats.DelayedDelayJobs)
	}

	if stats.ReservedDelayJobs != 1 {
		t.Errorf("ReservedDelayJobs = %d, want 1", stats.ReservedDelayJobs)
	}

	if stats.BuriedDelayJobs != 1 {
		t.Errorf("BuriedDelayJobs = %d, want 1", stats.BuriedDelayJobs)
	}

	if stats.TotalDelayJobs != 4 {
		t.Errorf("TotalDelayJobs = %d, want 4", stats.TotalDelayJobs)
	}
}

func TestTopicProcessTick(t *testing.T) {
	topic := newTopic("test")
	now := time.Now()

	// Add delayed delayJob that should be ready
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateDelayed
	meta1.ReadyAt = now.Add(-1 * time.Second) // Past
	topic.pushDelayed(meta1)

	// Add reserved delayJob that should timeout
	meta2 := NewDelayJobMeta(2, "test", 10, 0, 1*time.Second)
	meta2.DelayState = DelayStateReserved
	meta2.ReservedAt = now.Add(-2 * time.Second) // 2 seconds ago, TTR is 1s
	topic.addReserved(meta2)

	// Process tick
	topic.ProcessTick(now)

	// meta1 should now be in ready queue
	if topic.delayed.Len() != 0 {
		t.Errorf("delayed queue should be empty, got %d", topic.delayed.Len())
	}

	if topic.ready.Len() != 2 {
		t.Errorf("ready queue should have 2, got %d", topic.ready.Len())
	}

	// meta2 should be moved from reserved to ready
	if len(topic.reserved) != 0 {
		t.Errorf("reserved map should be empty, got %d", len(topic.reserved))
	}
}

func TestTopicNextTickTime(t *testing.T) {
	topic := newTopic("test")
	now := time.Now()

	// Empty topic should return zero time
	nextTime := topic.NextTickTime()
	if !nextTime.IsZero() {
		t.Error("NextTickTime for empty topic should be zero")
	}

	// Add delayed delayJob
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateDelayed
	meta1.ReadyAt = now.Add(10 * time.Second)
	topic.pushDelayed(meta1)

	nextTime = topic.NextTickTime()
	if nextTime.IsZero() {
		t.Error("NextTickTime should not be zero with delayed delayJob")
	}

	// Add reserved delayJob with earlier deadline
	meta2 := NewDelayJobMeta(2, "test", 10, 0, 5*time.Second)
	meta2.DelayState = DelayStateReserved
	meta2.ReservedAt = now
	topic.addReserved(meta2)

	nextTime = topic.NextTickTime()
	// Should return the earlier of delayed.ReadyAt and reserved deadline
	deadline := meta2.ReserveDeadline()
	if nextTime.After(deadline.Add(time.Millisecond)) {
		t.Error("NextTickTime should return the earlier deadline")
	}
}

func TestTopicNeedsTick(t *testing.T) {
	topic := newTopic("test")

	// Empty topic doesn't need tick
	if topic.NeedsTick() {
		t.Error("Empty topic should not need tick")
	}

	// Add ready delayJob - still doesn't need tick
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateReady
	topic.pushReady(meta1)

	if topic.NeedsTick() {
		t.Error("Topic with only ready delayJobs should not need tick")
	}

	// Add delayed delayJob - needs tick
	meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.DelayState = DelayStateDelayed
	topic.pushDelayed(meta2)

	if !topic.NeedsTick() {
		t.Error("Topic with delayed delayJobs should need tick")
	}

	// Clear delayed, add reserved - still needs tick
	topic.popDelayed()

	meta3 := NewDelayJobMeta(3, "test", 10, 0, 30*time.Second)
	meta3.DelayState = DelayStateReserved
	topic.addReserved(meta3)

	if !topic.NeedsTick() {
		t.Error("Topic with reserved delayJobs should need tick")
	}
}

func TestTopicProcessDelayed(t *testing.T) {
	topic := newTopic("test")
	now := time.Now()

	// Add multiple delayed delayJobs, some ready, some not
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.DelayState = DelayStateDelayed
	meta1.ReadyAt = now.Add(-1 * time.Second) // Ready

	meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.DelayState = DelayStateDelayed
	meta2.ReadyAt = now.Add(10 * time.Second) // Not ready

	meta3 := NewDelayJobMeta(3, "test", 10, 0, 30*time.Second)
	meta3.DelayState = DelayStateDelayed
	meta3.ReadyAt = now.Add(-2 * time.Second) // Ready

	topic.pushDelayed(meta1)
	topic.pushDelayed(meta2)
	topic.pushDelayed(meta3)

	topic.processDelayed(now)

	// 2 should be ready, 1 still delayed
	if topic.ready.Len() != 2 {
		t.Errorf("ready queue should have 2, got %d", topic.ready.Len())
	}

	if topic.delayed.Len() != 1 {
		t.Errorf("delayed queue should have 1, got %d", topic.delayed.Len())
	}
}

func TestTopicProcessReservedTimeout(t *testing.T) {
	topic := newTopic("test")
	now := time.Now()

	// Add reserved delayJobs, some timeout, some not
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 1*time.Second)
	meta1.DelayState = DelayStateReserved
	meta1.ReservedAt = now.Add(-2 * time.Second) // Timeout

	meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.DelayState = DelayStateReserved
	meta2.ReservedAt = now // Not timeout

	meta3 := NewDelayJobMeta(3, "test", 10, 0, 1*time.Second)
	meta3.DelayState = DelayStateReserved
	meta3.ReservedAt = now.Add(-3 * time.Second) // Timeout

	topic.addReserved(meta1)
	topic.addReserved(meta2)
	topic.addReserved(meta3)

	topic.processReservedTimeout(now)

	// 2 should timeout and move to ready, 1 still reserved
	if topic.ready.Len() != 2 {
		t.Errorf("ready queue should have 2, got %d", topic.ready.Len())
	}

	if len(topic.reserved) != 1 {
		t.Errorf("reserved map should have 1, got %d", len(topic.reserved))
	}

	// Timeout delayJobs should have increased counters
	// Pop and check
	popped := topic.popReady()
	if popped.Timeouts != 1 {
		t.Errorf("Timeouts = %d, want 1", popped.Timeouts)
	}
	if popped.Releases != 1 {
		t.Errorf("Releases = %d, want 1", popped.Releases)
	}
}

func TestDelayJobMetaHeapFIFO(t *testing.T) {
	topic := newTopic("test")

	// Add delayJobs with same priority
	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
	meta3 := NewDelayJobMeta(3, "test", 10, 0, 30*time.Second)

	topic.pushReady(meta1)
	topic.pushReady(meta2)
	topic.pushReady(meta3)

	// Should pop in FIFO order (by ID since same priority)
	popped := topic.popReady()
	if popped.ID != 1 {
		t.Errorf("First pop ID = %d, want 1", popped.ID)
	}

	popped = topic.popReady()
	if popped.ID != 2 {
		t.Errorf("Second pop ID = %d, want 2", popped.ID)
	}

	popped = topic.popReady()
	if popped.ID != 3 {
		t.Errorf("Third pop ID = %d, want 3", popped.ID)
	}
}

func TestDelayJobMetaHeapFind(t *testing.T) {
	h := newDelayJobMetaHeap()

	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta2 := NewDelayJobMeta(2, "test", 5, 0, 30*time.Second)

	h.Push(meta1)
	h.Push(meta2)

	// Find existing
	found := h.find(1)
	if found != meta1 {
		t.Error("find should return meta1")
	}

	found = h.find(2)
	if found != meta2 {
		t.Error("find should return meta2")
	}

	// Find non-existent
	found = h.find(999)
	if found != nil {
		t.Error("find non-existent should return nil")
	}
}

func TestDelayedDelayJobHeapFind(t *testing.T) {
	h := newDelayJobHeap()
	now := time.Now()

	meta1 := NewDelayJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.ReadyAt = now.Add(10 * time.Second)
	meta2 := NewDelayJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.ReadyAt = now.Add(5 * time.Second)

	h.Push(meta1)
	h.Push(meta2)

	// Find existing
	found := h.find(1)
	if found != meta1 {
		t.Error("find should return meta1")
	}

	// Find non-existent
	found = h.find(999)
	if found != nil {
		t.Error("find non-existent should return nil")
	}
}
