package delaygo

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestDeadlock_SQLite(t *testing.T) {
	dbPath := "deadlock_test.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	// Create SQLite storage
	storage, err := NewSQLiteStorage(dbPath, WithMaxBatchSize(10), WithMaxBatchBytes(1024*1024))
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	// Create Config
	config := DefaultConfig()
	config.Storage = storage
	config.Ticker = NewTimeWheelTicker(10*time.Millisecond, 100)

	// Create Queue
	q, err := New(config)
	if err != nil {
		t.Fatal(err)
	}

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	var wg sync.WaitGroup
	topic := "test-topic"

	// Producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, err := q.Put(topic, []byte(fmt.Sprintf("job-%d", i)), 1, 0, 10*time.Second)
			if err != nil {
				t.Errorf("Put error: %v", err)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			job, err := q.Reserve([]string{topic}, 5*time.Second)
			if err != nil {
				t.Errorf("Reserve error: %v", err)
				continue
			}

			// Simulate work
			time.Sleep(2 * time.Millisecond)

			if err := q.Delete(job.Meta.ID); err != nil {
				t.Errorf("Delete error: %v", err)
			}
		}
	}()

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Test finished successfully")
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out - possible deadlock")
	}
}
