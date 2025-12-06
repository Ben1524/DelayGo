package task

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Ben1524/delaygo"
)

type TestPayload struct {
	Message string `json:"message"`
	Count   int    `json:"count"`
}

func TestTaskSerialization(t *testing.T) {
	payload := TestPayload{
		Message: "hello",
		Count:   42,
	}

	task := NewTask("test-topic", payload,
		WithDelay[TestPayload](time.Second),
		WithTTR[TestPayload](30*time.Second),
		WithPriority[TestPayload](10),
	)

	data, err := task.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	var decoded TestPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Message != payload.Message {
		t.Errorf("Message mismatch: got %s, want %s", decoded.Message, payload.Message)
	}
	if decoded.Count != payload.Count {
		t.Errorf("Count mismatch: got %d, want %d", decoded.Count, payload.Count)
	}
}

func TestWorker(t *testing.T) {
	// Setup DelayQueue (using memory storage for testing)
	dq, err := delaygo.New(delaygo.DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create DelayQueue: %v", err)
	}
	defer dq.Stop()

	topic := "test-worker-topic"
	payload := TestPayload{Message: "work", Count: 1}

	// Create and push a task
	task := NewTask(topic, payload)
	data, _ := task.Serialize()
	jobID, err := dq.Put(topic, data, 0, 0, 10*time.Second)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Setup Worker
	done := make(chan struct{})
	handler := HandlerFunc[TestPayload](func(ctx context.Context, tsk *Task[TestPayload]) error {
		if tsk.ID != jobID {
			t.Errorf("Task ID mismatch: got %d, want %d", tsk.ID, jobID)
		}
		if tsk.Payload.Message != "work" {
			t.Errorf("Payload mismatch: got %v", tsk.Payload)
		}
		close(done)
		return nil
	})

	worker := NewWorker(dq, topic, handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker.Start(ctx)
	defer worker.Stop()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Worker timed out")
	}
}
