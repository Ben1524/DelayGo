package task

import (
	json "github.com/bytedance/sonic"
	"time"
)

// Task 泛型任务包装器
// T 是任务数据的类型
// @Description 泛型任务包装器
type Task[T any] struct {
	// @Description 任务 ID
	ID uint64 `json:"id"`
	// @Description 所属 topic
	Topic string `json:"topic"`
	// @Description 任务载荷
	Payload T `json:"payload"`
	// @Description 延迟时间
	Delay time.Duration `json:"delay"`
	// @Description TTR
	TTR time.Duration `json:"ttr"`
	// @Description 优先级
	Priority uint32 `json:"priority"`
}

// NewTask 创建新的任务
func NewTask[T any](topic string, payload T, opts ...TaskOption[T]) *Task[T] {
	t := &Task[T]{
		Topic:   topic,
		Payload: payload,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

type TaskOption[T any] func(*Task[T])

func WithDelay[T any](d time.Duration) TaskOption[T] {
	return func(t *Task[T]) {
		t.Delay = d
	}
}

func WithTTR[T any](d time.Duration) TaskOption[T] {
	return func(t *Task[T]) {
		t.TTR = d
	}
}

func WithPriority[T any](p uint32) TaskOption[T] {
	return func(t *Task[T]) {
		t.Priority = p
	}
}

// Serialize 序列化任务数据
func (t *Task[T]) Serialize() ([]byte, error) {
	return json.Marshal(t.Payload)
}

// SerializePayload 序列化任务数据 (helper function)
func SerializePayload[T any](payload T) ([]byte, error) {
	return json.Marshal(payload)
}

// DeserializePayload 反序列化任务数据
func DeserializePayload[T any](data []byte) (T, error) {
	var payload T
	err := json.Unmarshal(data, &payload)
	return payload, err
}
