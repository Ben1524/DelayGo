package task

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/Ben1524/delaygo"
)

// Handler 泛型任务处理器接口
type Handler[T any] interface {
	// Handle 处理任务
	// 返回 error 表示处理失败
	Handle(ctx context.Context, task *Task[T]) error
}

// HandlerFunc 函数适配器
type HandlerFunc[T any] func(ctx context.Context, task *Task[T]) error

func (f HandlerFunc[T]) Handle(ctx context.Context, task *Task[T]) error {
	return f(ctx, task)
}

// Worker 泛型任务工作者
type Worker[T any] struct {
	queue   *delaygo.DelayQueue
	topic   string
	handler Handler[T]
	logger  *slog.Logger

	concurrency int
	stopChan    chan struct{}
}

// NewWorker 创建新的工作者
func NewWorker[T any](queue *delaygo.DelayQueue, topic string, handler Handler[T], opts ...WorkerOption[T]) *Worker[T] {
	w := &Worker[T]{
		queue:       queue,
		topic:       topic,
		handler:     handler,
		logger:      slog.Default(),
		concurrency: 1,
		stopChan:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

type WorkerOption[T any] func(*Worker[T])

func WithLogger[T any](logger *slog.Logger) WorkerOption[T] {
	return func(w *Worker[T]) {
		w.logger = logger
	}
}

func WithConcurrency[T any](n int) WorkerOption[T] {
	return func(w *Worker[T]) {
		if n > 0 {
			w.concurrency = n
		}
	}
}

// Start 启动工作者
func (w *Worker[T]) Start(ctx context.Context) {
	for i := 0; i < w.concurrency; i++ {
		go w.run(ctx, i)
	}
}

// Stop 停止工作者
func (w *Worker[T]) Stop() {
	close(w.stopChan)
}

func (w *Worker[T]) run(ctx context.Context, id int) {
	w.logger.Info("worker started", "id", id, "topic", w.topic)

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopChan:
			return
		default:
			// 阻塞获取任务
			job, err := w.queue.Reserve([]string{w.topic}, 5*time.Second)
			if err != nil {
				// 忽略超时错误，继续轮询
				if err.Error() != "delaygo: timeout" { // TODO: Use exported error var if available or check string
					// delaygo.ErrTimeout is exported, let's use it if possible.
					// But we need to check if it matches.
					// Since we imported delaygo, we can use delaygo.ErrTimeout
					if err != delaygo.ErrTimeout {
						w.logger.Error("reserve failed", "error", err)
					}
				}
				continue
			}

			if job == nil {
				continue
			}

			// 反序列化
			var payload T
			if err := json.Unmarshal(job.Body(), &payload); err != nil {
				w.logger.Error("unmarshal failed", "error", err, "job_id", job.Meta.ID)
				// 数据错误，直接 Bury
				_ = w.queue.Bury(job.Meta.ID, job.Meta.Priority)
				continue
			}

			task := &Task[T]{
				ID:       job.Meta.ID,
				Topic:    job.Meta.Topic,
				Payload:  payload,
				Delay:    job.Meta.Delay,
				TTR:      job.Meta.TTR,
				Priority: job.Meta.Priority,
			}

			// 执行处理
			if err := w.handler.Handle(ctx, task); err != nil {
				w.logger.Error("handle failed", "error", err, "job_id", job.Meta.ID)
				// 处理失败，Bury
				_ = w.queue.Bury(job.Meta.ID, job.Meta.Priority)
			} else {
				// 处理成功，删除任务
				if err := w.queue.Delete(job.Meta.ID); err != nil {
					w.logger.Error("delete failed", "error", err, "job_id", job.Meta.ID)
				}
			}
		}
	}
}
