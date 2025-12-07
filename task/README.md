# Task Package

`task` 包提供了一个基于 Go 泛型的类型安全 API，用于简化 DelayGo 的使用。它封装了底层的 `[]byte` 操作，让你可以直接处理结构化的业务数据。

## 特性

- **类型安全**: 使用泛型 `Task[T]` 包装任务数据，避免手动类型断言。
- **自动序列化**: 内置 JSON 序列化/反序列化支持。
- **Worker 模式**: 提供 `Worker[T]` 结构，简化任务消费者的编写，自动处理任务的获取、反序列化、确认（Delete）和重试（Bury）。
- **并发控制**: 支持设置 Worker 的并发协程数。

## 快速开始

### 1. 定义任务载荷 (Payload)

定义一个结构体来表示你的任务数据。

```go
type EmailPayload struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}
```

### 2. 启动 Worker (消费者)

使用 `task.NewWorker` 创建并启动一个消费者。Worker 会自动从队列中拉取任务，反序列化为 `EmailPayload`，并调用你的 Handler。

```go
import (
    "context"
    "log"
    "github.com/Ben1524/delaygo"
    "github.com/Ben1524/delaygo/task"
)

func main() {
    // 初始化 DelayQueue
    dq, _ := delaygo.New(delaygo.DefaultConfig())
    dq.Start()
    defer dq.Stop()

    // 定义处理逻辑
    handler := task.HandlerFunc[EmailPayload](func(ctx context.Context, t *task.Task[EmailPayload]) error {
        log.Printf("Sending email to %s: %s", t.Payload.To, t.Payload.Subject)
        return nil // 返回 nil 表示处理成功，Worker 会自动 Delete 任务
                   // 返回 error 表示失败，Worker 会自动 Bury 任务以便重试
    })

    // 创建 Worker，并发数为 3
    worker := task.NewWorker(dq, "email_topic", handler, task.WithConcurrency[EmailPayload](3))
    
    // 启动 Worker
    worker.Start(context.Background())
    defer worker.Stop()
    
    select {} // 阻塞主进程
}
```

### 3. 发布任务 (生产者)

使用 `task.NewTask` 创建任务，并使用 `Serialize` 方法将其转换为字节流推送到 DelayQueue。

```go
func publish() {
    // ... dq 初始化 ...

    // 准备数据
    payload := EmailPayload{
        To:      "user@example.com",
        Subject: "Welcome",
        Body:    "Hello World",
    }

    // 创建任务对象
    t := task.NewTask("email_topic", payload)

    // 序列化
    data, err := t.Serialize()
    if err != nil {
        log.Fatal(err)
    }

    // 推送到队列
    // 参数: topic, body, priority, delay, ttr
    dq.Put("email_topic", data, 0, 10*time.Second, 60*time.Second)
}
```

## API 参考

### `Task[T]`

泛型任务包装器，包含任务元数据（ID, Topic, Delay 等）和业务载荷 `Payload`。

### `Worker[T]`

泛型工作者，负责轮询队列。

- `NewWorker(queue, topic, handler, opts...)`: 创建 Worker。
- `Start(ctx)`: 启动 Worker。
- `Stop()`: 停止 Worker。

### `Handler[T]`

任务处理器接口。

- `Handle(ctx, task *Task[T]) error`: 处理任务。

### `TaskOption[T]` / `WorkerOption[T]`

用于配置 Task 和 Worker 的选项模式函数。
