package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Ben1524/delaygo"
	"github.com/Ben1524/delaygo/task"
)

// EmailPayload 定义任务载荷
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	// 1. 初始化 DelayQueue (使用内存存储)
	dq, err := delaygo.New(delaygo.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	// 确保在退出时停止队列
	defer func() {
		if err := dq.Stop(); err != nil {
			log.Printf("Failed to stop queue: %v", err)
		}
	}()

	// 启动队列
	if err := dq.Start(); err != nil {
		log.Fatal(err)
	}

	// 2. 定义 Worker 处理逻辑
	handler := task.HandlerFunc[EmailPayload](func(ctx context.Context, t *task.Task[EmailPayload]) error {
		log.Printf("Processing email task [ID:%d]: To=%s, Subject=%s", t.ID, t.Payload.To, t.Payload.Subject)
		// 模拟耗时操作
		time.Sleep(500 * time.Millisecond)
		log.Printf("Email sent successfully [ID:%d]", t.ID)
		return nil
	})

	// 3. 创建并启动 Worker
	// 监听 "email_topic"
	worker := task.NewWorker(dq, "email_topic", handler, task.WithConcurrency[EmailPayload](3))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker.Start(ctx)
	defer worker.Stop()

	// 4. 发布任务
	log.Println("Publishing tasks...")
	for i := 0; i < 5; i++ {
		payload := EmailPayload{
			To:      "user@example.com",
			Subject: "Welcome!",
			Body:    "Hello from DelayGo",
		}

		// 创建任务对象
		t := task.NewTask("email_topic", payload)

		// 序列化载荷
		data, err := t.Serialize()
		if err != nil {
			log.Printf("Serialize failed: %v", err)
			continue
		}

		// 推送到队列 (延迟 1 秒)
		// Put(topic, body, priority, delay, ttr)
		id, err := dq.Put("email_topic", data, 0, 1*time.Second, 10*time.Second)
		if err != nil {
			log.Printf("Put failed: %v", err)
			continue
		}
		log.Printf("Task published [ID:%d]", id)
	}

	log.Println("Waiting for tasks to be processed... (Press Ctrl+C to exit)")

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down...")
}
