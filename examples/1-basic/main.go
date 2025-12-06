package main

import (
	"log"
	"time"

	"github.com/Ben1524/delaygo"
)

// 示例 1: 基础用法
// 展示最简单的延迟任务发布与消费
func main() {
	// 1. 创建默认配置 (内存存储 + 动态休眠 Ticker)
	config := delaygo.DefaultConfig()

	// 2. 初始化队列
	q, err := delaygo.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	// 3. 启动队列
	if err := q.Start(); err != nil {
		log.Fatal(err)
	}

	// 4. 发布一个延迟任务
	// Topic: "greet", Body: "Hello World", Delay: 2s
	log.Println("发布任务: 'Hello World' (延迟 2秒)")
	jobID, err := q.Put("greet", []byte("Hello World"), 1, 2*time.Second, 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("任务已发布 ID: %d\n", jobID)

	// 5. 消费任务
	log.Println("等待任务就绪...")

	// Reserve 会阻塞直到有任务就绪或超时
	// 这里设置 5秒超时，足够覆盖任务的 2秒延迟
	job, err := q.Reserve([]string{"greet"}, 5*time.Second)
	if err != nil {
		log.Fatalf("获取任务失败: %v", err)
	}

	// 6. 处理任务
	log.Printf("收到任务: ID=%d, Body=%s\n", job.Meta.ID, string(job.Body()))

	// 7. 删除任务 (确认消费成功)
	if err := job.Delete(); err != nil {
		log.Fatal(err)
	}
	log.Println("任务处理完成并删除")
}
