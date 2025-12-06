package main

import (
	"log"
	"time"

	"github.com/Ben1524/delaygo"
)

// 示例 4: 任务重试 (TTR)
// 展示任务处理失败（超时）后的自动重试机制
func main() {
	q, _ := delaygo.New(delaygo.DefaultConfig())
	q.Start()
	defer func() { _ = q.Stop() }()

	// 1. 发布任务，设置 TTR 为 2秒
	// 这意味着如果 Worker 在 2秒内没有 Delete 或 Touch，任务会重新变成 Ready
	log.Println("发布任务: TTR=2s")
	q.Put("retry_demo", []byte("Unstable Job"), 1, 0, 2*time.Second)

	// 2. 第一次尝试 (模拟失败/超时)
	log.Println("\n=== 第 1 次尝试 ===")
	job1, _ := q.Reserve([]string{"retry_demo"}, 5*time.Second)
	log.Printf("Worker 1 获取任务: %d", job1.Meta.ID)

	log.Println("Worker 1 正在处理 (模拟卡死/崩溃)...")
	time.Sleep(3 * time.Second) // 超过 TTR (2s)
	log.Println("Worker 1 处理超时，未发送确认")

	// 此时任务应该已经因为 TTR 超时被重新放回 Ready 队列

	// 3. 第二次尝试 (重试)
	log.Println("\n=== 第 2 次尝试 (重试) ===")
	job2, _ := q.Reserve([]string{"retry_demo"}, 5*time.Second)
	if job2 != nil {
		log.Printf("Worker 2 获取任务: %d (重试次数: %d)", job2.Meta.ID, job2.Meta.Timeouts)
		log.Println("Worker 2 处理成功")
		job2.Delete()
	} else {
		log.Fatal("未收到重试任务！")
	}

	log.Println("\n演示完成")
}
