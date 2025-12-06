package main

import (
	"log"
	"os"
	"time"

	"github.com/Ben1524/delaygo"
)

// 示例 2: 持久化 (SQLite)
// 展示如何使用 SQLite 存储任务，确保重启后不丢失
func main() {
	dbPath := "queue.db"
	// 清理旧数据
	os.Remove(dbPath)

	log.Println("=== 第一阶段: 发布任务并关闭队列 ===")
	{
		// 1. 配置 SQLite 存储
		storage, err := delaygo.NewSQLiteStorage(dbPath)
		if err != nil {
			log.Fatal(err)
		}

		config := delaygo.DefaultConfig()
		config.Storage = storage

		q, err := delaygo.New(config)
		if err != nil {
			log.Fatal(err)
		}

		if err := q.Start(); err != nil {
			log.Fatal(err)
		}

		// 发布任务
		log.Println("发布任务: 'Persisted Job' (延迟 5秒)")
		_, err = q.Put("persist", []byte("Persisted Job"), 1, 5*time.Second, 60*time.Second)
		if err != nil {
			log.Fatal(err)
		}

		// 停止队列 (模拟服务重启)
		log.Println("停止队列...")
		q.Stop()
		storage.Close()
	}

	log.Println("\n=== 第二阶段: 重启队列并恢复任务 ===")
	{
		// 1. 重新打开存储
		storage, err := delaygo.NewSQLiteStorage(dbPath)
		if err != nil {
			log.Fatal(err)
		}

		config := delaygo.DefaultConfig()
		config.Storage = storage

		// 2. 新建队列实例
		q, err := delaygo.New(config)
		if err != nil {
			log.Fatal(err)
		}

		// 3. 启动 (会自动从 SQLite 恢复任务)
		log.Println("启动队列 (正在恢复数据)...")
		if err := q.Start(); err != nil {
			log.Fatal(err)
		}
		defer func() {
			q.Stop()
			storage.Close()
			os.Remove(dbPath) // 清理
		}()

		// 4. 等待任务恢复并就绪
		// 之前延迟 5s，这里直接 Reserve
		log.Println("等待任务恢复...")

		// 注意：Reserve 会等待直到任务就绪
		job, err := q.Reserve([]string{"persist"}, 10*time.Second)
		if err != nil {
			log.Fatalf("恢复后获取任务失败: %v", err)
		}

		log.Printf("成功恢复并消费任务: ID=%d, Body=%s\n", job.Meta.ID, string(job.Body()))
		job.Delete()
	}
}
