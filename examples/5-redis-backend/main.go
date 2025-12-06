package main

import (
	"context"
	"log"
	"time"

	"github.com/Ben1524/delaygo"
	"github.com/redis/go-redis/v9"
)

// 示例 5: Redis 后端
// 展示如何使用 Redis 作为存储后端
func main() {
	// 1. 连接 Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     "100.108.24.7:6379",
		Password: "Redis@123456", // 如果没有密码则留空
	})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("无法连接 Redis: %v", err)
	}
	log.Println("已连接 Redis")

	// 2. 创建 Redis 存储
	// 使用 WithRedisPrefix 设置 Key 前缀，避免冲突
	storage := delaygo.NewRedisStorage(rdb, delaygo.WithRedisPrefix("delaygo:example"))

	// 3. 配置队列使用 Redis 存储
	config := delaygo.DefaultConfig()
	config.Storage = storage

	// 4. 初始化队列
	q, err := delaygo.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	// 5. 启动队列
	if err := q.Start(); err != nil {
		log.Fatal(err)
	}

	// 6. 发布任务
	log.Println("发布任务: 'Redis Job' (延迟 3秒)")
	jobID, err := q.Put("redis-topic", []byte("Redis Job"), 1, 3*time.Second, 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("任务已发布 ID: %d\n", jobID)

	// 7. 消费任务
	log.Println("等待任务就绪...")
	job, err := q.Reserve([]string{"redis-topic"}, 10*time.Second)
	if err != nil {
		log.Fatalf("获取任务失败: %v", err)
	}

	log.Printf("收到任务: ID=%d, Body=%s\n", job.Meta.ID, string(job.Body()))

	// 8. 删除任务
	if err := q.Delete(job.Meta.ID); err != nil {
		log.Printf("删除任务失败: %v", err)
	} else {
		log.Println("任务已处理并删除")
	}
}
