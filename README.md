# delaygo

🚀 **delaygo** 是一个基于 Go 语言开发的高性能、轻量级分布式延迟队列系统。受 beanstalkd 启发，但在性能、持久化和易用性上进行了深度优化。

它专为需要高吞吐量、低延迟和可靠性的异步任务调度场景设计，支持海量任务的精准延迟执行。

## ✨ 核心特性

*   ✅ **多 Topic 隔离** - 支持多个命名队列，业务隔离
*   ✅ **优先级调度** - 支持任务优先级（数值越小优先级越高）
*   ✅ **精准延迟** - 基于时间轮/最小堆的精准定时调度
*   ✅ **可靠性保障** - **TTR (Time-To-Run)** 机制防止任务死锁或丢失
*   ✅ **灵活的任务状态** - 支持 `Ready`, `Delayed`, `Reserved`, `Buried` 等状态流转
*   ✅ **高性能持久化** - 支持 **Memory** (纯内存), **SQLite**, **MySQL**, **Redis** 等多种存储后端
    *   **元数据分离**：任务元数据（Meta）常驻内存，任务体（Body）按需加载
    *   **批量写入**：存储层支持高并发下的批量合并写入（Batch Insert/Update），大幅提升吞吐量
*   ✅ **零 GC 压力** - 核心对象使用 `sync.Pool` 复用，热点路径零堆分配（Benchmark: ~8ns/op）
*   ✅ **类型安全 API** - 提供泛型 Task API，自动处理序列化/反序列化

## ⚠️ 限制

* **分布式唯一拉取**: 目前项目暂不支持分布式场景下的唯一拉取（即多个消费者实例可能同时拉取到同一个任务，需要业务层保证幂等性或使用分布式锁）。此功能计划在未来版本中支持。
* 请尝试在业务层面实现幂等性，或使用 Redis 等外部组件实现分布式锁以避免重复处理任务。
* 推荐使用 Batch 方法批量操作任务以提升性能，避免频繁单条操作带来的开销。

## 📦 安装

```bash
go get github.com/Ben1524/delaygo
```

## 🚀 快速开始

```go
package main

import (
    "log"
    "time"

    "github.com/Ben1524/delaygo"
)

func main() {
    // 1. 创建默认配置 (内存存储)
    config := delaygo.DefaultConfig()

    // 2. 初始化队列
    q, err := delaygo.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer q.Stop()

    // 3. 启动队列
    q.Start()

    // 4. 发布任务 (Topic: "order", Body: "ID:1001", Delay: 5s, TTR: 30s)
    jobID, _ := q.Put("order", []byte("ID:1001"), 1, 5*time.Second, 30*time.Second)
    log.Printf("Job Published: %d", jobID)

    // 5. 消费任务
    // Reserve 会阻塞直到有任务就绪
    job, _ := q.Reserve([]string{"order"}, 10*time.Second)
    
    if job != nil {
        log.Printf("Job Received: %s", string(job.Body()))

        // 6. 处理完成后删除任务
        q.Delete(job.Meta.ID)
    }
}
```

## 💾 存储后端配置

DelayGo 支持多种存储引擎，可根据业务需求灵活切换。

### 1. Memory (默认)
纯内存存储，性能最高，但重启后数据丢失。适用于测试或对数据可靠性要求不高的场景。

```go
config := delaygo.DefaultConfig()
// config.Storage 默认为 NewMemoryStorage()
```

### 2. SQLite
基于 SQLite 的本地持久化存储。支持 WAL 模式和批量写入优化。

```go
storage, _ := delaygo.NewSQLiteStorage("delaygo.db")
config := delaygo.Config{
    Storage: storage,
    // ...
}
```

### 3. Redis
基于 Redis 的持久化存储。支持分布式部署，利用 Lua 脚本保证原子性。

```go
storage, _ := delaygo.NewRedisStorage(delaygo.RedisConfig{
    Addr: "localhost:6379",
    Prefix: "delaygo:",
})
config := delaygo.Config{
    Storage: storage,
    // ...
}
```

### 4. MySQL

基于 MySQL 的持久化存储。支持高并发批量写入优化。

```go
storage, _ := delaygo.NewMySQLStorage("user:password@tcp(localhost:3306)/delaygo?parseTime=true")
config := delaygo.Config{
    Storage: storage,
    // ...
}
```

## 🧩 核心概念

### 任务生命周期

1. **Put**: 任务被发布，进入 `Delayed` 状态（如果延迟 > 0）或 `Ready` 状态。
2. **Delayed**: 等待倒计时结束。
3. **Ready**: 倒计时结束，任务进入就绪队列，等待被消费者拉取。
4. **Reserved**: 消费者调用 `Reserve` 成功获取任务，任务进入“执行中”状态。此时任务对其他消费者不可见。
5. **TTR (Time-To-Run)**: 任务在 `Reserved` 状态的最长存活时间。如果消费者在 TTR 内未处理完（未 Delete/Bury），任务会自动重置为 `Ready` 状态，重新被调度。
6. **Delete**: 任务处理成功，从系统中彻底移除。
7. **Bury**: 任务处理失败（如逻辑错误），被“掩埋”。被掩埋的任务不会被调度，直到人工介入（Kick）。

## 📂 示例

更多详细示例请参考 [examples](./examples) 目录：

* [基础用法](./examples/1-basic)
* [持久化配置](./examples/2-persistence)
* [订单超时取消场景](./examples/3-order-timeout)
* [失败重试机制](./examples/4-retry)
* [Redis 后端](./examples/5-redis-backend)

## 📄 License

MIT
