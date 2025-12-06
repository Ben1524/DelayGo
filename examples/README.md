# DelayGo 示例

本目录包含 DelayGo 的使用示例，从基础用法到高级场景。

## 示例列表

### 1. 基础用法 (1-basic)
最简单的 "Hello World" 示例。
- 启动队列
- 发布延迟任务
- 消费任务

```bash
go run examples/1-basic/main.go
```

### 2. 持久化 (2-persistence)
展示如何使用 SQLite 进行数据持久化。
- 即使程序重启，任务也不会丢失
- 适合生产环境

```bash
go run examples/2-persistence/main.go
```

### 3. 订单超时取消 (3-order-timeout)
模拟真实的电商场景：
- 用户下单 -> 创建 "检查支付状态" 的延迟任务
- 30分钟后（示例中为3秒） -> Worker 收到任务 -> 检查是否支付 -> 未支付则取消订单

```bash
go run examples/3-order-timeout/main.go
```

### 4. 任务重试与 TTR (4-retry)
展示 DelayGo 的可靠性机制：
- TTR (Time To Run): 任务处理超时自动重试
- 模拟 Worker 崩溃，任务自动重新进入队列被其他 Worker 消费

```bash
go run examples/4-retry/main.go
```
