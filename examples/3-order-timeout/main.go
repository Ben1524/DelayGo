package main

import (
	"log"
	"sync"
	"time"

	"github.com/Ben1524/delaygo"
)

// 示例 3: 订单超时取消
// 模拟场景：用户下单后，如果在指定时间内未支付，系统自动取消订单
func main() {
	q, _ := delaygo.New(delaygo.DefaultConfig())
	q.Start()
	defer func() { _ = q.Stop() }()

	// 模拟数据库
	orders := make(map[string]string) // orderID -> status
	var mu sync.Mutex

	// 1. 用户下单
	orderID := "ORDER_1001"
	mu.Lock()
	orders[orderID] = "PENDING"
	mu.Unlock()
	log.Printf("[下单] 订单 %s 创建成功，状态: PENDING", orderID)

	// 2. 发送超时检查任务 (3秒后检查)
	// Body 存储 OrderID
	_, err := q.Put("order_check", []byte(orderID), 1, 3*time.Second, 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[系统] 已安排超时检查任务 (3秒后)")

	// 3. 模拟支付 (这里我们故意不支付，或者注释掉下面这行来模拟未支付)
	// time.Sleep(1 * time.Second)
	// payOrder(orderID, orders, &mu)

	// 4. 启动 Worker 处理超时检查
	go func() {
		for {
			job, err := q.Reserve([]string{"order_check"}, 4*time.Second)
			if err != nil {
				return // 超时或关闭
			}

			targetOrderID := string(job.Body())
			log.Printf("[Worker] 收到检查任务，检查订单: %s", targetOrderID)

			mu.Lock()
			status := orders[targetOrderID]
			if status == "PENDING" {
				orders[targetOrderID] = "CANCELLED"
				log.Printf("[Worker] 订单 %s 未支付，执行取消操作！", targetOrderID)
			} else {
				log.Printf("[Worker] 订单 %s 状态为 %s，无需取消。", targetOrderID, status)
			}
			mu.Unlock()

			job.Delete()
		}
	}()

	// 等待演示结束
	time.Sleep(5 * time.Second)

	mu.Lock()
	log.Printf("[最终状态] 订单 %s: %s", orderID, orders[orderID])
	mu.Unlock()
}

func payOrder(orderID string, orders map[string]string, mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()
	if orders[orderID] == "PENDING" {
		orders[orderID] = "PAID"
		log.Printf("[支付] 订单 %s 支付成功", orderID)
	}
}
