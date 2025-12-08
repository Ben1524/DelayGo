package delaygo

import (
	"context"
	"time"
)

// recoveryRunner 负责从 Storage 恢复任务
type recoveryRunner struct {
	storage Storage
	ctx     context.Context
}

// newRecoveryRunner 创建新的 recoveryRunner
func newRecoveryRunner(ctx context.Context, storage Storage) *recoveryRunner {
	return &recoveryRunner{
		storage: storage,
		ctx:     ctx,
	}
}

// RecoveryResult 恢复结果
type RecoveryResult struct {
	MaxID           uint64                     // 最大任务 ID
	TopicDelayJobs  map[string][]*DelayJobMeta // topic -> delayJobs
	TotalDelayJobs  int                        // 总任务数
	FailedDelayJobs int                        // 失败的任务数
}



// Recover 从 Storage 恢复所有任务
func (rr *recoveryRunner) Recover() (*RecoveryResult, error) {
	// 扫描所有任务
	result, err := rr.storage.ScanDelayJobMeta(rr.ctx, nil)
	if err != nil {
		return nil, err
	}

	recovery := &RecoveryResult{
		TopicDelayJobs:  make(map[string][]*DelayJobMeta),
		TotalDelayJobs:  len(result.Metas),
		FailedDelayJobs: 0,
	}

	// 查找最大 ID
	maxID := uint64(0)
	for _, meta := range result.Metas {
		if meta.ID > maxID {
			maxID = meta.ID
		}
	}
	recovery.MaxID = maxID

	// 按 Topic 分组
	for _, meta := range result.Metas {
		// 预处理任务状态
		processedMeta := rr.preprocessDelayJobMeta(meta)
		if processedMeta == nil {
			recovery.FailedDelayJobs++
			continue
		}

		recovery.TopicDelayJobs[meta.Topic] = append(recovery.TopicDelayJobs[meta.Topic], processedMeta)
	}

	return recovery, nil
}

// RecoverAsync 异步恢复任务
// 返回一个 channel，调用者可以从中接收恢复进度
func (rr *recoveryRunner) RecoverAsync() <-chan *RecoveryProgress {
	progressCh := make(chan *RecoveryProgress, 10)

	go func() {
		defer close(progressCh)

		// 发送开始事件
		progressCh <- &RecoveryProgress{
			Phase: RecoveryPhaseStart,
		}

		// 执行恢复
		result, err := rr.Recover()
		if err != nil {
			progressCh <- &RecoveryProgress{
				Phase: RecoveryPhaseError,
				Error: err,
			}
			return
		}

		// 发送完成事件
		progressCh <- &RecoveryProgress{
			Phase:           RecoveryPhaseComplete,
			Result:          result,
			TotalDelayJobs:  result.TotalDelayJobs,
			FailedDelayJobs: result.FailedDelayJobs,
		}
	}()

	return progressCh
}

// RecoveryPhase 恢复阶段
type RecoveryPhase int

const (
	RecoveryPhaseStart    RecoveryPhase = iota // 开始恢复
	RecoveryPhaseScanning                      // 扫描中
	RecoveryPhaseLoading                       // 加载中
	RecoveryPhaseComplete                      // 完成
	RecoveryPhaseError                         // 错误
)

// RecoveryProgress 恢复进度
type RecoveryProgress struct {
	Phase           RecoveryPhase   // 当前阶段
	Result          *RecoveryResult // 恢复结果（仅在 Complete 阶段有值）
	TotalDelayJobs  int             // 总任务数
	LoadedDelayJobs int             // 已加载任务数
	FailedDelayJobs int             // 失败任务数
	Error           error           // 错误信息（仅在 Error 阶段有值）
}

// preprocessDelayJobMeta 预处理任务元数据
// 根据状态做必要的转换和修正
func (rr *recoveryRunner) preprocessDelayJobMeta(meta *DelayJobMeta) *DelayJobMeta {
	switch meta.DelayState {
	case DelayStateEnqueued:
		// Enqueued 是临时状态，说明上次崩溃时任务刚创建还未完全加载
		return rr.handleEnqueuedDelayJob(meta)

	case DelayStateReserved:
		// 崩溃前正在处理的任务，转为 Ready 重新分配
		return rr.handleReservedDelayJob(meta)

	case DelayStateReady, DelayStateDelayed, DelayStateBuried:
		// 这些状态直接恢复
		return meta

	default:
		// 未知状态，跳过
		return nil
	}
}

// handleEnqueuedDelayJob 处理 Enqueued 状态的任务
func (rr *recoveryRunner) handleEnqueuedDelayJob(meta *DelayJobMeta) *DelayJobMeta {
	if meta.Delay > 0 {
		meta.DelayState = DelayStateDelayed
	} else {
		meta.DelayState = DelayStateReady
	}

	// 更新状态到 Storage
	if rr.storage != nil {
		_ = rr.storage.UpdateDelayJobMeta(rr.ctx, meta)
	}

	return meta
}

// handleReservedDelayJob 处理 Reserved 状态的任务
// 崩溃前正在处理的任务，转为 Ready 重新分配
func (rr *recoveryRunner) handleReservedDelayJob(meta *DelayJobMeta) *DelayJobMeta {
	meta.DelayState = DelayStateReady
	meta.ReservedAt = time.Time{}
	meta.ReadyAt = time.Now()

	// 增加 Timeouts 计数（崩溃导致的隐式超时）
	meta.Timeouts++

	// 更新状态到 Storage
	if rr.storage != nil {
		_ = rr.storage.UpdateDelayJobMeta(rr.ctx, meta)
	}

	return meta
}
