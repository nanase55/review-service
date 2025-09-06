package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	v1 "review-service/api/review/v1"
	"review-service/internal/data/model"
	"review-service/pkg/snowflake"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

type ReviewUsecase struct {
	repo                ReviewRepo
	log                 *log.Helper
	createReviewLimiter *rate.Limiter
	wp                  WorkerPool
	buffers             map[int]*CommitMessagesBuffer
	mu                  sync.Mutex // 保护缓冲区
}

func NewReviewUsecase(repo ReviewRepo, logger log.Logger, wp WorkerPool) *ReviewUsecase {
	return &ReviewUsecase{
		repo:                repo,
		log:                 log.NewHelper(logger),
		createReviewLimiter: rate.NewLimiter(50, 100), // 每秒最多50个请求，最大突发100个
		wp:                  wp,
		buffers:             make(map[int]*CommitMessagesBuffer),
	}
}

// CreateReview 创建评价
func (r *ReviewUsecase) CreateReview(ctx context.Context, review *model.ReviewInfo) (*model.ReviewInfo, error) {
	r.log.WithContext(ctx).Debugf(" CreateReview, req: %#v", review)
	// 0. 漏桶限流
	if !r.createReviewLimiter.Allow() {
		return nil, v1.ErrorRateLimited("评价请求过于频繁，请稍后再试")
	}

	// 1. 数据处理
	// 1.1 数据校验: 业务参数校验
	// 比如用户是否存在、订单是否存在、用户是否有该订单等等

	// 1.2 拼装数据
	review.Status = 10
	if review.PicInfo != "" || review.VideoInfo != "" {
		review.HasMedia = 1
	}

	// 生成review Id (雪花算法 或 "分布式Id生成服务")
	review.ReviewID = snowflake.GenId()

	// 1.3 调用其他服务获取信息
	review.StoreID = "1"
	review.SpuID = 1
	review.SkuID = "1"

	// 2. 写入数据
	if err := r.repo.SaveReview(ctx, review); err != nil {
		return nil, err
	}

	// 3. 异步操作
	// 3.1 提交 MQ异步增量更新统计表 任务
	job := func() {
		r.repo.PublishUpdateReviewEvent(context.Background(), review)
	}
	if !r.wp.Submit(job) {
		r.log.WithContext(ctx).Warn("Worker pool full, retrying Submit...")
		// 重试一次
		time.Sleep(100 * time.Millisecond)
		if !r.wp.Submit(job) {
			r.log.WithContext(ctx).Error("Worker pool full after retry, executing synchronously")
			// 兜底：同步执行
			job()
		}
	}

	return review, nil
}

// GetReview 根据评价ID获取评价
func (uc *ReviewUsecase) GetReview(ctx context.Context, reviewID int64) (*model.ReviewInfo, error) {
	uc.log.WithContext(ctx).Debugf(" GetReview reviewID:%#v", reviewID)
	return uc.repo.GetReviewByReviewId(ctx, reviewID)
}

// CreateReply 创建评价回复
func (uc *ReviewUsecase) CreateReply(ctx context.Context, param *ReplyParam) (*model.ReviewReplyInfo, error) {
	uc.log.WithContext(ctx).Debugf(" CreateReply param:%#v", param)

	reply := &model.ReviewReplyInfo{
		ReplyID:   snowflake.GenId(),
		ReviewID:  param.ReviewID,
		StoreID:   param.StoreID,
		Content:   param.Content,
		PicInfo:   param.PicInfo,
		VideoInfo: param.VideoInfo,
	}

	return uc.repo.SaveReply(ctx, reply)
}

// AuditReview 审核评价
func (uc *ReviewUsecase) AuditReview(ctx context.Context, param *AuditParam) error {
	uc.log.WithContext(ctx).Debugf(" AuditReview param:%v", param)
	return uc.repo.AuditReview(ctx, param)
}

// AppealReview 申诉评价
func (uc *ReviewUsecase) AppealReview(ctx context.Context, param *AppealParam) (*model.ReviewAppealInfo, error) {
	uc.log.WithContext(ctx).Debugf(" AppealReview param:%v", param)
	return uc.repo.AppealReview(ctx, param)
}

// AuditAppeal 审核申诉
func (uc *ReviewUsecase) AuditAppeal(ctx context.Context, param *AuditAppealParam) error {
	uc.log.WithContext(ctx).Debugf(" AuditAppeal param:%v", param)
	return uc.repo.AuditAppeal(ctx, param)
}

// ListReviewByUserID 根据userID分页查询评价
func (uc *ReviewUsecase) ListReviewByUserID(ctx context.Context, userID string, lastId int64, size int) ([]*model.ReviewInfo, error) {
	uc.log.WithContext(ctx).Debugf(" ListReviewByUserID userID:%v", userID)

	return uc.repo.ListReviewByUserID(ctx, userID, lastId, size)
}

func (uc *ReviewUsecase) ListReviewByStoreAndSpu(ctx context.Context, param *ListReviewBySAndSParam) ([]*model.ReviewInfo, error) {
	uc.log.WithContext(ctx).Debugf(" ListReviewByStoreAndSpu StoreId:%v,SpuId:%d", param.StoreId, param.SpuId)

	return uc.repo.ListReviewByStoreAndSpu(ctx, param)
}

// ConsumeReviewEvents 读取消息并提交到工作池
func (uc *ReviewUsecase) ConsumeReviewEventsLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := uc.repo.ReadReviewMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					uc.log.WithContext(ctx).Info(" ConsumeReviewEvents stopped due to context Canceled")
					return
				}
				uc.log.WithContext(ctx).Errorf(" ConsumeReviewEvents Failed to read message: %v", err)
				continue
			}

			uc.log.WithContext(ctx).Debugf("读取到消息 msg Topic:%v,Partition:%v,offset:%v", msg.Topic, msg.Partition, msg.Offset)
			// 提交处理消息任务到协程池
			job := func() {
				uc.HandleReviewEvent(ctx, msg)
			}
			if !uc.wp.Submit(job) {
				uc.log.WithContext(ctx).Warn("Worker pool full, retrying Submit...")
				// 重试一次
				time.Sleep(100 * time.Millisecond)
				if !uc.wp.Submit(job) {
					uc.log.WithContext(ctx).Error("Worker pool full after retry, executing synchronously")
					// 兜底：同步执行
					job()
				}
			}
		}
	}
}

// HandleReviewEvent 处理从kafka读取的消息
func (uc *ReviewUsecase) HandleReviewEvent(ctx context.Context, msg *kafka.Message) {
	uc.log.WithContext(ctx).Debugf(" HandleReviewEvent 处理msg Topic:%v,Partition:%v,offset:%v", msg.Topic, msg.Partition, msg.Offset)
	var err error
	// 1. 反序列化消息
	event := &ReviewStatsEvent{}
	if err = json.Unmarshal(msg.Value, &event); err != nil {
		uc.log.WithContext(ctx).Errorf(" Failed to unmarshal message: %v", err)
		goto errtag
	}

	const maxRetries = 3
	for i := range maxRetries {
		// 2. 幂等性判断
		key := fmt.Sprintf("review_status:processed:%d", event.ReviewID)
		exists, e := uc.repo.ExistsRedis(ctx, key)
		if e != nil {
			err = e
			uc.log.WithContext(ctx).Errorf("ExistsRedis failed: %v", err)
			goto retry
		}
		if exists {
			uc.log.WithContext(ctx).Infof("Review %d already processed", event.ReviewID)
			goto addOffset // 跳到添加 offset
		}

		// 3. 设置幂等性
		if err = uc.repo.SetRedis(ctx, key, nil); err != nil {
			goto retry
		}

		// 3. 更新MySQL统计表
		if err = uc.repo.UpdateReviewStats(ctx, event); err != nil {
			uc.log.WithContext(ctx).Errorf("Failed to update stats: %v", err)
			goto retry
		}

		// 处理成功
		break

	retry:
		if i < maxRetries-1 {
			uc.log.WithContext(ctx).Warnf("Retrying... (%d/%d)", i+1, maxRetries)
			time.Sleep(100 * time.Millisecond)
		}
	}

	// 5. 如果重试失败,放入DLQ
errtag:
	if err != nil {
		// 重试失败，加入 DLQ
		uc.repo.PublishDLQEvent(ctx, msg.Value)
		uc.log.WithContext(ctx).Errorf("HandleReviewEvent failed after retries, sent to DLQ")
	}

addOffset:
	// 6. 写入 CommitMessagesBuffer (无论成功失败都提交)
	partition := msg.Partition
	uc.mu.Lock()
	if uc.buffers[msg.Partition] == nil {
		uc.buffers[partition] = NewCommitMessagesBuffer()
	}
	uc.buffers[partition].AddOffset(msg.Offset)
	uc.mu.Unlock()
}

// CommitMessagesLoop 定时检查并提交消息
func (uc *ReviewUsecase) CommitMessagesLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second) // 定时检查
	defer ticker.Stop()

	const (
		countThreshold = 50              // 数量阈值
		timeThreshold  = 6 * time.Second // 时间阈值
	)

	lastCommitted := make(map[int]int64)      // 记录上次提交的 offset
	lastCommitTime := make(map[int]time.Time) // 记录上次提交时间

	for {
		select {
		case <-ctx.Done():
			// 优雅关闭时提交所有未提交的消息
			uc.commitAllPendingMessages(ctx, lastCommitted)
			uc.log.WithContext(ctx).Info(" CommitMessagesLoop stopped due to context Canceled")
			return
		case <-ticker.C:
			uc.mu.Lock()

			for partition, buffer := range uc.buffers {
				maxOffset := buffer.GetMaxOffset()
				lastOffset := lastCommitted[partition]
				if _, ok := lastCommitTime[partition]; !ok {
					lastCommitTime[partition] = time.Now()
				}
				lastTime := lastCommitTime[partition]

				if int(maxOffset-lastCommitted[partition]) >= countThreshold ||
					time.Since(lastTime) >= timeThreshold && maxOffset > lastOffset { // 达到阈值
					dummyMsg := &kafka.Message{Partition: partition, Offset: maxOffset}

					if err := uc.repo.CommitReviewMessages(ctx, dummyMsg); err != nil {
						uc.log.Errorf("Commit failed for partition %d: %v", partition, err)
					} else {
						uc.log.WithContext(ctx).Debugf(" CommitMessagesLoop 提交消息 offset: %d", maxOffset)
						lastCommitted[partition] = maxOffset // 更新已提交
						lastCommitTime[partition] = time.Now()
					}
				}
			}

			uc.mu.Unlock()
		}
	}
}

// commitAllPendingMessages 优雅关闭时提交所有未提交的消息
func (uc *ReviewUsecase) commitAllPendingMessages(ctx context.Context, lastCommitted map[int]int64) {
	uc.log.WithContext(ctx).Infof(" commitAllPendingMessages")
	uc.mu.Lock()
	defer uc.mu.Unlock()

	for partition, buffer := range uc.buffers {
		maxOffset := buffer.GetMaxOffset()
		if maxOffset > lastCommitted[partition] {
			dummyMsg := &kafka.Message{Partition: partition, Offset: maxOffset}
			if err := uc.repo.CommitReviewMessages(ctx, dummyMsg); err != nil {
				uc.log.Errorf("Final commit failed for partition %d: %v", partition, err)
			} else {
				uc.log.WithContext(ctx).Infof("Final commit for partition %d offset:%d", partition, maxOffset)
			}
		}
	}
}
