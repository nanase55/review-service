package biz

import (
	"container/heap"
	"context"
	"review-service/internal/data/model"
	"review-service/pkg/myheap"
	"sync"

	"github.com/segmentio/kafka-go"
)

type ReviewRepo interface {
	SaveReview(context.Context, *model.ReviewInfo) error
	GetReviewByOrderId(context.Context, int64) ([]*model.ReviewInfo, error)
	GetReviewByReviewId(context.Context, int64) (*model.ReviewInfo, error)
	SaveReply(context.Context, *model.ReviewReplyInfo) (*model.ReviewReplyInfo, error)
	GetReviewReply(context.Context, int64) (*model.ReviewReplyInfo, error)
	AuditReview(context.Context, *AuditParam) error
	AppealReview(context.Context, *AppealParam) (*model.ReviewAppealInfo, error)
	AuditAppeal(context.Context, *AuditAppealParam) error
	ListReviewByUserID(context.Context, string, int64, int) ([]*model.ReviewInfo, error)
	ListReviewByStoreAndSpu(context.Context, *ListReviewBySAndSParam) ([]*model.ReviewInfo, error)

	SetRedis(context.Context, string, any) error
	ExistsRedis(context.Context, string) (bool, error)

	UpdateReviewStats(context.Context, *ReviewStatsEvent) error

	PublishUpdateReviewEvent(context.Context, *model.ReviewInfo) error
	PublishDLQEvent(context.Context, []byte)
	ReadReviewMessage(context.Context) (*kafka.Message, error)
	CommitReviewMessages(context.Context, *kafka.Message) error
}

type WorkerPool interface {
	Start(context.Context) error
	Stop(context.Context) error
	Submit(func()) bool
}

type CommitMessagesBuffer struct {
	mu          sync.Mutex
	offsetsHeap *myheap.IntHeap // 最小堆
	maxOffset   int64           // 未提交的连续offset最大值
	initialized bool            // 是否为第一次提交
}

func NewCommitMessagesBuffer() *CommitMessagesBuffer {
	h := &myheap.IntHeap{}
	heap.Init(h)
	return &CommitMessagesBuffer{
		offsetsHeap: h,
	}
}

func (b *CommitMessagesBuffer) AddOffset(offset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 第一次添加offset时，初始化maxOffset
	if !b.initialized {
		b.maxOffset = offset - 1 // 设置为第一个offset的前一个
		b.initialized = true
	}

	heap.Push(b.offsetsHeap, offset)
}

func (b *CommitMessagesBuffer) GetMaxOffset() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 尝试拼接连续的offset
	for b.offsetsHeap.Len() > 0 {
		minOffset := (*b.offsetsHeap)[0] // 堆顶

		if minOffset == b.maxOffset+1 {
			// 如果连续
			b.maxOffset = minOffset
			heap.Pop(b.offsetsHeap)
		} else if minOffset <= b.maxOffset {
			// 重复或已处理的offset
			heap.Pop(b.offsetsHeap)
		} else {
			// 不连续
			break
		}
	}

	return b.maxOffset
}
