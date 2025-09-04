package biz

import (
	"context"
	v1 "review-service/api/review/v1"
	"review-service/internal/data/model"
	"review-service/pkg/snowflake"

	"github.com/go-kratos/kratos/v2/log"
	"golang.org/x/time/rate"
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
}

type ReviewUsecase struct {
	repo                ReviewRepo
	log                 *log.Helper
	createReviewLimiter *rate.Limiter
}

func NewReviewUsecase(repo ReviewRepo, logger log.Logger) *ReviewUsecase {
	return &ReviewUsecase{
		repo:                repo,
		log:                 log.NewHelper(logger),
		createReviewLimiter: rate.NewLimiter(50, 100), // 每秒最多50个请求，最大突发100个
	}
}

// CreateReview 创建评价
func (r *ReviewUsecase) CreateReview(ctx context.Context, review *model.ReviewInfo) (*model.ReviewInfo, error) {
	r.log.WithContext(ctx).Debugf("[biz] CreateReview, req: %#v", review)
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

	// 调用其他服务获取信息
	review.StoreID = "1"
	review.SpuID = 1
	review.SkuID = "1"

	if err := r.repo.SaveReview(ctx, review); err != nil {
		return nil, err
	}

	return review, nil
}

// GetReview 根据评价ID获取评价
func (uc *ReviewUsecase) GetReview(ctx context.Context, reviewID int64) (*model.ReviewInfo, error) {
	uc.log.WithContext(ctx).Debugf("[biz] GetReview reviewID:%#v", reviewID)
	return uc.repo.GetReviewByReviewId(ctx, reviewID)
}

// CreateReply 创建评价回复
func (uc *ReviewUsecase) CreateReply(ctx context.Context, param *ReplyParam) (*model.ReviewReplyInfo, error) {
	uc.log.WithContext(ctx).Debugf("[biz] CreateReply param:%#v", param)

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
	uc.log.WithContext(ctx).Debugf("[biz] AuditReview param:%v", param)
	return uc.repo.AuditReview(ctx, param)
}

// AppealReview 申诉评价
func (uc ReviewUsecase) AppealReview(ctx context.Context, param *AppealParam) (*model.ReviewAppealInfo, error) {
	uc.log.WithContext(ctx).Debugf("[biz] AppealReview param:%v", param)
	return uc.repo.AppealReview(ctx, param)
}

// AuditAppeal 审核申诉
func (uc ReviewUsecase) AuditAppeal(ctx context.Context, param *AuditAppealParam) error {
	uc.log.WithContext(ctx).Debugf("[biz] AuditAppeal param:%v", param)
	return uc.repo.AuditAppeal(ctx, param)
}

// ListReviewByUserID 根据userID分页查询评价
func (uc ReviewUsecase) ListReviewByUserID(ctx context.Context, userID string, lastId int64, size int) ([]*model.ReviewInfo, error) {
	uc.log.WithContext(ctx).Debugf("[biz] ListReviewByUserID userID:%v", userID)

	return uc.repo.ListReviewByUserID(ctx, userID, lastId, size)
}

func (uc ReviewUsecase) ListReviewByStoreAndSpu(ctx context.Context, param *ListReviewBySAndSParam) ([]*model.ReviewInfo, error) {
	uc.log.WithContext(ctx).Debugf("[biz] ListReviewByStoreAndSpu StoreId:%v,SpuId:%d", param.StoreId, param.SpuId)

	return uc.repo.ListReviewByStoreAndSpu(ctx, param)
}
