package biz

import (
	"context"
	v1 "review-service/api/review/v1"
	"review-service/internal/data/model"
	"review-service/pkg/snowflake"

	"github.com/go-kratos/kratos/v2/log"
)

type ReviewRepo interface {
	SaveReview(context.Context, *model.ReviewInfo) (*model.ReviewInfo, error)
	GetReviewByOrderId(context.Context, int64) ([]*model.ReviewInfo, error)
}

type ReviewUsecase struct {
	repo ReviewRepo
	log  *log.Helper
}

func NewReviewUsecase(repo ReviewRepo, logger log.Logger) *ReviewUsecase {
	return &ReviewUsecase{
		repo: repo,
		log:  log.NewHelper(logger),
	}
}

// CreateReview 创建评价
func (r *ReviewUsecase) CreateReview(ctx context.Context, review *model.ReviewInfo) (*model.ReviewInfo, error) {
	r.log.WithContext(ctx).Debugf("[biz] CreateReview, req: %v", review)
	// 1. 数据校验: 业务参数校验
	// 1.1 已评价的订单不能再评价
	reviews, err := r.repo.GetReviewByOrderId(ctx, review.OrderID)
	if err != nil {
		return nil, v1.ErrorDbFailed("查询数据库失败")
	}
	if len(reviews) > 0 {
		return nil, v1.ErrorOrderReviewed("订单:%d已评价", review.OrderID)
	}
	// 2. 生成review Id (雪花算法 或 "分布式Id生成服务")
	review.ReviewID = snowflake.GenId()
	// 3. 查询订单和商品快照信息
	// 可能需要获取订单和商品的信息,通过RPC调用

	// 4. 拼装数据入库
	return r.repo.SaveReview(ctx, review)
}
