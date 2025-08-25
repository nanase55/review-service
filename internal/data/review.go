package data

import (
	"context"
	"errors"
	"review-service/internal/biz"
	"review-service/internal/data/model"
	"review-service/internal/data/query"

	"github.com/go-kratos/kratos/v2/log"
)

type reviewRepo struct {
	data *Data
	log  *log.Helper
}

func NewReviewRepo(data *Data, logger log.Logger) biz.ReviewRepo {
	return &reviewRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

func (r *reviewRepo) SaveReview(ctx context.Context, review *model.ReviewInfo) (*model.ReviewInfo, error) {
	err := r.data.q.ReviewInfo.WithContext(ctx).Save(review)
	return review, err
}

func (r *reviewRepo) GetReviewByReviewId(ctx context.Context, reviewID int64) (*model.ReviewInfo, error) {
	return r.data.q.ReviewInfo.
		WithContext(ctx).
		Where(r.data.q.ReviewInfo.ReviewID.Eq(reviewID)).
		First()
}

// GetReviewByOrderId 根据订单Id查询评价
func (r *reviewRepo) GetReviewByOrderId(ctx context.Context, orderId int64) ([]*model.ReviewInfo, error) {
	return r.data.q.ReviewInfo.
		WithContext(ctx).
		Where(r.data.q.ReviewInfo.OrderID.Eq(orderId)).
		Find()
}

// SaveReply 保存评价回复
func (r *reviewRepo) SaveReply(ctx context.Context, reply *model.ReviewReplyInfo) (*model.ReviewReplyInfo, error) {
	// 1. 数据校验

	// 1.1 数据合法性校验（已回复的评价不允许商家再次回复）
	// 用ReviewId查评价表
	review, err := r.data.q.ReviewInfo.
		WithContext(ctx).
		Where(r.data.q.ReviewInfo.ReviewID.Eq(reply.ReviewID)).
		First()
	if err != nil {
		return nil, err
	}
	// 是否已回复
	if review.HasReply == 1 {
		return nil, errors.New("该评价已回复")
	}

	// 1.2 水平越权校验（A商家只能回复自己的不能回复B商家的）
	// 评价表记录的店铺id = 传入的店铺id参数
	if review.StoreID != reply.StoreID {
		return nil, errors.New("水平越权")
	}

	// 2. 更新数据库中的数据（评价回复表和评价表要同时更新，涉及到事务操作）
	err = r.data.q.Transaction(func(tx *query.Query) error {
		// 评价回复表插入一条数据
		if err := tx.ReviewReplyInfo.
			WithContext(ctx).
			Save(reply); err != nil {
			r.log.WithContext(ctx).Errorf("SaveReply create reply fail, err:%v", err)
			return err
		}
		// 评价表更新hasReply字段
		if _, err := tx.ReviewInfo.
			WithContext(ctx).
			Where(tx.ReviewInfo.ReviewID.Eq(reply.ReviewID)).
			Update(tx.ReviewInfo.HasReply, 1); err != nil {
			r.log.WithContext(ctx).Errorf("SaveReply update review fail, err:%v", err)
			return err
		}
		return nil
	})

	return reply, err
}
