package data

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	v1 "review-service/api/review/v1"
	"review-service/internal/biz"
	"review-service/internal/data/model"
	"review-service/internal/data/query"
	"review-service/pkg/snowflake"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type reviewRepo struct {
	data *Data
	log  *log.Helper

	g singleflight.Group
}

func NewReviewRepo(data *Data, logger log.Logger) biz.ReviewRepo {
	return &reviewRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

func (r *reviewRepo) SaveReview(ctx context.Context, review *model.ReviewInfo) error {
	if err := r.data.q.ReviewInfo.WithContext(ctx).Create(review); err != nil {
		// 唯一约束冲突
		if strings.Contains(err.Error(), "Duplicate entry") {
			return v1.ErrorOrderReviewed("订单: %d 已评价", review.OrderID)
		}
		return err
	}
	return nil
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

// GetReviewReply 获取评价回复
func (r *reviewRepo) GetReviewReply(ctx context.Context, reviewID int64) (*model.ReviewReplyInfo, error) {
	return r.data.q.ReviewReplyInfo.
		WithContext(ctx).
		Where(r.data.q.ReviewReplyInfo.ReviewID.Eq(reviewID)).
		First()
}

// AuditReview 审核评价（运营对用户的评价进行审核）
func (r *reviewRepo) AuditReview(ctx context.Context, param *biz.AuditParam) error {
	_, err := r.data.q.ReviewInfo.
		WithContext(ctx).
		Where(r.data.q.ReviewInfo.ReviewID.Eq(param.ReviewID)).
		Updates(map[string]any{
			"status":     param.Status,
			"op_user":    param.OpUser,
			"op_reason":  param.OpReason,
			"op_remarks": param.OpRemarks,
		})
	return err
}

// AppealReview 申诉评价（商家对用户评价进行申诉）
func (r *reviewRepo) AppealReview(ctx context.Context, param *biz.AppealParam) (*model.ReviewAppealInfo, error) {
	// 先查询有没有申诉
	ret, err := r.data.q.ReviewAppealInfo.
		WithContext(ctx).
		Where(
			query.ReviewAppealInfo.ReviewID.Eq(param.ReviewID),
			query.ReviewAppealInfo.StoreID.Eq(param.StoreID),
		).First()
	r.log.Debugf("AppealReview query, ret:%v err:%v", ret, err)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		// 其他查询错误
		return nil, err
	}
	if err == nil && ret.Status > 10 {
		return nil, errors.New("该评价已有审核过的申诉记录")
	}

	// 有申诉记录但没有审核 或者 没有申诉记录，需要创建
	appeal := &model.ReviewAppealInfo{
		ReviewID:  param.ReviewID,
		StoreID:   param.StoreID,
		Status:    10,
		Reason:    param.Reason,
		Content:   param.Content,
		PicInfo:   param.PicInfo,
		VideoInfo: param.VideoInfo,
	}
	if ret != nil {
		appeal.AppealID = ret.AppealID
	} else {
		appeal.AppealID = snowflake.GenId()
	}
	err = r.data.q.ReviewAppealInfo.
		WithContext(ctx).
		// MySQL 的 INSERT ... ON DUPLICATE KEY UPDATE ... 语义
		// 如果存在review_id,则更新. 否则插入记录
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "review_id"}, // ON DUPLICATE KEY
			},
			DoUpdates: clause.Assignments(map[string]any{ // UPDATE
				"status":     appeal.Status,
				"content":    appeal.Content,
				"reason":     appeal.Reason,
				"pic_info":   appeal.PicInfo,
				"video_info": appeal.VideoInfo,
			}),
		}).
		Create(appeal) // INSERT
	if err != nil {
		r.log.Errorf("AppealReview, err:%v", err)
		return nil, err
	}

	return appeal, err
}

// AuditAppeal 审核申诉（运营对商家的申诉进行审核，审核通过会隐藏该评价）
func (r *reviewRepo) AuditAppeal(ctx context.Context, param *biz.AuditAppealParam) error {
	err := r.data.q.Transaction(func(tx *query.Query) error {
		// 申诉表
		if _, err := tx.ReviewAppealInfo.
			WithContext(ctx).
			Where(r.data.q.ReviewAppealInfo.AppealID.Eq(param.AppealID)).
			Updates(map[string]any{
				"status":    param.Status,
				"op_user":   param.OpUser,
				"op_reason": param.OpReason,
			}); err != nil {
			return err
		}
		// 评价表
		if param.Status == 20 { // 申诉通过则需要隐藏评价
			if _, err := tx.ReviewInfo.WithContext(ctx).
				Where(tx.ReviewInfo.ReviewID.Eq(param.ReviewID)).
				Update(tx.ReviewInfo.Status, 40); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// ListReviewByUserID 根据userID查询所有评价
func (r *reviewRepo) ListReviewByUserID(ctx context.Context, userID string, lastID int64, limit int) ([]*model.ReviewInfo, error) {
	query := r.data.q.ReviewInfo.WithContext(ctx).Where(r.data.q.ReviewInfo.UserID.Eq(userID))

	// 如果有 lastID，筛选ID, 比lastID大跳过
	if lastID > 0 {
		query = query.Where(r.data.q.ReviewInfo.ID.Lt(lastID))
	}

	// 然后按最新的评价排序结果,因为mysql查询出来的结果不一定是按主键顺序
	return query.Order(r.data.q.ReviewInfo.ID.Desc()).Limit(limit).Find()
}

func (r *reviewRepo) ListReviewByStoreAndSpu(ctx context.Context, param *biz.ListReviewBySAndSParam) ([]*model.ReviewInfo, error) {
	// 1. 先查缓存
	queryHash := fmt.Sprintf(`{"store_id":"%s","spu_id":%d,"sort_field":"%s","sort_order":"%s","last_sort_value":%d,"last_id":%d,"size":%d}`,
		param.StoreId, param.SpuId, param.SortField, param.SortOrder, param.LastSortValue, param.LastId, param.Size,
	)
	hash := md5.Sum([]byte(queryHash))
	key := fmt.Sprintf("review:lrss:%x", hash)

	data, err := r.data.redisClient.Get(ctx, key).Bytes()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	hm := &types.HitsMetadata{}
	// 2. 如果缓存未命中,使用单飞模式查es
	if err == redis.Nil {
		v, err, _ := r.g.Do(key, func() (any, error) {
			return r.getReviewByStoreAndSpuFromEs(ctx, param)
		})
		if err != nil {
			return nil, err
		}
		hm = v.(*types.HitsMetadata)

		value, err := json.Marshal(hm)
		if err != nil {
			r.log.Warn("序列化es查询返回值失败")
		} else {
			if err := r.data.redisClient.Set(ctx, key, value, time.Second*30).Err(); err != nil {
				r.log.Warnf("redis缓存 key=%v 失败,", key)
			}
		}
	} else {
		// 缓存命中,解析数据
		if err := json.Unmarshal(data, hm); err != nil {
			return nil, err
		}
	}

	// 3. 解析查询结果
	var reviews []*model.ReviewInfo
	for _, hit := range hm.Hits {
		var review *model.ReviewInfo
		if err := json.Unmarshal(hit.Source_, &review); err != nil {
			r.log.WithContext(ctx).Errorf("ListReviewByStoreAndSpu JSON unmarshal failed: %v", err)
			return nil, err
		}
		reviews = append(reviews, review)
	}

	return reviews, nil
}

func (r *reviewRepo) getReviewByStoreAndSpuFromEs(ctx context.Context, param *biz.ListReviewBySAndSParam) (*types.HitsMetadata, error) {
	// 从es里查询结果,根据SortField字段排序
	// 构造查询条件
	query := &types.Query{
		Bool: &types.BoolQuery{
			Filter: []types.Query{
				{
					Term: map[string]types.TermQuery{
						"store_id": {Value: param.StoreId},
					},
				},
				{
					Term: map[string]types.TermQuery{
						"spu_id": {Value: param.SpuId},
					},
				},
			},
		},
	}

	// 构造排序条件
	sort := []types.SortCombinations{
		types.SortOptions{
			SortOptions: map[string]types.FieldSort{
				param.SortField: {Order: &sortorder.SortOrder{Name: param.SortOrder}},
			},
		},
		types.SortOptions{
			SortOptions: map[string]types.FieldSort{
				"review_id": {Order: &sortorder.SortOrder{Name: "desc"}},
			},
		},
	}

	// 构造 search_after 参数
	var searchAfter []types.FieldValue
	// 如果是第一次查询,不带 searchAfter 参数,默认从0查询size条数据
	// 判断是否同时传入,避免0值影响结果
	if param.LastId > 0 && param.LastSortValue > 0 {
		searchAfter = []types.FieldValue{
			int64(param.LastSortValue),
			param.LastId,
		}
	}

	// 执行查询
	res, err := r.data.esClient.Search().
		Index(r.data.esClient.ReviewInfosIdx). // 指定索引名
		Query(query).                          // 设置查询条件
		Sort(sort...).                         // 设置排序条件
		SearchAfter(searchAfter...).           // 设置 search_after 参数
		Size(int(param.Size)).                 // 设置分页大小
		Do(ctx)                                // 执行查询
	if err != nil {
		r.log.WithContext(ctx).Errorf("ListReviewByStoreAndSpu ES query failed: %v", err)
		return nil, err
	}

	return &res.Hits, nil
}
