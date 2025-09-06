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
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type reviewRepo struct {
	data *Data
	log  *log.Helper

	g    singleflight.Group
	pool biz.WorkerPool
}

func NewReviewRepo(data *Data, logger log.Logger, pool biz.WorkerPool) biz.ReviewRepo {
	return &reviewRepo{
		data: data,
		log:  log.NewHelper(logger),
		pool: pool,
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
	// Json字符串序列化后作为key,反序列化时方便调试查看
	queryHash := fmt.Sprintf(
		`{"store_id":"%s","spu_id":%d,"sort_field":"%s","sort_order":"%s","last_sort_value":%d,"last_id":%d,"size":%d,"has_media":%v,"has_reply:%v","keywords:%s"}`,
		param.StoreId,
		param.SpuId,
		param.SortField,
		param.SortOrder,
		param.LastSortValue,
		param.LastId,
		param.Size,
		param.HasMedia,
		param.HasReply,
		param.KeyWords,
	)
	r.log.Debugf("ListReviewByStoreAndSpu 查redis key: %s", queryHash)
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

	// 动态添加筛选条件
	if param.HasMedia == 1 { // 是否有图
		query.Bool.Filter = append(query.Bool.Filter, types.Query{
			Term: map[string]types.TermQuery{
				"has_media": {Value: param.HasMedia},
			},
		})
	}

	if param.HasReply == 1 { // 是否有商家回复
		query.Bool.Filter = append(query.Bool.Filter, types.Query{
			Term: map[string]types.TermQuery{
				"has_reply": {Value: param.HasReply},
			},
		})
	}

	// 动态添加关键词搜索条件
	if param.KeyWords != "" {
		query.Bool.Must = append(query.Bool.Must, types.Query{
			Match: map[string]types.MatchQuery{
				"content": {
					Query: param.KeyWords,
				},
			},
		})
	}

	// ps: 下面的实现还有带商榷,需要根据业务组合
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

// PublishUpdateReviewEvent 发送更新评价事件
func (r *reviewRepo) PublishUpdateReviewEvent(ctx context.Context, review *model.ReviewInfo) error {
	r.log.WithContext(ctx).Debugf("PublishUpdateReviewEvent ReviewID: %v", review.ReviewID)

	event := &biz.ReviewStatsEvent{
		ReviewID: review.ReviewID,
		StoreID:  review.StoreID,
		SpuID:    review.SpuID,
		Score:    review.Score,
		HasMedia: review.HasMedia,
	}
	data, _ := json.Marshal(event)
	// 最大重试次数
	const maxRetries = 3
	var err error

	for i := range maxRetries {
		err = r.data.mqClient.kafkaWriter.WriteMessages(ctx, kafka.Message{
			Topic: r.data.mqClient.reviewStatusTopic,
			Value: data,
		})
		if err == nil {
			// 发送成功
			return nil
		}
		r.log.WithContext(ctx).Warnf("PublishUpdateReviewEvent failed, retrying... (%d/%d), err: %v", i+1, maxRetries, err)
		time.Sleep(time.Second * 2) // 重试间隔
	}

	// 如果重试失败，记录错误日志
	r.log.WithContext(ctx).Errorf("PublishUpdateReviewEvent failed after %d retries, err: %v", maxRetries, err)
	// 兜底机制: 加入DLQ队列
	r.PublishDLQEvent(ctx, data)
	return err
}

// PublishDLQEvent 发送事件到DLQ(Dead Letter Queue)
func (r *reviewRepo) PublishDLQEvent(ctx context.Context, data []byte) {
	r.log.WithContext(ctx).Warnf(" PublishDLQEvent")
	// 最大重试次数
	const maxRetries = 3
	var err error

	for i := range maxRetries {
		err = r.data.mqClient.kafkaWriter.WriteMessages(ctx, kafka.Message{
			Topic: r.data.mqClient.dlqTopic,
			Value: data,
		})
		if err == nil {
			// 发送成功
			return
		}
		r.log.WithContext(ctx).Warnf("PublishUpdatePublishDLQEventReviewEvent failed, retrying... (%d/%d), err: %v", i+1, maxRetries, err)
		time.Sleep(time.Second * 2) // 重试间隔
	}

	// 如果重试失败，记录错误日志
	r.log.WithContext(ctx).Errorf("PublishDLQEvent failed after %d retries, err: %v", maxRetries, err)
	// 再兜底,本地持久化
}

// ReadReviewMessage 读取消息
func (r *reviewRepo) ReadReviewMessage(ctx context.Context) (*kafka.Message, error) {
	msg, err := r.data.mqClient.kafkaReader.ReadMessage(ctx)
	return &msg, err
}

// CommitReviewMessages 提交消息
func (r *reviewRepo) CommitReviewMessages(ctx context.Context, msg *kafka.Message) error {
	if msg.Topic == "" {
		msg.Topic = r.data.mqClient.kafkaReader.Config().Topic
	}
	return r.data.mqClient.kafkaReader.CommitMessages(ctx, *msg)
}

func (r *reviewRepo) UpdateReviewStats(ctx context.Context, param *biz.ReviewStatsEvent) error {
	var p_cnt, n_cnt int
	if param.Score > 3 {
		p_cnt = 1
	} else if param.Score < 3 {
		n_cnt = 1
	}

	err := r.data.q.Transaction(func(tx *query.Query) error {
		// 使用原生 SQL 执行 UPSERT，性能最优
		sql := `
            INSERT INTO review_stats (store_id, spu_id, total_count, avg_score, positive_rate, negative_rate, has_media_rate, update_at)
            VALUES (?, ?, 1, ?, ?, ?, ?, NOW())
            ON DUPLICATE KEY UPDATE
                total_count = total_count + 1,
                avg_score = (avg_score * total_count + VALUES(avg_score)) / (total_count + 1),
                positive_rate = (positive_rate * total_count + VALUES(positive_rate)) / (total_count + 1),
                negative_rate = (negative_rate * total_count + VALUES(negative_rate)) / (total_count + 1),
                has_media_rate = (has_media_rate * total_count + VALUES(has_media_rate)) / (total_count + 1),
                update_at = NOW()
        `

		// 获取事务中的 gorm.DB 实例
		db := tx.ReviewInfo.WithContext(ctx).UnderlyingDB()
		if err := db.Exec(sql,
			param.StoreID, param.SpuID,
			param.Score, float64(p_cnt), float64(n_cnt), float64(param.HasMedia),
		).Error; err != nil {
			r.log.WithContext(ctx).Errorf("UpdateReviewStats failed, err: %v", err)
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	// 异步更新 Redis 缓存，避免阻塞主流程
	r.pool.Submit(func() {
		cacheKey := fmt.Sprintf("review_stats:%s:%d", param.StoreID, param.SpuID)
		if err := r.data.redisClient.Del(context.Background(), cacheKey).Err(); err != nil {
			r.log.Warnf("Failed to clear Redis cache for key: %s, err: %v", cacheKey, err)
		}
	})

	return nil
}

// SetRedis 实现 Redis 的 set命令
func (r *reviewRepo) SetRedis(ctx context.Context, key string, value any) error {
	if err := r.data.redisClient.Set(ctx, key, value, 30*time.Second).Err(); err != nil {
		r.log.WithContext(ctx).Errorf("SetRedis failed, key: %s, err: %v", key, err)
		return err
	}
	return nil
}

// ExistsRedis 判断 Redis 是否存在 key
func (r *reviewRepo) ExistsRedis(ctx context.Context, key string) (bool, error) {
	exists, err := r.data.redisClient.Exists(ctx, key).Result()
	if err != nil {
		r.log.WithContext(ctx).Errorf("ExistsRedis failed, key: %s, err: %v", key, err)
		return false, err
	}
	return exists == 1, nil
}
