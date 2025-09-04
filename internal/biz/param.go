package biz

// ReplyParam 商家回复评价的参数
type ReplyParam struct {
	ReviewID  int64
	StoreID   string
	Content   string
	PicInfo   string
	VideoInfo string
}

// AuditParam 运营审核评价的参数
type AuditParam struct {
	ReviewID  int64
	OpUser    string
	OpReason  string
	OpRemarks string
	Status    int32
}

// AppealParam 商家申诉评价的参数
type AppealParam struct {
	ReviewID  int64
	StoreID   string
	Reason    string
	Content   string
	PicInfo   string
	VideoInfo string
	OpUser    string
}

// AuditAppealParam O端审核商家申诉的参数
type AuditAppealParam struct {
	ReviewID int64
	AppealID int64
	OpUser   string
	Status   int32
	OpReason string
}

// 查询店铺下某个商品的评价
type ListReviewBySAndSParam struct {
	StoreId       string // 店铺ID
	SpuId         int64  // 商品SPU ID
	Size          int32  // 每页大小
	LastId        int64  // 上一页最后一条记录的_id，用于分页
	LastSortValue int32  // 上一页最后一条记录的分数，用于分页
	SortField     string // 排序字段，"score" "service_score" "express_score"
	SortOrder     string // 排序顺序，例如 "asc" 或 "desc"
	HasMedia      int32
	HasReply      int32
	KeyWords      string // 关键词
}
