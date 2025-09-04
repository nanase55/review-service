package service

import (
	"context"

	pb "review-service/api/review/v1"
	"review-service/internal/biz"
	"review-service/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
)

type ReviewService struct {
	pb.UnimplementedReviewServer
	log *log.Helper
	uc  *biz.ReviewUsecase
}

func NewReviewService(uc *biz.ReviewUsecase, logger log.Logger) *ReviewService {
	return &ReviewService{uc: uc, log: log.NewHelper(logger)}
}

func (s *ReviewService) CreateReview(ctx context.Context, req *pb.CreateReviewRequest) (*pb.CreateReviewReply, error) {
	s.log.WithContext(ctx).Debugf("[service] CreateReview, req: %#v", req)

	review, err := s.uc.CreateReview(ctx, &model.ReviewInfo{
		UserID:       req.UserId,
		OrderID:      req.OrderId,
		Score:        req.Score,
		ServiceScore: req.ServiceScore,
		ExpressScore: req.ExpressScore,
		Content:      req.Content,
		PicInfo:      req.PicInfo,
		VideoInfo:    req.VideoInfo,
	})

	if err != nil {
		return nil, err
	}

	return &pb.CreateReviewReply{ReviewId: review.ReviewID}, nil
}

func (s *ReviewService) GetReview(ctx context.Context, req *pb.GetReviewRequest) (*pb.GetReviewReply, error) {
	s.log.WithContext(ctx).Debugf("GetReview req:%#v\n", req)

	review, err := s.uc.GetReview(ctx, req.ReviewId)

	if err != nil {
		return nil, err
	}

	return &pb.GetReviewReply{
		Data: &pb.ReviewInfo{
			ReviewId:     review.ReviewID,
			UserId:       review.UserID,
			OrderId:      review.OrderID,
			Score:        review.Score,
			ServiceScore: review.ServiceScore,
			ExpressScore: review.ExpressScore,
			Content:      review.Content,
			PicInfo:      review.PicInfo,
			VideoInfo:    review.VideoInfo,
			Status:       review.Status,
		},
	}, err
}

func (s *ReviewService) ListReviewByUserId(ctx context.Context, req *pb.ListReviewByUserIdRequest) (*pb.ListReviewByUserIdReply, error) {
	s.log.WithContext(ctx).Debugf("[service] ListReviewByUserID req:%#v\n", req)

	dataList, err := s.uc.ListReviewByUserID(ctx, req.GetUserId(), req.GetLastId(), int(req.GetSize()))
	if err != nil {
		return nil, err
	}

	list := make([]*pb.ReviewInfo, 0, len(dataList))
	for _, review := range dataList {
		list = append(list, &pb.ReviewInfo{
			ReviewId:     review.ReviewID,
			UserId:       review.UserID,
			OrderId:      review.OrderID,
			Score:        review.Score,
			ServiceScore: review.ServiceScore,
			ExpressScore: review.ExpressScore,
			Content:      review.Content,
			PicInfo:      review.PicInfo,
			VideoInfo:    review.VideoInfo,
			Status:       review.Status,
		})
	}

	if len(dataList) > 0 {
		lastId := dataList[len(dataList)-1].ID
		return &pb.ListReviewByUserIdReply{List: list, LastId: lastId}, nil
	}

	return &pb.ListReviewByUserIdReply{}, nil
}

func (s *ReviewService) ReplyReview(ctx context.Context, req *pb.ReplyReviewRequest) (*pb.ReplyReviewReply, error) {
	s.log.WithContext(ctx).Debugf("ReplyReview req:%#v\n", req)

	reply, err := s.uc.CreateReply(ctx, &biz.ReplyParam{
		ReviewID:  req.GetReviewId(),
		StoreID:   req.GetStoreId(),
		Content:   req.GetContent(),
		PicInfo:   req.GetPicInfo(),
		VideoInfo: req.GetVideoInfo(),
	})
	if err != nil {
		return nil, err
	}

	return &pb.ReplyReviewReply{ReplyId: reply.ReplyID}, nil
}

func (s *ReviewService) AppealReview(ctx context.Context, req *pb.AppealReviewRequest) (*pb.AppealReviewReply, error) {
	s.log.WithContext(ctx).Debugf("[service] AppealReview req:%#v\n", req)
	ret, err := s.uc.AppealReview(ctx, &biz.AppealParam{
		ReviewID:  req.GetReviewId(),
		StoreID:   req.GetStoreId(),
		Reason:    req.GetReason(),
		Content:   req.GetContent(),
		PicInfo:   req.GetPicInfo(),
		VideoInfo: req.GetVideoInfo(),
	})
	if err != nil {
		return nil, err
	}
	s.log.WithContext(ctx).Debugf("[service] AppealReview ret:%v err:%v\n", ret, err)
	return &pb.AppealReviewReply{AppealId: ret.AppealID}, nil
}

func (s *ReviewService) AuditReview(ctx context.Context, req *pb.AuditReviewRequest) (*pb.AuditReviewReply, error) {
	s.log.WithContext(ctx).Debugf("AuditReview req:%#v\n", req)
	err := s.uc.AuditReview(ctx, &biz.AuditParam{
		ReviewID:  req.GetReviewId(),
		OpUser:    req.GetOpUser(),
		OpReason:  req.GetOpReason(),
		OpRemarks: req.GetOpRemarks(),
		Status:    req.GetStatus(),
	})
	if err != nil {
		return nil, err
	}
	return &pb.AuditReviewReply{
		ReviewId: req.ReviewId,
		Status:   req.Status,
	}, nil
}

func (s *ReviewService) AuditAppeal(ctx context.Context, req *pb.AuditAppealRequest) (*pb.AuditAppealReply, error) {
	s.log.WithContext(ctx).Debugf("[service] AuditAppeal req:%#v\n", req)
	err := s.uc.AuditAppeal(ctx, &biz.AuditAppealParam{
		ReviewID: req.GetReviewID(),
		AppealID: req.GetAppealId(),
		OpUser:   req.GetOpUser(),
		Status:   req.GetStatus(),
		OpReason: req.GetOpReason(),
	})
	if err != nil {
		return nil, err
	}
	return &pb.AuditAppealReply{}, nil
}

func (s *ReviewService) ListReviewByStoreAndSpu(ctx context.Context, req *pb.ListReviewByStoreAndSpuRequest) (*pb.ListReviewByStoreAndSpuReply, error) {
	s.log.WithContext(ctx).Debugf("[service] ListReviewByStoreAndSpuRequest, req: %#v", req)

	dataList, err := s.uc.ListReviewByStoreAndSpu(ctx, &biz.ListReviewBySAndSParam{
		StoreId:       req.GetStoreId(),
		SpuId:         req.GetSpuId(),
		LastId:        req.GetLastId(),
		LastSortValue: req.GetSortValue(),
		Size:          req.GetSize(),
		SortOrder:     req.GetSortOrder(),
		SortField:     req.GetSortField(),
		HasMedia:      req.GetHasMedia(),
		HasReply:      req.GetHasReply(),
		KeyWords:      req.GetKeywords(),
	})
	if err != nil {
		return nil, err
	}

	list := make([]*pb.ReviewInfo, 0, len(dataList))
	for _, review := range dataList {
		list = append(list, &pb.ReviewInfo{
			ReviewId:     review.ReviewID,
			UserId:       review.UserID,
			OrderId:      review.OrderID,
			Score:        review.Score,
			ServiceScore: review.ServiceScore,
			ExpressScore: review.ExpressScore,
			Content:      review.Content,
			PicInfo:      review.PicInfo,
			VideoInfo:    review.VideoInfo,
			Status:       review.Status,
		})
	}

	if len(dataList) > 0 {
		lastId := dataList[len(dataList)-1].ReviewID
		lastScore := dataList[len(dataList)-1].Score
		return &pb.ListReviewByStoreAndSpuReply{List: list, LastId: lastId, SortValue: lastScore}, nil
	}

	return &pb.ListReviewByStoreAndSpuReply{}, nil
}
