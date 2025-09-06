package server

import (
	"context"
	"review-service/internal/biz"

	"github.com/go-kratos/kratos/v2/log"
)

type GoroutineManager struct {
	log    log.Helper
	cancel context.CancelFunc
	tasks  []func(context.Context)
}

func NewGoroutineManager(logger log.Logger, uc *biz.ReviewUsecase) *GoroutineManager {
	gm := &GoroutineManager{
		tasks: make([]func(context.Context), 0, 2),
		log:   *log.NewHelper(logger),
	}
	gm.tasks = append(gm.tasks, func(ctx context.Context) {
		// 读取消息并提交到工作池
		uc.ConsumeReviewEventsLoop(ctx)
	})
	gm.tasks = append(gm.tasks, func(ctx context.Context) {
		// 统一提交消息
		uc.CommitMessagesLoop(ctx)
	})

	return gm
}

func (gm *GoroutineManager) Start(ctx context.Context) error {
	gm.log.Info("GoroutineManager start...")
	bizCtx, cancel := context.WithCancel(ctx)
	gm.cancel = cancel

	for _, task := range gm.tasks {
		go task(bizCtx)
	}

	return nil
}

func (gm *GoroutineManager) Stop(ctx context.Context) error {
	gm.log.Info("GoroutineManager stop...")
	if gm.cancel != nil {
		gm.cancel()
	}
	return nil
}
