package server

import (
	"context"
	"review-service/internal/biz"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

type workerPool struct {
	workers int
	log     *log.Helper
	cancel  context.CancelFunc

	jobs chan func()
}

func NewWorkerPool(logger log.Logger) biz.WorkerPool {
	return &workerPool{
		jobs:    make(chan func(), 100),
		workers: 5,
		log:     log.NewHelper(logger),
	}
}

func (wp *workerPool) Start(ctx context.Context) error {
	wp.log.WithContext(ctx).Info("MessageWorkerPool start...")

	bizCtx, cancel := context.WithCancel(ctx) // 继承ctx,可被ctx.cancel()
	wp.cancel = cancel

	for i := 0; i < wp.workers; i++ {
		go func() {
			for {
				select {
				case job := <-wp.jobs:
					job()
				case <-bizCtx.Done(): // Stop() 和 Start()过程中被cancel
					return
				}
			}
		}()
	}

	return nil
}

func (wp *workerPool) Stop(ctx context.Context) error {
	wp.log.Info("MessageWorkerPool stop...")

	if wp.cancel != nil {
		wp.cancel()
	}

	return nil
}

func (wp *workerPool) Submit(job func()) bool {
	select {
	case wp.jobs <- job:
		return true // 成功提交
	case <-time.After(50 * time.Millisecond):
		return false
	}
}
