package data

import (
	"errors"
	"review-service/internal/conf"
	"review-service/internal/data/query"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewDB, NewReviewRepo, NewESClient)

// Data .
type Data struct {
	// TODO wrapped database client
	// db *gorm.DB	不需要这个
	q        *query.Query
	log      *log.Helper
	esClient *ESClient // ES client
}

// NewData .
func NewData(db *gorm.DB, logger log.Logger, es *ESClient) (*Data, func(), error) {
	cleanup := func() {
		log.NewHelper(logger).Info("closing the data resources")
	}

	// 非常重要!为GEN生成的query代码设置数据库连接对象
	query.SetDefault(db)

	return &Data{
		q:        query.Q, // query包下的Q
		log:      log.NewHelper(logger),
		esClient: es,
	}, cleanup, nil
}

func NewDB(cf *conf.Data) (*gorm.DB, error) {
	switch strings.ToLower(cf.Database.GetDriver()) {
	case "mysql":
		db, err := gorm.Open(mysql.Open(cf.Database.GetSource()))
		return db, err
	case "sqlite":
		db, err := gorm.Open(sqlite.Open(cf.Database.GetSource()))
		return db, err
	}
	return nil, errors.New("connect db don't supported db driver")
}

type ESClient struct {
	*elasticsearch.TypedClient
	// 索引库
	ReviewInfosIdx  string
	ReviewReplyIdx  string
	ReviewAppealIdx string
}

func NewESClient(cfg *conf.Elasticsearch) (*ESClient, error) {
	// ES 配置
	c := elasticsearch.Config{
		Addresses: cfg.Addresses,
	}

	// 创建客户端连接
	client, err := elasticsearch.NewTypedClient(c)
	if err != nil {
		return nil, err
	}
	return &ESClient{
		TypedClient:     client,
		ReviewInfosIdx:  cfg.ReviewInfosIndex,
		ReviewReplyIdx:  cfg.ReviewReplyIndex,
		ReviewAppealIdx: cfg.ReviewAppealIndex,
	}, nil
}
