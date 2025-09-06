package data

import (
	"errors"
	"review-service/internal/conf"
	"review-service/internal/data/query"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(
	NewData,
	NewDB,
	NewReviewRepo,
	NewESClient,
	NewRedisClient,
	NewMQClient,
)

// Data .
type Data struct {
	// TODO wrapped database client
	// db *gorm.DB	不需要这个
	q           *query.Query
	log         *log.Helper
	esClient    *ESClient
	redisClient *redis.Client
	mqClient    *MQClient
}

// NewData .
func NewData(db *gorm.DB, logger log.Logger, es *ESClient, rdb *redis.Client, mq *MQClient) (*Data, func(), error) {
	cleanup := func() {
		log.NewHelper(logger).Info("closing the data resources")
		rdb.Close()
		mq.kafkaReader.Close()
		mq.kafkaWriter.Close()
	}

	// 非常重要!为GEN生成的query代码设置数据库连接对象
	query.SetDefault(db)

	return &Data{
		q:           query.Q, // query包下的Q
		log:         log.NewHelper(logger),
		esClient:    es,
		redisClient: rdb,
		mqClient:    mq,
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

func NewRedisClient(cfg *conf.Data) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		WriteTimeout: cfg.Redis.WriteTimeout.AsDuration(),
		ReadTimeout:  cfg.Redis.ReadTimeout.AsDuration(),
	})
}

type MQClient struct {
	kafkaReader       *kafka.Reader
	kafkaWriter       *kafka.Writer
	reviewStatusTopic string
	dlqTopic          string
}

func NewMQClient(cfg *conf.Kafka) *MQClient {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		GroupID: cfg.GroupId,
		Topic:   cfg.ReviewStatusTopic, // 读取消息时,无法动态指定topic,所以必须传入。并且提交时msg的topic必须一致
	})

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: cfg.Brokers,
		// topic不用设置,写入时依据mgs的topic
	})

	return &MQClient{
		kafkaReader:       reader,
		kafkaWriter:       writer,
		reviewStatusTopic: cfg.ReviewStatusTopic,
		dlqTopic:          "ReviewDLQ",
	}
}
