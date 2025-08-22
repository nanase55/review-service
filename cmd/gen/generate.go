package main

// GORM GEN生成代码配置
import (
	"flag"
	"fmt"
	"review-service/internal/conf"
	"strings"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"gorm.io/gen"
)

func connectDB(cf *conf.Data_Database) *gorm.DB {
	// 根据数据库类型使用
	switch strings.ToLower(cf.GetDriver()) {
	case "mysql":
		db, err := gorm.Open(mysql.Open(cf.GetSource()))
		if err != nil {
			panic(fmt.Errorf("connect db fail: %w", err))
		}
		return db
	case "sqlite":
		db, err := gorm.Open(sqlite.Open(cf.GetSource()))
		if err != nil {
			panic(fmt.Errorf("connect db fail: %w", err))
		}
		return db
	}
	panic("GEN: connectDB fail unsupported db driver")
}

var flagconf string

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
}

func main() {
	// 从配置文件获取数据库source
	flag.Parse()

	c := config.New(
		config.WithSource(
			file.NewSource(flagconf),
		),
	)
	defer c.Close()

	if err := c.Load(); err != nil {
		panic(err)
	}

	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		panic(err)
	}

	g := gen.NewGenerator(gen.Config{
		// 0. 默认生成一个package model，即表对应的struct，在../../internal/data/query

		// 1. 指定查询相关生成的路径
		OutPath: "../../internal/data/query",

		// gen.WithoutContext：禁用WithContext模式
		// 默认使用WithContext，用于控制超时
		// gen.WithDefaultQuery：生成一个全局Query对象Q
		// gen.WithQueryInterface：生成Query接口
		Mode:          gen.WithDefaultQuery | gen.WithQueryInterface,
		FieldNullable: true, // 表字段可以为null时，生成*类型
	})

	// 通常复用已有的SQL连接配置db(*gorm.DB)
	g.UseDB(connectDB(bc.Data.Database))

	// g.GenerateAllTable()生成表结构 在package model
	// 为所有表生成Model结构体和CRUD代码
	g.ApplyBasic(g.GenerateAllTable()...)

	// 执行并生成代码
	g.Execute()
}
