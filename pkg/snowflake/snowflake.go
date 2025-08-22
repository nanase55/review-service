package snowflake

import (
	"errors"
	"time"

	"github.com/bwmarrin/snowflake"
)

var (
	ErrInvalidInitParam  = errors.New("snowflake初始化失败,无效的startTime或machineId")
	ErrInvalidTimeFormat = errors.New("snowflake初始化失败,无效的startTime格式")
)

var node *snowflake.Node

func Init(startTime string, machineId int64) error {
	if len(startTime) == 0 || machineId <= 0 {
		return ErrInvalidInitParam
	}

	st, err := time.Parse("2006-01-02", startTime)
	if err != nil {
		return ErrInvalidTimeFormat
	}

	snowflake.Epoch = st.UnixNano() / 1000000
	node, err = snowflake.NewNode(machineId)
	return err
}

// GenId 生成一个id
func GenId() int64 {
	return node.Generate().Int64()
}
