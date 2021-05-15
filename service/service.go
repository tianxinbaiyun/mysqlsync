package service

import (
	"errors"
	"github.com/tianxinbaiyun/mysqlsync/config"
	"github.com/tianxinbaiyun/mysqlsync/database"
	"log"
)

// Sync 同步函数
func Sync() {

	// 变量定义
	var (
		err      error
		rows     [][]string
		affectID int64
		offset   int64
		fistFlag bool
	)

	// 读取配置文件到struct,初始化变量
	config.InitConfig()

	//连接数据库 同步表结构
	dstDB := database.GetDB(config.C.Destination)
	srcDB := database.GetDB(config.C.Source)

	//同步数据
	for _, table := range config.C.Table {
		// 如果配置重建，则清空数据
		if table.Rebuild {
			err = database.TruncateTable(dstDB, table)
			if err != nil {
				log.Println("err:", err)
				return
			}
		}

		fistFlag = true
		syncCount := 0

		// 获取目标表数据数量
		offset, err = database.GetCount(dstDB, table.Name)
		if err != nil {
			log.Println("err:", err)
			return
		}

		for fistFlag || len(rows) > 0 {

			// 从新获取数据
			rows, offset, err = database.GetRows(srcDB, table, offset, table.Batch)
			if err != nil {
				log.Println("err:", err)
				return
			}

			rowLen := len(rows)

			if rowLen <= 0 {
				break
			}
			fistFlag = false

			// 循环插入数据
			for _, row := range rows {
				affectID, err = database.Insert(dstDB, table.Name, row)
				if err != nil {
					log.Println("err:", err)
					return
				}
				if affectID == 0 {
					err = errors.New("affected rows is 0")
					log.Println("err:", err)
					return
				}
			}

			// 统计同步数量
			syncCount = syncCount + rowLen

			// 如果返回数量小于size，结束循环
			if int64(rowLen) < table.Batch {
				break
			}
		}
		log.Printf("sync done Table %s sync count %d", table.Name, syncCount)
	}
	return
}
