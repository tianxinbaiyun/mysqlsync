package service

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"tool.site/dbsync/config"
	"tool.site/dbsync/database"
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
			err = truncateTable(dstDB, table)
			if err != nil {
				log.Println("err:", err)
				return
			}
		}

		fistFlag = true
		syncCount := 0

		// 获取目标表数据数量
		offset, err = fetchDstCount(dstDB, table)
		if err != nil {
			log.Println("err:", err)
			return
		}

		for fistFlag || len(rows) > 0 {

			// 从新获取数据
			rows, offset, err = fetchSrcRow(srcDB, table, offset, table.Batch)
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
				affectID, err = insertDstRow(dstDB, table, row)
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

// insertDstRow insertDstRow
func insertDstRow(db *sql.DB, table config.TableInfo, row []string) (affect int64, err error) {

	// 拼凑values
	values := ""
	for _, s := range row {
		if s != "nil" {
			values = fmt.Sprintf("%s,%s", values, convertString(s))
		} else {
			values = fmt.Sprintf("%s,null", values)
		}
	}
	values = strings.Trim(values, ",")

	var sqlStr = fmt.Sprintf("insert into %s values ( %s )", table.Name, values)
	var ret sql.Result
	ret, err = db.Exec(sqlStr)
	if err != nil {
		return 0, err
	}
	rowCount, err := ret.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowCount, nil
}

// fetchSrcRow 查询源数据
func fetchSrcRow(db *sql.DB, table config.TableInfo, offset int64, size int64) (ret [][]string, newOffset int64, err error) {
	var rows *sql.Rows

	// 条件字符串拼接
	sl := make([]string, 0)
	sl = append(sl, table.Where...)
	andWhere := strings.Join(sl[:], " and ")
	andWhere = strings.Trim(andWhere, "and")
	var sqlStr = fmt.Sprintf("select * from %s", table.Name)
	if andWhere != "" && strings.Trim(andWhere, " ") != "1" {
		sqlStr = fmt.Sprintf("%s where %s", sqlStr, andWhere)
	}
	sqlStr = fmt.Sprintf("%s limit %d,%d", sqlStr, offset, size)

	log.Println(sqlStr)

	// 查询数据
	rows, err = db.Query(sqlStr)
	if err != nil {
		return nil, 0, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}
	colSize := len(columns)
	pts := make([]interface{}, colSize)
	container := make([]interface{}, colSize)
	for i := range pts {
		pts[i] = &container[i]
	}

	for rows.Next() {
		err = rows.Scan(pts...)
		if err != nil {
			return nil, 0, err
		}
		sl := toString(container)
		ret = append(ret, sl)
	}
	err = rows.Close()
	if err != nil {
		return nil, 0, err
	}
	if newOffset == 0 {
		newOffset = offset + size
	}
	log.Println("Fetched offset:", offset, " - size:", size)
	return ret, newOffset, nil
}

// toString 转成字符串
func toString(columns []interface{}) []string {
	var strCln []string
	for _, column := range columns {
		if column == nil {
			column = []uint8{'n', 'i', 'l'}
		}
		strCln = append(strCln, string(column.([]uint8)))
	}
	return strCln
}

// fetchDstCount 获取dst库表记录数量
func fetchDstCount(db *sql.DB, table config.TableInfo) (count int64, err error) {
	var rows *sql.Rows
	var sqlStr = "select count(*) as count from " + table.Name
	rows, err = db.Query(sqlStr)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0, err
		}
	}
	err = rows.Close()
	if err != nil {
		return 0, err
	}
	return count, nil
}

// truncateTable 清空表数据
func truncateTable(db *sql.DB, table config.TableInfo) (err error) {
	var sqlStr = "truncate table " + table.Name
	log.Println(sqlStr)
	_, err = db.Exec(sqlStr)
	if err != nil {
		return err
	}
	return nil
}

// convertString 字段内容单引号转换
func convertString(arg string) string {
	var buf strings.Builder
	buf.WriteRune('\'')
	for _, c := range arg {
		if c == '\\' || c == '\'' {
			buf.WriteRune('\\')
		}
		buf.WriteRune(c)
	}
	buf.WriteRune('\'')
	return buf.String()
}
