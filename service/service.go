package service

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"tool.site/dbsync/config"
	"tool.site/dbsync/database"
)

// Sync 同步函数
func Sync() {

	var err error
	var rows [][]string
	var affectID int64
	var offset int64
	var fistFlag bool

	// 读取配置文件到struct,初始化变量
	config.InitConfig()

	//连接数据库 同步表结构
	dstDB := database.GetDB(config.C.Destination)
	srcDB := database.GetDB(config.C.Source)

	//同步数据
	for _, table := range config.C.Table {

		if table.Rebuild {
			err = truncateTable(dstDB, table)
			if err != nil {
				log.Println("err:", err)
				return
			}
		}

		fistFlag = true
		offset, err = fetchDstCount(dstDB, table)
		if err != nil {
			log.Println("err:", err)
			return
		}

		for fistFlag || len(rows) > 0 {
			rows, offset, err = fetchSrcRow(srcDB, table, offset, table.Batch)
			if err != nil {
				log.Println("err:", err)
				return
			}

			fistFlag = false

			//TODO 如果数据插入异常怎么办 主键重复
			for _, row := range rows {
				affectID, err = insertDstRow(dstDB, table, row)
				if err != nil {
					log.Println("err:", err)
					return
				}
				if affectID == 0 {
					err = errors.New("affected rows is zero")
					log.Println("err:", err)
					return
				}
			}
		}
		log.Println("Done with Table " + table.Name)
	}
	return
}

// insertDstRow insertDstRow
func insertDstRow(db *sql.DB, table config.TableInfo, row []string) (affect int64, err error) {
	str := ""
	for _, s := range row {
		if s != "nil" {
			str = fmt.Sprintf("%s,%s", str, convertString(s))
		} else {
			str = fmt.Sprintf("%s,null", str)
		}
	}
	var s = "insert into " + table.Name + " values (" + strings.Trim(str, ",") + ")"
	var ret sql.Result
	ret, err = db.Exec(s)
	if err != nil {
		return 0, err
	}
	rowCount, err := ret.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowCount, nil
}

// fetchSrcRow fetchSrcRow
func fetchSrcRow(db *sql.DB, table config.TableInfo, offset int64, size int64) (ret [][]string, newOffset int64, err error) {
	var rows *sql.Rows
	str := "1"
	var sl = []string{str}
	sl = append(sl, table.Where...)
	clause := strings.Join(sl[:], " and ")
	var sqlStr = "select * from " + table.Name + " where " + clause + " limit " + strconv.FormatInt(offset, 10) + "," + strconv.FormatInt(size, 10)

	log.Println(sqlStr)

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
