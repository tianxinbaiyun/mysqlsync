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
	var lastID int64
	var fistFlag bool

	//读取配置文件到struct
	config.InitConfig()

	//连接数据库 同步表结构
	dstDB := database.GetConn(config.W.Destination)
	srcDB := database.GetConn(config.W.Source)

	//同步数据
	for _, table := range config.W.Table {

		if table.Rebuild {
			err = truncateTable(dstDB, table)
			if err != nil {
				goto EXCEPTION
			}
		}

		fistFlag = true
		lastID, err = fetchDstLatestID(dstDB, table)
		if err != nil {
			goto EXCEPTION
		}

		for fistFlag || len(rows) > 0 {
			rows, lastID, err = fetchSrcRow(srcDB, table, lastID, table.Batch)
			if err != nil {
				goto EXCEPTION
			}

			fistFlag = false

			//TODO 如果数据插入异常怎么办 主键重复
			for _, row := range rows {
				affectID, err = insertDstRow(dstDB, table, row)
				if err != nil {
					goto EXCEPTION
				}
				if affectID == 0 {
					err = errors.New("affected rows is zero")
					goto EXCEPTION
				}
			}
		}
		log.Println("Done with Table " + table.Name)
	}
	return
EXCEPTION:
	log.Println("Aparently Oops -> ", err)
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
func fetchSrcRow(db *sql.DB, table config.TableInfo, id int64, size int64) (ret [][]string, offset int64, err error) {
	var rows *sql.Rows
	str := "1"
	var sl = []string{str}
	sl = append(sl, table.Where...)
	clause := strings.Join(sl[:], " and ")
	var sql = "select * from " + table.Name + " where " + clause + " limit " + strconv.FormatInt(id, 10) + "," + strconv.FormatInt(size, 10)

	log.Println(sql)

	rows, err = db.Query(sql)
	if err != nil {
		return nil, 0, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}
	lsize := len(columns)
	pts := make([]interface{}, lsize)
	container := make([]interface{}, lsize)
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
		//offset, err = strconv.ParseInt(sl[0], 10, 0)
		//if err != nil {
		//	return nil, 0, err
		//}
	}
	rows.Close()
	if offset == 0 {
		offset = id + size
	}
	log.Println("Fetched offset:", offset, " - size:", size)
	return ret, offset, nil
}

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

func fetchDstLatestID(db *sql.DB, table config.TableInfo) (id int64, err error) {
	var rows *sql.Rows
	var sql = "select count(*) as id from " + table.Name
	rows, err = db.Query(sql)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			log.Println(1)
			return 0, err
		}
	}
	rows.Close()
	return id, nil
}

func truncateTable(db *sql.DB, table config.TableInfo) (err error) {
	var sql = "truncate table " + table.Name
	_, err = db.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

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
