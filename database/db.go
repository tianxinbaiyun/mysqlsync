package database

import (
	"database/sql"
	"log"

	"github.com/tianxinbaiyun/mysqlsync/config"

	_ "github.com/go-sql-driver/mysql" //mysql
)

// DB 数据库定义
var DB = make(map[string]*sql.DB)

// InitDB 初始化连接
func InitDB() {
	GetDB(config.C.Source)
	GetDB(config.C.Destination)
}

// GetDB 获取连接
func GetDB(conn config.Conn) *sql.DB {
	if _, ok := DB[conn.Host]; ok {
		return DB[conn.Host]
	}
	//root:root@tcp(127.0.0.1:3306)/test
	dsn := conn.User + ":" + conn.Pass + "@tcp(" + conn.Host + ":" + conn.Port + ")/" + conn.Database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	DB[conn.Host] = db
	return db
}
