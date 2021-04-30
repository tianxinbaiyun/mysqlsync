package config

import (
	"io/ioutil"
	"log"

	yaml "gopkg.in/yaml.v2"
)

//Config 配置信息 yaml 结构体
type Config struct {
	Version     string      `yaml:"version"`
	Source      Conn        `yaml:"src"`
	Destination Conn        `yaml:"dst"`
	Table       []TableInfo `yaml:"table"`
}

//Conn 数据库连接结构体
type Conn struct {
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Pass     string `yaml:"pwd"`
	Database string `yaml:"dbname"`
	Port     string `yaml:"port"`
}

//TableInfo 表结构体
type TableInfo struct {
	Name    string   `yaml:"name"`
	Rebuild bool     `yaml:"rebuild"`
	Batch   int64    `yaml:"batch"`
	Where   []string `yaml:"where"`
}

// C 全局配置信息
var C = Config{}

// InitConfig 初始化配置
func InitConfig() {
	ret, err := ioutil.ReadFile("./config.yaml")
	if err != nil {
		log.Println(err)
	}
	err = yaml.Unmarshal(ret, &C)
	if err != nil {
		panic(err)
	}
}
