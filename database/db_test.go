package database

import (
	"github.com/stretchr/testify/assert"
	"github.com/tianxinbaiyun/mysqlsync/config"
	"testing"
)

func init() {
	config.InitConfig()
	InitDB()
}

func TestGetDB(t *testing.T) {

}

// TestGetCount TestGetCount
func TestGetCount(t *testing.T) {
	db := GetDB(config.C.Destination)
	tableList := config.C.Table
	count, err := GetCount(db, tableList[0].Name)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	t.Log(count)
}

func TestGetRows(t *testing.T) {
	db := GetDB(config.C.Destination)
	tableList := config.C.Table
	for _, info := range tableList {
		rows, offset, err := GetRows(db, info, 0, info.Batch)
		if err != nil {
			assert.NoError(t, err)
			return
		}
		t.Log(rows)
		t.Log(offset)
	}

}
