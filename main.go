package main

import (
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"net/http"
	"tool.site/dbsync/crontab"
	"tool.site/dbsync/service"
)

func main() {

	// 定时任务
	go crontab.AddCron()

	// 添加路由
	engine := gin.New()
	engine.GET("/sync", sync) // 手动同步数据

	// 如果不是生产环境，启用pprof性能检查
	pprof.Register(engine)

	// 启用监听端口
	if err := engine.Run(":8080"); err != nil {
		panic(err)
	}

}

// 同步操作
func sync(ctx *gin.Context) {
	service.Sync()
	// 返回成功
	res := gin.H{
		"msg":    "success",
		"status": 0,
		"result": nil,
	}
	ctx.JSON(http.StatusOK, res)
}
