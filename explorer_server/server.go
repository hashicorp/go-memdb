package explorer_server

import (
	"github.com/gin-gonic/gin"
	"github.com/manhdaovan/go-memdb"
)

const GIN_CTX_EXPLORER = "explorer"

func SetExplorerToGinCtxHandler(explr memdb.Explorer) gin.HandlerFunc {
	return func(gCtx *gin.Context) {
		gCtx.Set(GIN_CTX_EXPLORER, explr)
	}
}

func NewServer(explr memdb.Explorer) *gin.Engine {
	sv := gin.Default()
	// Add handlers
	sv.Use(InternalServerErrorHandler)
	sv.Use(SetExplorerToGinCtxHandler(explr))

	// Load views
	sv.Static("/assets", "../explorer_server/assets")
	sv.LoadHTMLGlob("../explorer_server/templates/*")

	// Add routes
	sv.GET("/", ListAllTablesHandler)
	sv.GET("/data", TableDataViewHandler)

	return sv
}

