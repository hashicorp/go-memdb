package explorer_server

import (
	"github.com/gin-gonic/gin"
	"github.com/manhdaovan/go-memdb"
)

const GIN_CTX_EXPLORER = "explorer"
type explorerWrapper struct {
	memdb.Explorer
}

func SetExplorerToGinCtxHandler(explr *explorerWrapper) gin.HandlerFunc {
	return func(gCtx *gin.Context) {
		gCtx.Set(GIN_CTX_EXPLORER, explr)
	}
}

func NewServer(explr memdb.Explorer, assetsPath, templatesPath string) *gin.Engine {
	sv := gin.Default()
	// Add handlers
	sv.Use(InternalServerErrorHandler)
	explrWrapper := explorerWrapper{explr}
	sv.Use(SetExplorerToGinCtxHandler(&explrWrapper))

	// Load views
	sv.Static("/assets", assetsPath)
	sv.LoadHTMLGlob(templatesPath)

	// Add routes
	sv.GET("/", ListAllTablesHandler)
	sv.GET("/data", TableRecordsViewHandler)

	return sv
}
