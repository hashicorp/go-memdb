package explorer_server

import (
	"github.com/gin-gonic/gin"
	"github.com/manhdaovan/go-memdb"
)

func ListAllTablesHandler(gCtx *gin.Context){
	explorer, ok := gCtx.Get(GIN_CTX_EXPLORER)
	if !ok {
		gCtx.JSON(500, "explorer not set to gin context yet")
		return
	}

	tables, err := explorer.(memdb.Explorer).ListAllTablesName()
	if err != nil {
		gCtx.JSON(500, err)
		return
	}

	gCtx.HTML(200,
		"list_all_tables.html",
		gin.H{
			"tables": tables,
			"title": "List all tables",
		},
	)
}
