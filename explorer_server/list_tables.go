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
	explr := explorer.(memdb.Explorer)
	tables, err := explr.ListAllTablesName()
	if err != nil {
		gCtx.JSON(500, err)
		return
	}

	recordCnts := make(map[string]uint64)
	for _, tbl := range tables {
		recordCnts[tbl], err = explr.CountRecords(tbl)
		if err != nil {
			gCtx.JSON(500, err)
			return
		}
	}

	gCtx.HTML(200,
		"list_all_tables.html",
		gin.H{
			"title": "List all tables",
			"tables": recordCnts,
		},
	)
}
