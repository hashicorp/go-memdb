package explorer_server

import (
	"github.com/gin-gonic/gin"
	"github.com/manhdaovan/go-memdb"
)

func ListAllTables(gCtx *gin.Context) {
	connector, err := memdb.GetGlobalExplorer()
	if err != nil {
		gCtx.JSON(500, err)
		return
	}

	tables, err := connector.ListAllTablesName()
	if err != nil {
		gCtx.JSON(500, err)
		return
	}

	gCtx.JSON(200, tables)

	//gCtx.HTML(200, "list_all_tables.tmpl", []string{})
}
