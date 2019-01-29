package explorer_server

import (
	"github.com/gin-gonic/gin"
	"github.com/manhdaovan/go-memdb"
)

func ListAllTables(gCtx *gin.Context) {
	connector := memdb.GetGlobalConnector()
	gCtx.JSON(200, connector.ListAllTablesName())

	//gCtx.HTML(200, "list_all_tables.tmpl", []string{})
}
