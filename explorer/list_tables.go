package explorer

import "github.com/gin-gonic/gin"

func ListAllTables(gCtx *gin.Context) {
	gCtx.HTML(200, "list_all_tables.tmpl", []string{})
}
