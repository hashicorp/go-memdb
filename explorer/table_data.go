package explorer

import "github.com/gin-gonic/gin"

func TableDataView(gCtx *gin.Context) {
	gCtx.HTML(200, "table_data_view.tmpl", []string{})
}
