package explorer_server

import (
	"github.com/gin-gonic/gin"
	"github.com/manhdaovan/go-memdb"
	"strconv"
)

func paramsFromCtx(gCtx *gin.Context) memdb.TableDataViewParams {
	limit, _ := strconv.ParseUint(gCtx.DefaultQuery("limit", "100"), 10, 64)
	offset, _ := strconv.ParseUint(gCtx.DefaultQuery("offset", "0"), 10, 64)

	return memdb.TableDataViewParams{
		Table: gCtx.Query("table"),
		Index: gCtx.DefaultQuery("index", "id"),
		Limit: limit,
		Offset: offset,
	}
}

func TableDataViewHandler(gCtx *gin.Context) {
	explorer, ok := gCtx.Get(GIN_CTX_EXPLORER)
	if !ok {
		gCtx.JSON(500, "explorer not set to gin context yet")
		return
	}

	params := paramsFromCtx(gCtx)
	records, err := explorer.(memdb.Explorer).TableDataView(params)
	if err != nil {
		gCtx.JSON(500, err)
		return
	}


	gCtx.HTML(200,
		"table_data_view.html",
		gin.H{
			"title": "Table Data: " + params.Table,
			"records": records,
		},
	)
}
