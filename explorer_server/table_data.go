package explorer_server

import (
	"github.com/gin-gonic/gin"
	"github.com/manhdaovan/go-memdb"
)

func TableDataView(gCtx *gin.Context) {
	connector := memdb.GetGlobalConnector()
	data, err := connector.TableDateView()
	if err != nil {
		gCtx.JSON(200, err)
	}else{
		gCtx.JSON(200, data)
	}

	//gCtx.HTML(200, "table_data_view.tmpl", []string{})
}
