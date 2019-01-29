package explorer

import (
	"github.com/gin-gonic/gin"
)

func NewServer() *gin.Engine {
	sv := gin.Default()

	// Add routes
	sv.GET("/", ListAllTables)
	sv.GET("/table", TableDataView)

	// Add handlers
	sv.Use(HandleInternalServerError)

	// Load views
	sv.LoadHTMLGlob("../explorer/templates/*")

	return sv
}

