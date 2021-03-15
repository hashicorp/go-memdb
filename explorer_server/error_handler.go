package explorer_server

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
)

type ErrorCode int

const (
	AuthErrorCode ErrorCode = iota + 1001
	InternalServerErrorCode
	InvalidRequestParams
)

// HandleInternalServerError handles error 500
func InternalServerErrorHandler(c *gin.Context) {
	defer func(c *gin.Context) {
		if rec := recover(); rec != nil {
			errMsg := fmt.Sprintf("%v", rec)
			renderInternalServerError(c, errMsg)
		}
	}(c)

	c.Next()
}

// RenderInternalServerError when error 500
func renderInternalServerError(c *gin.Context, err string) {
	renderError(c, http.StatusInternalServerError, gin.H{
		"err_code": InternalServerErrorCode,
		"err_msg":  err,
	})
}

func renderError(c *gin.Context, status int, msg gin.H) {
	c.JSON(status, msg)
}
