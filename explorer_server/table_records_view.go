package explorer_server

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"reflect"
	"strconv"
)

var limitValues []uint64 = []uint64{10, 50, 100, 200}

type tableRecordsViewParams struct {
	table       string
	limit       uint64
	currentPage uint64
	format      string
}

func (p *tableRecordsViewParams) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"table":  p.table,
		"limit":  p.limit,
		"page":   p.currentPage,
		"format": p.format,
	}
}

func (p *tableRecordsViewParams) GetTableName() string {
	return p.table
}

func (p *tableRecordsViewParams) GetLimit() uint64 {
	return p.limit
}

func (p *tableRecordsViewParams) GetOffset() uint64 {
	return p.limit * (p.currentPage - 1)
}

func (p *tableRecordsViewParams) GetCurrentPage() uint64 {
	return p.currentPage
}

func (p *tableRecordsViewParams) GetResponseFormat() string {
	return p.format
}

func (p *tableRecordsViewParams) concatParamsToUrl() string {
	return fmt.Sprintf("/data?table=%s&limit=%d&format=%s",
		p.table, p.limit, p.format)
}

func paramsFromCtx(gCtx *gin.Context) *tableRecordsViewParams {
	limit, _ := strconv.ParseUint(gCtx.DefaultQuery("limit", "100"), 10, 64)
	currentPage, _ := strconv.ParseUint(gCtx.DefaultQuery("page", "1"), 10, 64)

	return &tableRecordsViewParams{
		table:       gCtx.Query("table"),
		limit:       limit,
		currentPage: currentPage,
		format:      gCtx.DefaultQuery("format", "html"),
	}
}

func extractTableColumn(records []interface{}) []string {
	columns := make([]string, 0)
	if len(records) == 0 {
		return columns
	}

	record := records[0]
	typeOfRecord := reflect.TypeOf(record)
	if typeOfRecord.Kind() == reflect.Ptr {
		typeOfRecord = typeOfRecord.Elem()
	}
	for i := 0; i < typeOfRecord.NumField(); i++ {
		columns = append(columns, typeOfRecord.Field(i).Name)
	}

	return columns
}

func extractRecordData(record interface{}) []interface{} {
	data := make([]interface{}, 0)
	value := reflect.ValueOf(record)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	for i := 0; i < value.NumField(); i++ {
		data = append(data, value.Field(i).Interface())
	}

	return data
}

func formatTableData(records []interface{}) [][]interface{} {
	data := make([][]interface{}, 0)

	for _, rcd := range records {
		data = append(data, extractRecordData(rcd))
	}

	return data
}

func TableRecordsViewHandler(gCtx *gin.Context) {
	explorer, ok := gCtx.Get(GIN_CTX_EXPLORER)
	if !ok {
		gCtx.JSON(500, "explorer not set to gin context yet")
		return
	}

	params := paramsFromCtx(gCtx)
	records, err := explorer.(*explorerWrapper).TableRecordsView(params)
	if err != nil {
		gCtx.JSON(500, err)
		return
	}

	tables, _ := explorer.(*explorerWrapper).ListAllTablesName()
	switch params.GetResponseFormat() {
	case "json":
		renderJson(gCtx, records, params)
	case "html":
		renderHtml(gCtx, records, params, tables)
	default:
		renderHtml(gCtx, records, params, tables)
	}
}

func renderHtml(c *gin.Context, records []interface{}, params *tableRecordsViewParams, tables []string) {
	columns := extractTableColumn(records)
	data := formatTableData(records)
	paginator := paginator{
		baseUrl:     params.concatParamsToUrl(),
		currentPage: params.currentPage,
	}

	c.HTML(200,
		"table_records_view.html",
		gin.H{
			"title":   "Table Data: " + params.GetTableName(),
			"columns": columns,
			"records": data,
			"tables":  tables,
			"pages":   paginator.BuildPaginationUrls(),
			"params":  params,
			"limits":  limitValues,
		},
	)
}

func renderJson(c *gin.Context, records []interface{}, params *tableRecordsViewParams) {
	columns := extractTableColumn(records)

	c.JSON(200, gin.H{
		"columns": columns,
		"records": records,
		"params":  params.ToMap(),
	})
}
