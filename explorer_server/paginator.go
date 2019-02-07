package explorer_server

import (
	"fmt"
	"strings"
)

type paginator struct {
	baseUrl string
	currentPage uint64
}

type page struct {
	CurrentPage uint64
	PageNumber uint64
	Link string
}

func (p *paginator) BuildPaginationUrls() []page {
	combineChar := "?"
	if strings.Contains(p.baseUrl, "?") {
		combineChar = "&"
	}

	var min uint64 = 1
	if p.currentPage > 10 {
		min = p.currentPage - 10
	}
	max := p.currentPage + 20

	pages := make([]page, 0)
	for pageNumber := min; pageNumber <= max; pageNumber ++ {
		pages = append(pages, page{
			Link: fmt.Sprintf("%s%spage=%d", p.baseUrl, combineChar, pageNumber),
			PageNumber: pageNumber,
			CurrentPage: p.currentPage,
		})
	}

	return pages
}
