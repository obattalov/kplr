package api

import (
	"fmt"
	"strings"
)

type (
	QueryParams map[string]string

	SelectQuery struct {
		Query  string       `json:"query"`
		Params *QueryParams `json:"params,omitempty"`
	}
)

func (sq *SelectQuery) String() string {
	return fmt.Sprint("{query=", sq.Query, ", params=", sq.Params, "}")
}

func (sq *SelectQuery) applyParams() string {
	if sq.Params == nil || len(*sq.Params) == 0 {
		return sq.Query
	}

	params := make([]string, 0, len(*sq.Params)*2)
	for p, v := range *sq.Params {
		params = append(params, ":"+p, v)
	}

	r := strings.NewReplacer(params...)
	return r.Replace(sq.Query)
}
