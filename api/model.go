package api

import (
	"fmt"
	"strings"

	"github.com/kplr-io/kplr"
)

type (
	QueryParams map[string]string

	QueryRequest struct {
		Query  *string      `json:"query,omitempty"`
		Params *QueryParams `json:"params,omitempty"`
	}

	CurDescDO struct {
		Id          string           `json:"id"`
		Created     kplr.ISO8601Time `json:"created"`
		LastTouched kplr.ISO8601Time `json:"accessed"`
		LastKQL     string           `json:"lastQuery"`
		Position    string           `json:"position"`
	}

	PageDo struct {
		Data   interface{} `json:"data"`
		Offset int         `json:"offset"`
		Count  int         `json:"count"`
		Total  int         `json:"total"`
	}
)

func toCurDescDO(cd *cur_desc, curId string) *CurDescDO {
	var curDO CurDescDO
	curDO.Id = curId
	curDO.Created = kplr.ISO8601Time(cd.createdAt)
	curDO.LastTouched = kplr.ISO8601Time(cd.lastTouched)
	curDO.LastKQL = cd.lastKQL
	curDO.Position = curPosToCurPosDO(cd.cur.GetPosition())
	return &curDO
}

func (sq *QueryRequest) String() string {
	return fmt.Sprint("{query=", sq.Query, ", params=", sq.Params, "}")
}

func (sq *QueryRequest) applyParams() string {
	qry := kplr.GetStringVal(sq.Query, "")
	if sq.Params == nil || len(*sq.Params) == 0 {
		return qry
	}

	params := make([]string, 0, len(*sq.Params)*2)
	for p, v := range *sq.Params {
		params = append(params, ":"+p, v)
	}

	r := strings.NewReplacer(params...)
	return r.Replace(qry)
}

// getKQL returns KQL corresponded to the QueryRequest
func (qr *QueryRequest) getKQL() string {
	//TODO: add params
	return qr.applyParams()
}
