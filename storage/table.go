package storage

import (
	"errors"
	"sync"

	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/query"
)

type (
	Table interface {
		// Upserts tags. tags must contain source Id, the tags and kOrder
		// can contain weak strings. TODO: introduce it!
		Upsert(kOrder []string, tags map[string]string) error
		GetSrcId(qry *query.Query) ([]string, error)
	}

	table struct {
		lock sync.Mutex
		tags map[model.Tags]string
	}
)

func NewTable() Table {
	t := new(table)
	t.tags = make(map[model.Tags]string)
	return t
}

func (t *table) Upsert(kOrder []string, m map[string]string) error {
	srcId, ok := m[model.TAG_SRC_ID]
	if !ok {
		return errors.New("expected " + model.TAG_SRC_ID + " tag")
	}
	tags := model.MapToTags(kOrder, m)

	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.tags[tags]; ok {
		return nil
	}

	t.tags[tags] = srcId

	return nil
}

func (t *table) GetSrcId(qry *query.Query) ([]string, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	ed := qry.GetSrcExpr()
	if ed.Priority == 0 {
		return nil, errors.New("The query condition doesn't contains tags, cannot identify sources")
	}
	var le model.LogEvent
	m := make(map[string]string)
	for tags, srcId := range t.tags {
		if _, ok := m[srcId]; ok {
			continue
		}
		le.Reset(0, "", tags)
		if ed.Expr(&le) {
			m[srcId] = srcId
		}
	}

	res := make([]string, 0, len(m))
	for _, sid := range m {
		res = append(res, sid)
	}

	return res, nil
}
