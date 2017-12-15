package storage

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/query"
)

type (
	Table interface {
		Upsert(tags string) error
		GetSrcId(qry *query.Query) []string
	}

	table struct {
		lock     sync.Mutex
		cur_id   int
		v2iStore atomic.Value

		vals2id *treemap.Map
		// every id contains tags string
		id2vals *treemap.Map
	}
)

func NewTable() Table {
	t := new(table)
	return t
}

func (t *table) Upsert(tags string) error {
	m := t.v2iStore.Load().(map[string]int)
	if _, ok := m[tags]; ok {
		return nil
	}

	if model.GetTag(tags, model.TAG_SRC_ID) == "" {
		return errors.New("tags must contain " + model.TAG_SRC_ID + " tag")
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	t.cur_id++
	t.vals2id.Put(tags, t.cur_id)
	t.id2vals.Put(t.cur_id, tags)

	m = make(map[string]int, t.vals2id.Size())
	t.vals2id.Each(func(key interface{}, value interface{}) {
		m[key.(string)] = value.(int)
	})
	t.v2iStore.Store(m)
	return nil
}

func (t *table) GetSrcId(qry *query.Query) []string {
	// TODO right now returns whateever we have in the query
	return qry.GetSources()
}
