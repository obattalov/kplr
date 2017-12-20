package storage

import (
	"reflect"
	"sort"
	"testing"

	"github.com/kplr-io/kplr/model/query"
)

func TestGetSource(t *testing.T) {
	qry := compQuery("select where key1=abc limit 2", t)
	tbl := prepareTable()
	ss, err := tbl.GetSrcId(qry)
	if err != nil || !equalSS(ss, []string{"1", "2"}) {
		t.Fatal("expects 1 and 2, but ", ss, ", err=", err)
	}

	qry = compQuery("select where key1 like '*1' and key2 like *f limit 2", t)
	ss, err = tbl.GetSrcId(qry)
	if err != nil || !equalSS(ss, []string{"3"}) {
		t.Fatal("expects 1 and 2, but ", ss, ", err=", err)
	}

	qry = compQuery("select where key1='abc1' or key2=def1 limit 2", t)
	ss, err = tbl.GetSrcId(qry)
	if err != nil || !equalSS(ss, []string{"3", "4"}) {
		t.Fatal("expects 1 and 2, but ", ss, ", err=", err)
	}
}

func equalSS(s1, s2 []string) bool {
	sort.Sort(sort.StringSlice(s1))
	sort.Sort(sort.StringSlice(s2))
	return reflect.DeepEqual(s1, s2)
}

func compQuery(q string, t *testing.T) *query.Query {
	qry, err := query.NewQuery(q)
	if err != nil {
		t.Fatal("could not compile query err=", err)
	}
	return qry
}

func prepareTable() Table {
	t := NewTable()
	ko := []string{"__source_id__", "key1", "key2"}
	m := map[string]string{"key1": "abc", "key2": "def", "__source_id__": "1"}
	t.Upsert(ko, m)
	m = map[string]string{"key1": "abc", "key2": "def", "__source_id__": "2"}
	t.Upsert(ko, m)
	m = map[string]string{"key1": "abc1", "key2": "def", "__source_id__": "3"}
	t.Upsert(ko, m)
	m = map[string]string{"key1": "abc1", "key2": "def1", "__source_id__": "4"}
	t.Upsert(ko, m)
	return t
}
