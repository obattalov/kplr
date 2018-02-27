package kql

import (
	"testing"

	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/index"
	"github.com/kplr-io/kplr/model/index/tidx"
)

func prepareIndexer(t *testing.T) (index.TagsIndexer, []int64) {
	ids := []int64{}
	idxr := tidx.NewInMemTagIndex()
	tgs, err := idxr.UpsertTags(model.TagLine("label1=aaa|label2=bbb"))
	if err != nil {
		t.Fatal("expecting no err, but err=", err)
	}
	rb := &index.RecordsBatch{tgs.GetId(), model.JournalRecord{"j1", journal.RecordId{}}}
	idxr.OnRecords(rb)
	ids = append(ids, tgs.GetId())

	tgs, err = idxr.UpsertTags(model.TagLine("label1=bbb|label2=ccc"))
	if err != nil {
		t.Fatal("expecting no err, but err=", err)
	}
	rb = &index.RecordsBatch{tgs.GetId(), model.JournalRecord{"j1", journal.RecordId{}}}
	ids = append(ids, tgs.GetId())
	idxr.OnRecords(rb)
	rb = &index.RecordsBatch{tgs.GetId(), model.JournalRecord{"j2", journal.RecordId{}}}
	idxr.OnRecords(rb)
	return idxr, ids
}

func checkJournals(exp, act []string, t *testing.T) {
	if len(act) != len(exp) {
		t.Fatal("Expected journals ", exp, len(exp), ", but actuals are ", act, len(act))
	}
	m := map[string]bool{}
	for _, j := range exp {
		m[j] = true
	}

	for _, j := range act {
		if _, ok := m[j]; !ok {
			t.Fatal("Journal ", j, " is not expected")
		}
	}
}

func BenchmarkQueryFilterRun(b *testing.B) {
	idxr, ids := prepareIndexer(nil)
	q, _ := Compile("select where ts < 10 and label1=aaa limit 10", idxr)

	var le model.LogEvent
	le.Init(9, model.WeakString("aaa"))
	le.SetTGroupId(ids[0])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Filter(&le)
	}
}

func TestQueryNotIgnore(t *testing.T) {
	idxr, ids := prepareIndexer(t)
	q, err := Compile("select where ts < 10 and label1=aaa limit 10", idxr)
	if err != nil {
		t.Fatal("Query should be compiled ok, but err=", err)
	}

	checkJournals(q.jrnls, []string{"j1"}, t)
	if q.tgChkFunc.typ == cFTIgnore {
		t.Fatal("expecting not ignore func")
	}
	if len(q.tgCache) != 2 {
		t.Fatal("expecting 2 tags in cache, but found ", len(q.tgCache))
	}
	var le model.LogEvent
	le.Init(9, model.WeakString("aaa"))
	le.SetTGroupId(ids[0])
	if q.Filter(&le) {
		t.Fatal("The record should not be filtered ", le)
	}

	le.SetTGroupId(ids[1])
	if !q.Filter(&le) {
		t.Fatal("The record should be filtered ", le)
	}

	le.Init(19, model.WeakString("aaa"))
	le.SetTGroupId(ids[0])
	if !q.Filter(&le) {
		t.Fatal("The record should be filtered ", le)
	}
}

func TestQueryIgnore(t *testing.T) {
	idxr, ids := prepareIndexer(t)
	q, err := Compile("select where ts < 10 or label1=aaa limit 10", idxr)
	if err != nil {
		t.Fatal("Query should be compiled ok, but err=", err)
	}

	checkJournals(q.jrnls, []string{"j1", "j2"}, t)
	if q.tgChkFunc.typ != cFTIgnore {
		t.Fatal("expecting ignore func")
	}
	if len(q.tgCache) != 0 {
		t.Fatal("expecting 0 tags in cache, but found ", len(q.tgCache))
	}
	var le model.LogEvent
	le.Init(9, model.WeakString("aaa"))
	le.SetTGroupId(ids[0])
	if q.Filter(&le) {
		t.Fatal("The record should not be filtered ", le)
	}
	if len(q.tgCache) != 1 {
		t.Fatal("expecting 1 tag in cache, but found ", len(q.tgCache))
	}

	le.SetTGroupId(ids[1])
	if q.Filter(&le) {
		t.Fatal("The record should not be filtered ", le)
	}
	if len(q.tgCache) != 2 {
		t.Fatal("expecting 2 tags in cache, but found ", len(q.tgCache))
	}

	le.Init(19, model.WeakString("aaa"))
	le.SetTGroupId(ids[1])
	if !q.Filter(&le) {
		t.Fatal("The record should be filtered ", le)
	}
}
