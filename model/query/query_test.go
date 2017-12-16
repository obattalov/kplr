package query

import (
	"testing"
	"time"

	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model"
)

func TestSimple(t *testing.T) {
	f := getFunc("select limit 10", t)
	if f(nil) {
		t.Fatal("Should accept everything")
	}

	var le model.LogEvent
	le.Reset(1234, "Hello test", "")
	if f(&le) {
		t.Fatal("Should accept everything and le")
	}
}

func TestTimestampCond(t *testing.T) {
	var le1, le2 model.LogEvent
	le1.Reset(10, "Hello test", "")
	le2.Reset(20, "Hello test", "")

	f := getFunc("select where ts<15 limit 10", t)
	if f(&le1) || !f(&le2) {
		t.Fatal("Should be false, true, but ", f(&le1), ", ", f(&le2))
	}

	f = getFunc("select where ts>15 limit 10", t)
	if !f(&le1) || f(&le2) {
		t.Fatal("Should be true, false, but ", f(&le1), ", ", f(&le2))
	}

	f = getFunc("select where ts=20 limit 10", t)
	if !f(&le1) || f(&le2) {
		t.Fatal("Should be true, false, but ", f(&le1), ", ", f(&le2))
	}

	f = getFunc("select where ts!=20 limit 10", t)
	if f(&le1) || !f(&le2) {
		t.Fatal("Should be false, true, but ", f(&le1), ", ", f(&le2))
	}

	tm := time.Now()
	tmf := tm.Format(time.RFC3339)
	le1.Reset(uint64(tm.Unix())-1, "", "")
	le2.Reset(uint64(tm.Unix()), "", "")
	kq := "select where ts < '" + tmf + "' limit 10"
	f = getFunc(kq, t)
	t.Log("kql=", kq)
	if f(&le1) || !f(&le2) {
		t.Fatal("Should be false, true, but ", f(&le1), ", ", f(&le2))
	}

	tmf = tmf[:len(tmf)-6]
	kq = "select where ts > '" + tmf + "' limit 10"
	f = getFunc(kq, t)
	t.Log("kql=", kq)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}
}

func TestSrcCond(t *testing.T) {
	var le1 model.LogEvent
	le1.Reset(10, "Hello test", "")

	f := getFunc("select where src contains 'ell' limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}

	f = getFunc("select where src PREFIX 'Hell' limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}

	f = getFunc("select where Src sufFIX 'est' limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}
}

func TestTagsConds(t *testing.T) {
	var le1 model.LogEvent
	le1.Reset(10, "Hello test", model.TagsToStr(map[string]string{"pod": "123", "key": ""}))

	f := getFunc("select where pod = \"123\" limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}

	f = getFunc("select where pod =123 limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}

	f = getFunc("select where key ='' limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}

	f = getFunc("select where key ='123' limit 10", t)
	if !f(&le1) {
		t.Fatal("Should be true, but ", f(&le1))
	}

	f = getFunc("select where key ='' and pod=123 limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}
}

func TestLike(t *testing.T) {
	var le1 model.LogEvent
	le1.Reset(10, "Hello test", model.TagsToStr(map[string]string{"pod": "a123f", "key": "afdf"}))

	f := getFunc("select where pod like \"a*f?f\" limit 10", t)
	if !f(&le1) {
		t.Fatal("Should be true, but ", f(&le1))
	}

	le1.Reset(10, "Hello test", model.TagsToStr(map[string]string{"pod": "a12f3f", "key": "afdf"}))
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}

	f = getFunc("select where pod like * limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}

	f = getFunc("select where pod like '????3?' limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}

	f = getFunc("select where pod like *3f  or key like adf limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}

	f = getFunc("select where pod like *3f and key like afdf limit 10", t)
	if f(&le1) {
		t.Fatal("Should be false, but ", f(&le1))
	}
}

func TestNot(t *testing.T) {
	var le model.LogEvent
	le.Reset(10, "Hello test", model.TagsToStr(map[string]string{"pod": "a123f", "key": "afdf"}))

	f := getFunc("select where not (pod like \"a*f?f\") limit 10", t)
	if f(&le) {
		t.Fatal("Should be false, but ", f(&le))
	}

	f = getFunc("select where pod=a123f and key=afdf limit 10", t)
	if f(&le) {
		t.Fatal("Should be false, but ", f(&le))
	}

	f = getFunc("select where not pod=a3f and key=afdf limit 10", t)
	if f(&le) {
		t.Fatal("Should be false, but ", f(&le))
	}

	f = getFunc("select where not (pod=a3f and key=afdf) limit 10", t)
	if f(&le) {
		t.Fatal("Should be false, but ", f(&le))
	}

	f = getFunc("select where not (pod=a123f and key=afdf) limit 10", t)
	if !f(&le) {
		t.Fatal("Should be true, but ", f(&le))
	}
}

func getFunc(q string, t *testing.T) journal.FilterF {
	r, err := NewQuery(q)
	if err != nil {
		t.Fatal("kql=\"", q, "\" while parsing, got an unexpected err=", err)
		return nil
	}

	return r.GetFilterF()
}
