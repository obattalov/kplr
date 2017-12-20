package query

import (
	"testing"
	"time"

	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model"
)

func TestSimple(t *testing.T) {
	f := getFunc("select WHERE ts=1234 limit 10", t)
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
	le1.Reset(10, "Hello test", model.MapToTags(map[string]string{"pod": "123", "key": ""}))

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
	le1.Reset(10, "Hello test", model.MapToTags(map[string]string{"pod": "a123f", "key": "afdf"}))

	f := getFunc("select where pod like \"a*f?f\" limit 10", t)
	if !f(&le1) {
		t.Fatal("Should be true, but ", f(&le1))
	}

	le1.Reset(10, "Hello test", model.MapToTags(map[string]string{"pod": "a12f3f", "key": "afdf"}))
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
	le.Reset(10, "Hello test", model.MapToTags(map[string]string{"pod": "a123f", "key": "afdf"}))

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

func TestNoSourceExp(t *testing.T) {
	var le model.LogEvent
	le.Reset(10, "Hello test", model.MapToTags(map[string]string{"pod": "a123f", "key": "afdf"}))
	exp := getExp("select where ts=1234 limit 1", t)
	if exp.Priority != 0 || !exp.Expr(&le) {
		t.Fatal("Should be comprehensive, because of ts check")
	}

	exp = getExp("select where src contains '123' limit 1", t)
	if exp.Priority != 0 || !exp.Expr(&le) {
		t.Fatal("Should be comprehensive, because of ts src")
	}

	exp = getExp("select where src contains '123' or ts=1234 AND not t=123 limit 1", t)
	if exp.Priority != 3 || !exp.Expr(&le) {
		t.Fatal("Should be comprehensive, because of ts src Priority=", exp.Priority)
	}
}

func TestSourceExp(t *testing.T) {
	var le model.LogEvent
	le.Reset(10, "Hello test", model.MapToTags(map[string]string{"pod": "a123f", "key": "afdf"}))
	exp := getExp("select where pod=1234 limit 1", t)
	if exp.Priority != 1 || exp.Expr(&le) {
		t.Fatal("Should be comprehensive, because of ts check")
	}
	exp = getExp("select where pod=a123f limit 1", t)
	if !exp.Expr(&le) {
		t.Fatal("Should be comprehensive, because of ts check")
	}

	exp = getExp("select where pod=1234 or ts=234 limit 1", t)
	if exp.Priority != 1 || exp.Expr(&le) {
		t.Fatal("Should ignore or with ts condition, but Priority=", exp.Priority)
	}

	exp = getExp("select where pod=a123f and ts=234 and src contains asd limit 1", t)
	if exp.Priority != 1 || !exp.Expr(&le) {
		t.Fatal("Should ignore or with ts condition, but Priority=", exp.Priority)
	}

	exp = getExp("select where pod=a123f and ts<234 and src contains asd limit 1", t)
	if exp.Priority != 1 || !exp.Expr(&le) {
		t.Fatal("Should ignore or with ts condition, but Priority=", exp.Priority)
	}

	exp = getExp("select where pod like a123f and ts<234 and src contains asd limit 1", t)
	if exp.Priority != 3 || !exp.Expr(&le) {
		t.Fatal("Should ignore or with ts condition, but Priority=", exp.Priority)
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

func getExp(q string, t *testing.T) ExprDesc {
	r, err := NewQuery(q)
	if err != nil {
		t.Fatal("kql=\"", q, "\" while parsing, got an unexpected err=", err)
		return ExprDesc{Priority: -1}
	}

	return r.srcExp
}
