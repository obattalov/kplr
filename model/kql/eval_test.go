package kql

import (
	"testing"

	"github.com/kplr-io/kplr/model"
)

type (
	testExpValuator struct {
		t    *testing.T
		ts   uint64
		ms   model.WeakString
		tags map[string]string
		flgs int
	}
)

func (tev *testExpValuator) timestamp() uint64 {
	if tev.flgs&cExpValTsIgnore != 0 {
		tev.t.Fatal("tsFunc must not be called, but it is.")
	}
	return tev.ts
}

func (tev *testExpValuator) msg() model.WeakString {
	if tev.flgs&cExpValMsgIgnore != 0 {
		tev.t.Fatal("msgFunc must not be called, but it is.")
	}

	return tev.ms
}

func (tev *testExpValuator) tagVal(tag string) string {
	if tev.flgs&cExpValTagsIgnore != 0 {
		tev.t.Fatal("tagsFunc must not be called, but it is.")
	}
	return tev.tags[tag]
}

func getTagsOnlyValuator(t *testing.T, tags map[string]string) *testExpValuator {
	return &testExpValuator{t, 0, model.WeakString(""), tags, cExpValMsgIgnore | cExpValTsIgnore}
}

func getTsOnlyValuator(t *testing.T) *testExpValuator {
	return &testExpValuator{t, 0, model.WeakString(""), nil, cExpValMsgIgnore | cExpValTagsIgnore}
}

func getNoTagsValuator(t *testing.T) *testExpValuator {
	return &testExpValuator{t, 0, model.WeakString(""), nil, cExpValTagsIgnore}
}

func getExpFuncDesc(t *testing.T, kQuery string, ev expValuator, flags int) *expFuncDesc {
	s, err := Parse(kQuery)
	if err != nil {
		t.Fatal("The expression '", kQuery, "' must be compiled, but err=", err)
	}

	res, err := evaluate(s.Where, ev, flags)
	if err != nil {
		t.Fatal("the expression '", kQuery, "' must be evaluated no problem, but err=", err)
	}
	return res
}

func BenchmarkSimpleTagCheck(b *testing.B) {
	tev := getTagsOnlyValuator(nil, map[string]string{"aaa": "bbb", "bbb": "ddd"})
	fnd := getExpFuncDesc(nil, "select where somebug='abc' or another=123 or (ts < 3 and aaa='bbb' and bbb='ddd') limit 1", tev, tev.flgs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fnd.fn()
	}
}

func BenchmarkSimpleTsOnlyCheck(b *testing.B) {
	tev := getTsOnlyValuator(nil)
	tev.ms = model.WeakString("This is some text which contains some text")
	fnd := getExpFuncDesc(nil, "select where (ts < 3 and aaa='bbb' and bbb='ddd' and msg contains 'ntains') limit 1", tev, tev.flgs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fnd.fn()
	}
}

func BenchmarkSimpleTsAndMsgOnlyCheck(b *testing.B) {
	tev := getNoTagsValuator(nil)
	fnd := getExpFuncDesc(nil, "select where (ts < 3 and aaa='bbb' and bbb='ddd') limit 1", tev, tev.flgs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fnd.fn()
	}
}

func TestEvalTagsEqual(t *testing.T) {
	tev := getTagsOnlyValuator(t, map[string]string{"aaa": "bbb"})
	fnd := getExpFuncDesc(t, "select where ts < 3 and aaa='bbb' limit 1", tev, tev.flgs)
	if !fnd.fn() {
		t.Fatal("Must be true")
	}

	tev.tags["aaa"] = "ccc"
	if fnd.fn() {
		t.Fatal("Must be false")
	}

	fnd = getExpFuncDesc(t, "select where ts < 3 or aaa='bbb' limit 1", tev, tev.flgs)
	if !fnd.fn() {
		t.Fatal("Must be true")
	}
}

func TestEvalTsLess(t *testing.T) {
	tev := getTsOnlyValuator(t)
	fnd := getExpFuncDesc(t, "select where ts < 3 and aaa='bbb' limit 1", tev, tev.flgs)
	if !fnd.fn() {
		t.Fatal("Must be true")
	}

	tev.ts = 10
	if fnd.fn() {
		t.Fatal("Must be false")
	}
}

func TestEvalTsAndMsgContains(t *testing.T) {
	tev := getNoTagsValuator(t)
	tev.ms = model.WeakString("This is some text which contains some text")
	fnd := getExpFuncDesc(t, "select where (ts < 3 and aaa='bbb' and bbb='ddd' and msg contains 'ntains') limit 1", tev, tev.flgs)
	if !fnd.fn() {
		t.Fatal("Must be true")
	}

	tev.ms = model.WeakString("This is some text which is some text")
	if fnd.fn() {
		t.Fatal("Must be false")
	}
}

func TestAllValues(t *testing.T) {
	tev := &testExpValuator{t, 0, model.WeakString(""), map[string]string{"aaa": "bbb"}, 0}
	tev.ms = model.WeakString("This is some text which contains some text")
	fnd := getExpFuncDesc(t, "select where (ts < 3 and aaa='bbb' and msg contains 'ntains') limit 1", tev, 0)
	if !fnd.fn() {
		t.Fatal("First attempt must be true")
	}

	tev.ts = 3
	if fnd.fn() {
		t.Fatal("Second attempt must be False")
	}
	tev.ts = 0

	tev.ms = model.WeakString("This ext")
	if fnd.fn() {
		t.Fatal("Third attempt must be False")
	}
	tev.ms = model.WeakString("This is some text which contains some text")

	tev.tags["aaa"] = "ccc"
	if fnd.fn() {
		t.Fatal("Forth attempt must be False")
	}
	tev.tags["aaa"] = "bbb"

	if !fnd.fn() {
		t.Fatal("Fivth attempt must be true")
	}

}
