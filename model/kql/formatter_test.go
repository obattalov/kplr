package kql

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/kplr-io/container/btsbuf"
)

func testEcho(varName string, escape bool) string {
	if escape {
		return fmt.Sprintf("\"%s\"", varName)
	}
	return varName
}

func testPositive(t *testing.T, fm, exp string) *Formatter {
	f, err := NewFormatter(fm, testEcho)
	if err != nil {
		t.Fatal("Unexpected err=", err)
	}

	var c btsbuf.Concatenator
	f.Format(&c)

	res := string(c.Buf())
	if res != exp {
		t.Fatal("res=", res, ", but expected=", exp)
	}

	return f
}

func testNegative(t *testing.T, fm string) {
	_, err := NewFormatter(fm, testEcho)
	if err == nil {
		t.Fatal("Expecting non-nil err, for fm=", fm, ", but got nil error")
	}
}

func TestFormatterOk(t *testing.T) {
	testPositive(t, "", "")
	testPositive(t, "a", "a")
	testPositive(t, "{}", "")
	testPositive(t, "{}{{}}", "")
	testPositive(t, "{}a{{}}a", "aa")
	testPositive(t, "\\}", "}")
	testPositive(t, "\\{", "{")
	testPositive(t, "\\\\", "\\")
	testPositive(t, "test={v}", "test=v")
	testPositive(t, "test={{v}}", "test=\"v\"")
	testPositive(t, "{a}{b}", "ab")
	testPositive(t, "{a}{{b}}", "a\"b\"")
	testPositive(t, "abc_{}{}_def", "abc__def")

	f := testPositive(t, "a", "a")
	if len(f.ftkns) != 1 || !reflect.DeepEqual(*f.ftkns[0], fmtToken{tt: cFmtTknConst, bv: []byte("a")}) {
		t.Fatal("wrong ftns[0]=", *f.ftkns[0])
	}

	f = testPositive(t, "a{a}", "aa")
	if len(f.ftkns) != 2 || !reflect.DeepEqual(*f.ftkns[1], fmtToken{tt: cFmtTknVar, varName: "a"}) {
		t.Fatal("wrong ftns[1]=", *f.ftkns[1])
	}

	f = testPositive(t, "a{{a}}", "a\"a\"")
	if len(f.ftkns) != 2 || !reflect.DeepEqual(*f.ftkns[1], fmtToken{tt: cFmtTknVar, varName: "a", escaping: true}) {
		t.Fatal("wrong ftns[1]=", *f.ftkns[1])
	}
}

func TestFormatterNotOk(t *testing.T) {
	testNegative(t, "{asdf")
	testNegative(t, "}asdf")
	testNegative(t, "{{}")
	testNegative(t, "{asdf}}")
	testNegative(t, "{{asd}d}")
	testNegative(t, "{d{asd}}")
	testNegative(t, "{d{asd}d}")
}
