package kql

import (
	"fmt"
	"testing"

	"github.com/kplr-io/container/btsbuf"
)

func testEcho(varName string, escape bool) string {
	if escape {
		return fmt.Sprintf("\"%s\"", varName)
	}
	return varName
}

func testPositive(t *testing.T, fm, exp string) {
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
