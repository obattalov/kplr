package kql

import (
	"reflect"
	"testing"

	"github.com/kplr-io/kplr"
)

func TestParse(t *testing.T) {
	testOk(t, "select limit 120")
	testOk(t, "select limit 100")
	testOk(t, "select offset 123 limit 100")
	testOk(t, "select 'text' limit 100")
	testOk(t, "select 'json' position tail limit 100")
	testOk(t, "select position head limit 100")
	testOk(t, "select position asdf limit 100")
	testOk(t, "select position 'hasdf123' limit 100")
	testOk(t, "select WHERE NOT a='1234' limit 100")
	testOk(t, "select WHERE NOT (a='1234' AND c=abc) limit 100")
	testOk(t, "select WHERE NOT a='1234' AND c=abc limit 100")
	testOk(t, "select WHERE NOT a='1234' AND not c=abc limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or x=123 limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or not x=123 limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or not (x=123) limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or not (x=123 or c = abc) limit 100")
	testOk(t, "select WHERE a='1234' AND bbb>=adfadf234798* or xxx = yyy limit 100")
	testOk(t, "select WHERE a='1234' AND bbb like 'adfadf234798*' or xxx = yyy limit 10")
	testOk(t, "SELECT FROM a, b, *c WHERE filename=\"system.log\" or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
	testOk(t, "SELECT FROM a, b, *c WHERE filename=\"system.log\" or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
	testOk(t, "SELECT FROM a, b, *c WHERE 'from'='this is tag value' or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
	testOk(t, "SELECT FROM a, b, *c WHERE filename=\"system.log\" or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
}

func TestParams(t *testing.T) {
	s := testOk(t, "Select 'abc' where a = '123' position tail offset -10 limit 13")
	if !reflect.DeepEqual(s.Format, kplr.GetStringPtr("abc")) || s.Position.PosId != "tail" || *s.Offset != -10 || s.Limit != 13 {
		t.Fatal("Something goes wrong ", s)
	}
}

func TestPosition(t *testing.T) {
	s := testOk(t, "Select 'abc' where a = '123' position 'tail' offset -10 limit 13")
	if s.Position.PosId != "tail" {
		t.Fatal("Something goes wrong ", s)
	}

	s = testOk(t, "Select 'abc' where a = '123' position tail offset -10 limit 13")
	if s.Position.PosId != "tail" {
		t.Fatal("Something goes wrong ", s)
	}

	posId := "AAAABXNyY0lkAAAE0gAAAAAAAeIqAAAAGHNyYzEyMzQ3OUAkJV8gQTIzNEF6cUlkMgAAAA4AAAAAAAAE0g=="
	s = testOk(t, "Select 'abc' where a = '123' position '"+posId+"' offset -10 limit 13")
	if s.Position.PosId != posId {
		t.Fatal("Something goes wrong ", s)
	}
}

func TestFormat(t *testing.T) {
	testFormat(t, "select limit 10", nil)
	testFormat(t, "select 'text\"' limit 10", kplr.GetStringPtr("text\""))
	testFormat(t, "select 't\\'ext' limit 10", kplr.GetStringPtr("t'ext"))
	testFormat(t, "select \"t\\\"ext\" limit 10", kplr.GetStringPtr("t\"ext"))
	testFormat(t, "select \"{\\\"msg\\\": \\\"%{msg}%\\\"}\" limit 10", kplr.GetStringPtr("{\"msg\": \"%{msg}%\"}"))
}

func testFormat(t *testing.T, kql string, val *string) {
	s, err := Parse(kql)
	if err != nil {
		t.Fatal("kql=\"", kql, "\" unexpected err=", err)
	}

	if !reflect.DeepEqual(s.Format, val) {
		t.Fatal("Seems like they are different ", s.Format, " ", val)
	}
}

func TestUnquote(t *testing.T) {
	testUnquote(t, "'aaa", "'aaa")
	testUnquote(t, "'aaa\"", "'aaa\"")
	testUnquote(t, "aaa'", "aaa'")
	testUnquote(t, "\"aaa'", "\"aaa'")
	testUnquote(t, "   'aaa'", "aaa")
	testUnquote(t, "\"aaa\"   ", "aaa")
	testUnquote(t, "   'aaa\"", "   'aaa\"")
	testUnquote(t, " aaa   ", " aaa   ")
}

func testUnquote(t *testing.T, in, out string) {
	if unquote(in) != out {
		t.Fatal("unqoute(", in, ") != ", out)
	}
}

func TestParseWhere(t *testing.T) {
	testWhereOk(t, "a=adsf and b=adsf")
}

func testWhereOk(t *testing.T, whr string) *Expression {
	e, err := ParseExpr(whr)
	if err != nil {
		t.Fatal("whr=\"", whr, "\" unexpected err=", err)
	}
	return e
}

func testOk(t *testing.T, kql string) *Select {
	s, err := Parse(kql)
	if err != nil {
		t.Fatal("kql=\"", kql, "\" unexpected err=", err)
	}
	t.Log(s.Limit)
	return s
}
