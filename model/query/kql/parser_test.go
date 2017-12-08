package kql

import (
	"testing"
)

func TestParse(t *testing.T) {
	testOk(t, "select limit 120")
	testOk(t, "select tail limit 100")
	testOk(t, "select tail limit 100 offset 123")
	testOk(t, "select tail 'format-%ts-%pod' limit 100")
	testOk(t, "select tail from c1 limit 100")
	testOk(t, "select tail from c1,123Kd limit 100")
	testOk(t, "select tail from c1,123Kd WHERE a='1234' AND bbb>=adfadf234798* limit 100")
	testOk(t, "select WHERE a='1234' AND bbb>=adfadf234798* or xxx = yyy limit 100")
	testOk(t, "select WHERE a='1234' AND bbb like 'adfadf234798*' or xxx = yyy limit 10")
}

func testOk(t *testing.T, kql string) {
	s, err := Parse(kql)
	if err != nil {
		t.Fatal("kql=\"", kql, "\" unexpected err=", err)
	}
	t.Log(s.Limit)
}
