package model

import (
	"testing"
)

func BenchmarkFromTags(b *testing.B) {
	m := map[WeakString]WeakString{"key1": "adadasdfadfc", "key0": "zzaadfadsfadfz", "ddd": "aakfhjaldjhfal", "abc": "adfds"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MapToTags(SSlice{"key0", "key1", "ddd", "abc"}, m)
	}
}

func TestFormTags(t *testing.T) {
	m := map[WeakString]WeakString{}
	if MapToTags(SSlice{}, m) != "" {
		t.Fatal("should be empty, but ", MapToTags(SSlice{}, m))
	}

	m = map[WeakString]WeakString{"key1": "adc", "key0": "zzz"}
	str := MapToTags(SSlice{"key0", "key1"}, m)
	if str != "|key0=zzz|key1=adc|" && str != "|key1=adc|key0=zzz|" {
		t.Fatal("unexpected str=", str)
	}
}

func TestContainsAll(t *testing.T) {
	t1 := Tags("|k=abc|k2=def|k3=aaa|")
	t2 := Tags("|k3=aaa|")
	if !t1.ContainsAll(t2) || t2.ContainsAll(t1) {
		t.Fatal("must contain")
	}

	t2 = Tags("|k3=aa|")
	if t1.ContainsAll(t2) {
		t.Fatal("must NOT contain")
	}

	t2 = Tags("|k3=aaa|k=abc|")
	if !t1.ContainsAll(t2) {
		t.Fatal("must contain")
	}

	t2 = Tags("")
	if !t1.ContainsAll(t2) {
		t.Fatal("must contain")
	}

	t2 = Tags("|k3=aaa|k=abcc|")
	if t1.ContainsAll(t2) {
		t.Fatal("must not contain")
	}

	if !t2.ContainsAll(t2) {
		t.Fatal("must not contain itself")
	}
}

func TestTagSubst(t *testing.T) {
	if TagSubst("Key", "VaLue") != "|key=VaLue|" {
		t.Fatal("not expected ", TagSubst("Key", "VaLue"))
	}
}

func TestTagsAdd(t *testing.T) {
	var tg Tags
	tg2 := tg.Add("A", "B")
	if string(tg2) != "|a=B|" {
		t.Fatal("wrong value tg2=", tg2)
	}

	tg = tg2.Add("c", "d")
	if string(tg) != "|a=B|c=d|" {
		t.Fatal("wrong value tg=", tg)
	}
}

func TestGetTagNames(t *testing.T) {
	t1 := Tags("|k=abc|k2=def|k3=aaa|")
	tn := t1.GetTagNames()
	if len(tn) != 3 || tn[0] != "k" || tn[1] != "k2" || tn[2] != "k3" {
		t.Fatal("wrong data ", t1)
	}

	t1 = Tags("")
	tn = t1.GetTagNames()
	if len(tn) != 0 {
		t.Fatal("expecting empty list")
	}
}
