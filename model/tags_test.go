package model

import (
	"testing"
)

func BenchmarkFromTags(b *testing.B) {
	m := map[string]string{"key1": "adadasdfadfc", "key0": "zzaadfadsfadfz", "ddd": "aakfhjaldjhfal", "abc": "adfds"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MapToTags([]string{"key0", "key1", "ddd", "abc"}, m)
	}
}

func TestFormTags(t *testing.T) {
	m := map[string]string{}
	if MapToTags([]string{}, m) != "" {
		t.Fatal("should be empty, but ", MapToTags([]string{}, m))
	}

	m = map[string]string{"key1": "adc", "key0": "zzz"}
	str := MapToTags([]string{"key0", "key1"}, m)
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
