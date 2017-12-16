package model

import (
	"testing"
)

func BenchmarkMarshalLogEvent(b *testing.B) {
	var le LogEvent
	le.Reset(123456, tstStr, tstTags)
	var store [200]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		le.Marshal(store[:])
	}
}

func BenchmarkUnmarshalLogEventFast(b *testing.B) {
	var le LogEvent
	le.Reset(123456, tstStr, tstTags)
	var store [2000]byte
	le.Marshal(store[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		le.Unmarshal(store[:])
	}
}

func BenchmarkUnmarshalLogEventSlow(b *testing.B) {
	var le LogEvent
	le.Reset(123456, tstStr, tstTags)
	var store [2000]byte
	le.Marshal(store[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		le.UnmarshalCopy(store[:])
	}
}

func TestBufSize(t *testing.T) {
	var le LogEvent
	le.Reset(123412341234123, "ha ha ha", tstTags)
	bf := make([]byte, le.BufSize())
	n, err := le.Marshal(bf)
	if n != len(bf) || err != nil {
		t.Fatal("Expecting n=", n, " == ", len(bf), ", err=", err)
	}
}

func TestCasts(t *testing.T) {
	s := "Hello WOrld"
	bf := StringToByteArray(s)
	s1 := ByteArrayToString(bf)
	if s != s1 {
		t.Fatal("Oops, expecting s1=", s, ", but really s1=", s1)
	}

	bf = StringToByteArray("")
	s1 = ByteArrayToString(bf)
	if s1 != "" {
		t.Fatal("Oops, expecting empty string, but got s1=", s1)
	}
}
