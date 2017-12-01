package model

import (
	"testing"
)

func BenchmarkMarshalLogEvent(b *testing.B) {
	var le LogEvent
	le.Reset(123456, tstStr)
	var store [200]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		le.Marshal(store[:])
	}
}

func BenchmarkUnmarshalLogEventFast(b *testing.B) {
	var le LogEvent
	le.Reset(123456, tstStr)
	var store [200]byte
	le.Marshal(store[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		le.Unmarshal(store[:])
	}
}

func BenchmarkUnmarshalLogEventSlow(b *testing.B) {
	var le LogEvent
	le.Reset(123456, tstStr)
	var store [200]byte
	le.Marshal(store[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		le.UnmarshalCopy(store[:])
	}
}
