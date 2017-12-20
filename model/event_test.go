package model

import (
	"reflect"
	"testing"
)

func BenchmarkMarshalEvent(b *testing.B) {
	meta := []FieldType{FTInt64, FTString, FTInt64}
	ev := Event([]interface{}{int64(1234898734), tstStr, int64(97495739)})
	var store [200]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MarshalEvent(meta, ev, store[:])
	}
}

func BenchmarkUnmarshalEventFast(b *testing.B) {
	meta := []FieldType{FTInt64, FTString, FTInt64}
	ev := Event([]interface{}{int64(1234898734), tstStr, int64(97495739)})
	var store [200]byte
	MarshalEvent(meta, ev, store[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnmarshalEvent(meta, store[:], ev)
	}
}

func BenchmarkUnmarshalEventByCopyStrings(b *testing.B) {
	meta := []FieldType{FTInt64, FTString, FTInt64}
	ev := Event([]interface{}{int64(1234898734), tstStr, int64(97495739)})
	var store [200]byte
	MarshalEvent(meta, ev, store[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnmarshalEventCopy(meta, store[:], ev)
	}
}

func TestGeneral(t *testing.T) {
	meta := []FieldType{FTInt64, FTString, FTInt64}
	ev := Event([]interface{}{int64(1234898734), "Hello World", int64(97495739)})
	var store [100]byte
	n, err := MarshalEvent(meta, ev, store[:])
	if n < 20 || err != nil {
		t.Fatal("Something goes wrong, should be marshal ok err=", err)
	}
	if n != ev.Size(meta) {
		t.Fatal("Oops, the marshaled size ", n, " and desired Size()=", ev.Size(meta), " are different!")
	}
	ev2 := Event(make([]interface{}, len(meta)))
	n2, err := UnmarshalEvent(meta, store[:], ev2)
	if n2 != n || err != nil {
		t.Fatal("Something goes wrong, should be umarshal ok err=", err, ", n=", n, ", n2=", n2)
	}
	if !reflect.DeepEqual(ev, ev2) {
		t.Fatal("ev=", ev, ", must be equal to ev2=", ev2)
	}

	ev[0] = nil
	ev[1] = nil
	n3, err := MarshalEvent(meta, ev, store[:])
	if n3 >= n || err != nil {
		t.Fatal("Something goes wrong, should be marshal ok err=", err, ", n3=", n3, ", n=", n)
	}
	if n3 != ev.Size(meta) {
		t.Fatal("Oops, the marshaled size ", n3, " and desired Size()=", ev.Size(meta), " are different!")
	}
	n2, err = UnmarshalEvent(meta, store[:], ev2)
	if n3 != n2 || err != nil {
		t.Fatal("Something goes wrong, should be umarshal ok err=", err, ", n=", n, ", n2=", n2)
	}
	if !reflect.DeepEqual(ev, ev2) {
		t.Fatal("ev=", ev, ", must be equal to ev2=", ev2)
	}
}
