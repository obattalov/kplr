package model

import (
	"encoding/binary"
	"testing"
)

func marshalStringByCast(v string, buf []byte) (int, error) {
	bl := len(buf)
	ln := len(v)
	if ln+4 > bl {
		return 0, noBufErr("MarshalString-size-body", bl, ln+4)
	}
	binary.BigEndian.PutUint32(buf, uint32(ln))
	copy(buf[4:ln+4], []byte(v))
	return ln + 4, nil
}

var tstStr = "This is some string for test marshalling speed Yahhoooo 11111111111111111111111111111111111111111111111111"
var tstTags = Tags("pod=1234134kjhakfdjhlakjdsfhkjahdlf,key=abc,")

func BenchmarkMarshalStringByCast(b *testing.B) {
	var buf [200]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		marshalStringByCast(tstStr, buf[:])
	}
}

func BenchmarkMarshalString(b *testing.B) {
	var buf [200]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MarshalString(tstStr, buf[:])
	}
}

func BenchmarkUnmarshalStringByCast(b *testing.B) {
	var buf [200]byte
	marshalStringByCast(tstStr, buf[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnmarshalStringCopy(buf[:])
	}
}

func BenchmarkUnmarshalString(b *testing.B) {
	var buf [200]byte
	marshalStringByCast(tstStr, buf[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnmarshalString(buf[:])
	}
}

func TestMarshalString(t *testing.T) {
	str := "hello str"
	buf := make([]byte, len(str)+4)
	n, err := MarshalString(str, buf)
	if err != nil {
		t.Fatal("Should be enough space, but err=", err)
	}
	if n != len(str)+4 {
		t.Fatal("expected string marshal size is ", len(str)+4, ", but actual is ", n)
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	str := "hello str"
	buf := make([]byte, len(str)+4)
	MarshalString(str, buf)
	_, str2, _ := UnmarshalString(buf)
	if str2 != str {
		t.Fatal("Wrong unmarshaling str=", str, ", str2=", str2)
	}

	buf[4] = buf[5]
	if str2 == str {
		t.Fatal("They must be different now: str=", str, ", str2=", str2)
	}
}

func TestMarshalUnmarshalSSliceEmpty(t *testing.T) {
	ss := []string{}
	buf := make([]byte, 100)
	n, err := MarshalSSlice(SSlice(ss), buf)
	if n != 2 || err != nil {
		t.Fatal("Must be able to write 2 bytes for the SSlice length n=", n, ", err=", err)
	}

	ss, n, err = UnmarshalSSlice(ss, buf)
	if n != 2 || err != nil || len(ss) != 0 {
		t.Fatal("Must be able to read 2 bytes for the SSlice length n=", n, ", err=", err)
	}
}

func TestMarshalUnmarshalSSlice(t *testing.T) {
	ss := []string{"aaa", "bbb"}

	if SSlice(ss).Size() != 16 {
		t.Fatal("Expecting marshaled size 16, but really ", SSlice(ss).Size())
	}

	buf := make([]byte, 100)
	n, err := MarshalSSlice(SSlice(ss), buf)
	if n != 16 || err != nil {
		t.Fatal("Must be able to write 16 bytes for the SSlice length n=", n, ", err=", err)
	}

	s := []string{"", ""}
	ss, n, err = UnmarshalSSlice(SSlice(s[:1]), buf)
	if n != 16 || err != nil || len(ss) != 2 || ss[0] != "aaa" || ss[1] != "bbb" {
		t.Fatal("Must be able to read 2 bytes for the SSlice length n=", n, ", err=", err, ", ss=", ss)
	}

	s = []string{""}
	ss, n, err = UnmarshalSSlice(SSlice(s), buf)
	if err == nil {
		t.Fatal("Must report error - slice not big enough!")
	}
}

func TestCasts(t *testing.T) {
	b := make([]byte, 10)
	s := ByteArrayToString(b)
	b[0] = 'a'
	if len(s) != 10 || s[0] != 'a' {
		t.Fatal("must be pointed to same object s=", s, " b=", b)
	}

	s1 := CopyString(s)
	b[0] = 'b'
	if s1[0] != 'a' || s[0] != 'b' {
		t.Fatal("must be different objects s=", s, " b=", b, ", s1=", s1)
	}

	s = "Hello WOrld"
	bf := StringToByteArray(s)
	s1 = ByteArrayToString(bf)
	if s != s1 {
		t.Fatal("Oops, expecting s1=", s, ", but really s1=", s1)
	}

	bf = StringToByteArray("")
	s1 = ByteArrayToString(bf)
	if s1 != "" {
		t.Fatal("Oops, expecting empty string, but got s1=", s1)
	}
}
