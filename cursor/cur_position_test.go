package cursor

import (
	"reflect"
	"testing"

	"github.com/kplr-io/journal"
)

func TestEmptyCursorPosition(t *testing.T) {
	cp := make(CursorPosition)
	if cp.BufSize() != 0 {
		t.Fatal("BufSize must be 0")
	}

	buf := []byte{}
	n, err := MarshalCursorPosition(cp, buf)
	if n != 0 || err != nil {
		t.Fatal("n must be 0, and err == nil")
	}
}

func TestCursorPosition(t *testing.T) {
	cp := CursorPosition{"s1": journal.RecordId{1, 2}}
	sz := cp.BufSize()
	if sz != 18 {
		t.Fatal("BufSize must be 18")
	}

	buf := make([]byte, sz)
	n, err := MarshalCursorPosition(cp, buf)
	if n != sz || err != nil {
		t.Fatal("Expecting ", sz, " but n=", n, " or err != nil, but err=", err)
	}

	n, cp2, err := UnmarshalCursorPosition(buf)
	if n != sz || err != nil {
		t.Fatal("Expecting ", sz, " but n=", n, " or err != nil, but err=", err)
	}

	if !reflect.DeepEqual(cp, cp2) {
		t.Fatal("Expecting ", cp, ", but got ", cp2)
	}

	cp = CursorPosition{"s1": journal.RecordId{1, 2}, "sasdf1": journal.RecordId{123, 2454}}
	buf = make([]byte, cp.BufSize())
	MarshalCursorPosition(cp, buf)
	_, cp2, _ = UnmarshalCursorPosition(buf)
	if !reflect.DeepEqual(cp, cp2) {
		t.Fatal("Expecting ", cp, ", but got ", cp2)
	}
}
