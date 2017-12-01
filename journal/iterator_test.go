package journal

import (
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/kplr/model"
)

type jreader struct {
	idx int
	ss  []string
}

func (jr *jreader) reset(idx int, ss []string) {
	jr.idx = idx
	jr.ss = ss
}

func (jr *jreader) ReadForward(bbw *btsbuf.Writer) (int, error) {
	n := 0
	var le model.LogEvent
	for jr.idx < len(jr.ss) {
		le.Reset(uint64(jr.idx), jr.ss[jr.idx])
		bb, err := bbw.Allocate(le.BufSize())
		if err != nil {
			if n == 0 {
				return 0, err
			}
			return n, nil
		}
		le.Marshal(bb)
		jr.idx++
		n++
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (jr *jreader) ReadBack(bbw *btsbuf.Writer) (int, error) {
	n := 0
	var le model.LogEvent
	for jr.idx >= 0 {
		le.Reset(uint64(jr.idx), jr.ss[jr.idx])
		bb, err := bbw.Allocate(le.BufSize())
		if err != nil {
			if n == 0 {
				return 0, err
			}
			return n, nil
		}
		le.Marshal(bb)
		jr.idx--
		n++
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func TestForward(t *testing.T) {
	var jr jreader
	jr.reset(0, []string{"aa", "bb", "cc"})

	var buf [20]byte
	it := NewIterator(&jr, buf[:])

	n := 0
	for !it.End() {
		var le model.LogEvent
		err := it.Get(&le)
		if err != nil {
			t.Fatal("should be fine to read, but got err=", err)
		}
		if int(le.Timestamp()) != n || le.Source() != jr.ss[n] {
			t.Fatal("Expecting record ", n, " but read ", &le)
		}
		it.Next()
		n++
	}
	if n != 3 {
		t.Fatal("Must be 3 records be read")
	}

	jr.idx = 0
	it.NoCpy = true
	n = 0
	for !it.End() {
		var le model.LogEvent
		err := it.Get(&le)
		if err != nil {
			t.Fatal("should be fine to read, but got err=", err)
		}
		if int(le.Timestamp()) != n || le.Source() != jr.ss[n] {
			t.Fatal("Expecting record ", n, " but read ", &le)
		}
		it.Next()
		n++
	}
}

func TestBackward(t *testing.T) {
	var jr jreader
	jr.reset(2, []string{"aa", "bb", "cc"})

	var buf [20]byte
	it := NewIterator(&jr, buf[:])
	it.Fwd = false

	n := 0
	for !it.End() {
		var le model.LogEvent
		err := it.Get(&le)
		if err != nil {
			t.Fatal("should be fine to read, but got err=", err)
		}
		if int(le.Timestamp()) != 2-n || le.Source() != jr.ss[2-n] {
			t.Fatal("Expecting record ", n, " but read ", &le)
		}
		it.Next()
		n++
	}
	if n != 3 {
		t.Fatal("Must be 3 records be read")
	}
}

func TestEmptyReader(t *testing.T) {
	var jr jreader
	jr.reset(0, []string{})

	var buf [20]byte
	it := NewIterator(&jr, buf[:])

	if it.Get(nil) != io.EOF || !it.End() {
		t.Fatal("Must be empty from the start")
	}
}

func TestInsufficientBuf(t *testing.T) {
	var jr jreader
	jr.reset(0, []string{"aaaa"})

	var buf [10]byte
	it := NewIterator(&jr, buf[:])

	var le model.LogEvent
	err := it.Get(&le)
	if err == nil || err == io.EOF {
		t.Fatal("must be error, but not io.EOF")
	}
	t.Log(err)
	if !it.End() {
		t.Fatal("Must be empty from the start")
	}
}

func TestForwardNegativeFilter(t *testing.T) {
	var jr jreader
	jr.reset(0, []string{"aa", "bb", "cc"})

	var buf [20]byte
	it := NewIterator(&jr, buf[:])
	it.FltF = func(ev *model.LogEvent) bool {
		return true
	}

	if !it.End() {
		t.Fatal("must be empty from start ")
	}
}

func TestForwardFilter(t *testing.T) {
	var jr jreader
	jr.reset(0, []string{"aa", "bb", "cc"})

	var buf [20]byte
	it := NewIterator(&jr, buf[:])
	// skip all, but contains `b`
	it.FltF = func(ev *model.LogEvent) bool {
		return !strings.Contains(ev.Source(), "b")
	}

	var le model.LogEvent
	it.Get(&le)
	if le.Timestamp() != 1 || le.Source() != "bb" {
		t.Fatal("expecting bb, but got ", &le)
	}
	it.Next()
	if !it.End() || it.Get(&le) == nil {
		t.Fatal("Must be end now")
	}
}

func TestBackwardFilter(t *testing.T) {
	var jr jreader
	jr.reset(3, []string{"aa", "bb", "cc", "dd"})

	var buf [20]byte
	it := NewIterator(&jr, buf[:])
	it.Fwd = false

	skip := true
	res := []string{}
	// skip all, but even
	it.FltF = func(ev *model.LogEvent) bool {
		skip = !skip
		return skip
	}

	var le model.LogEvent
	for !it.End() {
		if it.Get(&le) == nil {
			res = append(res, le.Source())
		}
		it.Next()
	}

	if !reflect.DeepEqual([]string{"dd", "bb"}, res) {
		t.Fatal("Unexpected ", res)
	}

}
