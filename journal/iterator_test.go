package journal

import (
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/model"
)

func TestForward(t *testing.T) {
	var jr JReaderMock
	jr.Reset(0, []model.WeakString{"aa", "bb", "cc"})

	var buf [30]byte
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
		t.Fatal("Must be 3 records be read, but n=", n)
	}
}

func TestPositioning0(t *testing.T) {
	var jr JReaderMock
	jr.Reset(0, []model.WeakString{"aa", "bb", "cc", "dd", "ee"})

	var buf [30]byte
	it := NewIterator(&jr, buf[:])
	it.Id = "Test"
	maxRec := journal.RecordId{0, jr.offs[len(jr.offs)-1]}
	it.SetCurrentPos(maxRec)
	if it.End() {
		t.Fatal("Must be last record")
	}
	itPos := it.GetIteratorPos().(IteratorPosition)
	if itPos.Id != it.Id || itPos.Position != it.pos {
		t.Fatal("Expecting ", it.Id, " ", it.pos, ", but got ", itPos)
	}

	it.Next()
	if !it.End() {
		t.Fatal("End() must be reached")
	}

	if it.GetCurrentPos() != maxRec {
		t.Fatal("Expected ", maxRec, ", but ", it.GetCurrentPos())
	}
}

func TestPositioning(t *testing.T) {
	var jr JReaderMock
	jr.Reset(0, []model.WeakString{"aa", "bb", "cc", "dd", "ee"})

	var buf [30]byte
	it := NewIterator(&jr, buf[:])

	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < 1000; i++ {
		idx := 1 + rand.Intn(len(jr.ss)-2)
		dir := 1
		it.Backward(false)
		if rand.Intn(2) == 0 {
			dir = -1
			it.Backward(true)
		}
		it.SetCurrentPos(journal.RecordId{0, jr.offs[idx]})
		//t.Log("current i=", i, " dir=", dir, ", idx=", idx, " jr.idx=", jr.idx, " it.valid=", it.valid)

		var le model.LogEvent
		err := it.Get(&le)
		if err != nil {
			t.Fatal("oops err=", err)
		}
		if le.Source() != jr.ss[idx] || it.GetCurrentPos().Offset != jr.offs[idx] {
			t.Fatal("Expected offset=", jr.offs[idx], ", and val=", jr.ss[idx], ", but ", &le, ", pos=", it.GetCurrentPos())
		}
		it.Next()
		idx += dir

		err = it.Get(&le)
		if err != nil {
			t.Fatal("oops err=", err)
		}

		if le.Source() != jr.ss[idx] || it.GetCurrentPos().Offset != jr.offs[idx] {
			t.Fatal("Expected offset=", jr.offs[idx], ", and val=", jr.ss[idx], ", but ", &le, ", pos=", it.GetCurrentPos())
		}
	}
}

func TestBackward(t *testing.T) {
	var jr JReaderMock
	jr.Reset(2, []model.WeakString{"aa", "bb", "cc"})

	var buf [30]byte
	it := NewIterator(&jr, buf[:])
	it.SetCurrentPos(journal.RecordId{0, jr.offs[2]})
	it.Backward(true)

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
		t.Fatal("Must be 3 records be read, but n=", n)
	}
}

func TestEmptyReader(t *testing.T) {
	var jr JReaderMock
	jr.Reset(0, []model.WeakString{})

	var buf [20]byte
	it := NewIterator(&jr, buf[:])

	if it.Get(nil) != io.EOF || !it.End() {
		t.Fatal("Must be empty from the start")
	}
}

func TestInsufficientBuf(t *testing.T) {
	var jr JReaderMock
	jr.Reset(0, []model.WeakString{"aaaa"})

	var buf [10]byte
	it := NewIterator(&jr, buf[:])

	var le model.LogEvent
	err := it.Get(&le)
	if err == nil || err == io.EOF {
		t.Fatal("must be error, but not io.EOF, err=", err)
	}
	t.Log(err)
	if !it.End() {
		t.Fatal("Must be empty from the start")
	}
}

func TestForwardNegativeFilter(t *testing.T) {
	var jr JReaderMock
	jr.Reset(0, []model.WeakString{"aa", "bb", "cc"})

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
	var jr JReaderMock
	jr.Reset(0, []model.WeakString{"aa", "bb", "cc"})

	var buf [30]byte
	it := NewIterator(&jr, buf[:])
	// skip all, but contains `b`
	it.FltF = func(ev *model.LogEvent) bool {
		return !strings.Contains(string(ev.Source()), "b")
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
	var jr JReaderMock
	jr.Reset(3, []model.WeakString{"aa", "bb", "cc", "dd"})

	var buf [40]byte
	it := NewIterator(&jr, buf[:])
	it.SetCurrentPos(journal.RecordId{0, jr.offs[3]})
	it.Backward(true)

	skip := true
	res := []string{}
	// skip all, but even
	it.FltF = func(ev *model.LogEvent) bool {
		skip = !skip
		return skip
	}

	pos := []int64{}
	var le model.LogEvent
	for !it.End() {
		if it.Get(&le) == nil {
			res = append(res, le.Source().String())
		}
		pos = append(pos, it.GetCurrentPos().Offset)
		it.Next()
	}

	if !reflect.DeepEqual([]string{"dd", "bb"}, res) {
		t.Fatal("Unexpected ", res)
	}

	if !reflect.DeepEqual([]int64{jr.offs[3], jr.offs[1]}, pos) {
		t.Fatal("Unexpected ", pos, ", but offs=", jr.offs)
	}
}

func TestSkipRecords(t *testing.T) {
	var jr JReaderMock
	jr.Reset(0, []model.WeakString{"aa", "asdfbb", "cfasdc", "dadfd", "ee", "asdfasdf"})

	var buf [60]byte
	it := NewIterator(&jr, buf[:])
	for i := 0; i < len(jr.ss); i++ {
		t.Log("i=", i, " offs=", jr.offs, ", ", it.GetCurrentPos(), " ", it.valid, " ", jr.idx)
		if it.GetCurrentPos().Offset != jr.offs[i] {
			t.Fatal("Expected ", jr.offs[i], ", but got ", it.GetCurrentPos().Offset)
		}
		it.Next()
	}

	it.Backward(true)
	err := it.Get(nil)
	for i := len(jr.ss) - 1; i >= 0; i-- {
		if it.GetCurrentPos().Offset != jr.offs[i] {
			t.Fatal("Expected ", jr.offs[i], ", but got ", it.GetCurrentPos().Offset)
		}
		it.Next()
	}

	t.Log(jr, it.err)
	it.SetCurrentPos(journal.RecordId{0, jr.offs[2]})
	t.Log(jr, it.err)
	var le model.LogEvent
	err = it.Get(&le)
	if le.Source() != jr.ss[2] || err != nil {
		t.Fatal("Expected ", jr.ss[2], ", but got ", le, " err=", err)
	}
	it.next()
	it.next()
	it.next()

	if !it.End() {
		t.Fatal("expecting end met!")
	}

	it.Backward(false)
	err = it.Get(&le)
	if le.Source() != jr.ss[0] || err != nil {
		t.Fatal("Expected ", jr.ss[0], ", but got ", le, " err=", err)
	}
}

func TestWalkForthAndBack(t *testing.T) {
	var jr JReaderMock
	jr.Reset(0, []model.WeakString{"aa", "asdfbb", "cfasdc", "dadfd", "ee", "asdfasdf"})

	bkw := false
	var buf [60]byte
	it := NewIterator(&jr, buf[:])
	for i := 0; i < 1000; i++ {
		var le model.LogEvent
		err := it.Get(&le)
		if err == io.EOF {
			bkw = !bkw
			it.Backward(bkw)
			continue
		}

		idx := jr.getIdxByOffset(it.GetCurrentPos().Offset)
		if jr.ss[idx] != le.Source() {
			t.Fatal("idx=", idx, " ", it.GetCurrentPos(), " expected ", jr.ss[idx], ", but ", le.Source())
		}
		it.Next()
	}
}

func TestFilters(t *testing.T) {
	var jr JReaderMock
	jr.Reset(0, []model.WeakString{"aa", "asdfbb", "cfasdc", "dadfd", "ee", "asdfasdf"})

	bkw := false
	var buf [60]byte
	it := NewIterator(&jr, buf[:])
	it.FltF = func(ev *model.LogEvent) bool {
		return ev.Timestamp()&1 != 0
	}
	m := make(map[int]int)
	for i := 0; i < 1000; i++ {
		var le model.LogEvent
		err := it.Get(&le)
		if err == io.EOF {
			bkw = !bkw
			it.Backward(bkw)
			continue
		}

		idx := jr.getIdxByOffset(it.GetCurrentPos().Offset)
		m[idx] = idx
		if jr.ss[idx] != le.Source() {
			t.Fatal("idx=", idx, " ", it.GetCurrentPos(), " expected ", jr.ss[idx], ", but ", le.Source())
		}
		it.Next()
	}
	if len(m) != 3 || m[0] != 0 || m[2] != 2 || m[4] != 4 {
		t.Fatal("Only even values must be got ", m)
	}
}
