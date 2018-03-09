package journal

import (
	"context"
	"fmt"
	"io"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/model"
)

// JReaderMock mocking the journal.Reader behavior
type JReaderMock struct {
	idx  int
	ss   model.SSlice
	offs []int64
}

func (jr *JReaderMock) Reset(idx int, ss []model.WeakString) {
	jr.idx = idx
	jr.ss = model.SSlice(ss)
	nxt := int64(0)
	for i, s := range ss {
		jr.offs = append(jr.offs, nxt)
		var le model.LogEvent
		le.Init(int64(i), s)
		nxt += int64(le.BufSize()) + journal.DataRecMetaSize
	}
	jr.offs = append(jr.offs, nxt)
}

func (jr *JReaderMock) ReadForward(bbw *btsbuf.Writer) (journal.RecordId, error) {
	n := 0
	if jr.idx < 0 {
		jr.idx = 0
	}
	if jr.idx > len(jr.offs) {
		jr.idx = len(jr.offs)
	}
	res := journal.RecordId{0, jr.offs[jr.idx]}
	var le model.LogEvent
	for jr.idx < len(jr.ss) {
		le.Init(int64(jr.idx), jr.ss[jr.idx])
		bb, err := bbw.Allocate(le.BufSize(), false)
		if err != nil {
			if n == 0 {
				return res, err
			}
			return res, nil
		}
		le.Marshal(bb)
		jr.idx++
		n++
	}
	if n == 0 {
		return res, io.EOF
	}
	return res, nil
}

func (jr *JReaderMock) ReadBack(bbw *btsbuf.Writer) (journal.RecordId, error) {
	n := 0
	if jr.idx >= len(jr.ss) {
		jr.idx = len(jr.ss) - 1
	}

	var res journal.RecordId
	if jr.idx >= 0 {
		res = journal.RecordId{0, jr.offs[jr.idx]}
	}
	var le model.LogEvent
	for jr.idx >= 0 {
		le.Init(int64(jr.idx), jr.ss[jr.idx])
		bb, err := bbw.Allocate(le.BufSize(), false)
		if err != nil {
			if n == 0 {
				return res, err
			}
			return res, nil
		}
		le.Marshal(bb)
		jr.idx--
		n++
	}
	if n == 0 {
		return res, io.EOF
	}
	return res, nil
}

func (jr *JReaderMock) SetCurrentRecordId(curRecId journal.RecordId) {
	idx := jr.getIdxByOffset(curRecId.Offset)
	if idx < 0 {
		panic(fmt.Sprint("Provided offset is not found ", curRecId, " offs=", jr.offs))
	}
	// if after the last one, then shift it to the last record
	if idx == len(jr.ss) {
		idx--
	}
	jr.idx = idx
}

func (jr *JReaderMock) getIdxByOffset(off int64) int {
	for i := 0; i <= len(jr.ss); i++ {
		if jr.offs[i] == off {
			return i
		}
	}
	return -1
}

func (jr *JReaderMock) GetCurrentRecordId() journal.RecordId {
	return journal.RecordId{0, int64(jr.idx)}
}

func (jr *JReaderMock) Close() error {
	jr.idx = len(jr.ss)
	return nil
}

func (jr *JReaderMock) WaitNewData(ctx context.Context) error {
	panic("not supported")
}
