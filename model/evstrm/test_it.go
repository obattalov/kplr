package evstrm

import (
	"io"

	"github.com/kplr-io/kplr/model"
)

type (
	test_it struct {
		id  int
		ss  []model.WeakString
		idx int
		bkw bool
	}
)

func (ti *test_it) End() bool {
	if ti.bkw {
		return ti.idx < 0
	}
	return ti.idx >= len(ti.ss)
}

func (ti *test_it) Get(le *model.LogEvent) error {
	if ti.bkw {
		if ti.idx < 0 {
			return io.EOF
		}
		if ti.idx >= len(ti.ss) {
			ti.idx = len(ti.ss) - 1
		}
	} else {
		if ti.idx >= len(ti.ss) {
			return io.EOF
		}
		if ti.idx < 0 {
			ti.idx = 0
		}
	}
	le.Reset(uint64(ti.idx), ti.ss[ti.idx], model.Tags(ti.ss[ti.idx]))
	return nil
}

func (ti *test_it) Next() {
	if ti.bkw {
		if ti.idx >= 0 {
			ti.idx--
		}
		return
	}
	if ti.idx < len(ti.ss) {
		ti.idx++
	}
}

func (ti *test_it) Backward(bkwrd bool) {
	ti.bkw = bkwrd
}

func (ti *test_it) GetIteratorPos() IteratorPos {
	return ti.id + ti.idx
}

func (ti *test_it) Close() error {
	return nil
}
