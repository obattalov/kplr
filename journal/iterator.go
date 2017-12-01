package journal

import (
	"io"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/model"
)

type (

	// FilterF returns true, if the event must be filtered (disregarded), or false
	// if it is not.
	FilterF func(ev *model.LogEvent) bool

	// The Iterator allows to walk through a journal records forth or back.
	// It implements evstrm.Iterator interface what allows to use the iterator
	// in query pipelines.
	//
	// The iterator provides internal filtering records when ff function is provided
	Iterator struct {
		// the NoCpy means not create new buffer for returned event. The NoCpy
		// MUST be false in case of any non-trivial read approach or it is unknown
		// for sure what will happen with the returned event, can it be stored
		// for long time etc. Set it to true when you are absolutely sure no
		// leaks will happen using the event.
		NoCpy bool

		// filter function is used for filtering records while iterating.
		FltF FilterF

		// the journal reader
		JReader journal.Reader

		// Fwd defines whether ReadForward functions should be used or not
		Fwd bool

		bbw   btsbuf.Writer
		bbr   btsbuf.Reader
		le    model.LogEvent
		valid bool
		err   error
	}
)

func NewIterator(jr journal.Reader, buf []byte) *Iterator {
	it := new(Iterator)
	it.JReader = jr
	it.bbw.Reset(buf, false)
	it.Fwd = true
	return it
}

// --------------------------- evstrm.Iterator -------------------------------
func (it *Iterator) End() bool {
	return it.Get(&it.le) != nil
}

func (it *Iterator) Get(le *model.LogEvent) error {
	if it.valid {
		*le = it.le
		return nil
	}

	for {
		it.fillBuf()
		if it.bbr.End() {
			if it.err != nil {
				return it.err
			}
			return io.EOF
		}

		buf := it.bbr.Get()
		if it.FltF != nil {
			// consider, that we likely be filtering it, so just use the reader
			// buffer to unmarshal the event and check it
			_, it.err = it.le.Unmarshal(buf)
			if it.err != nil {
				it.bbr.Reset(nil)
				return it.err
			}

			if it.FltF(&it.le) {
				// likely it happens, so go to next record
				it.bbr.Next()
				continue
			}

			if it.NoCpy {
				it.valid = true
				*le = it.le
				return nil
			}
		}

		if it.NoCpy {
			_, it.err = it.le.Unmarshal(buf)
		} else {
			_, it.err = it.le.UnmarshalCopy(buf)
		}

		if it.err != nil {
			it.bbr.Reset(nil)
			return it.err
		}

		*le = it.le
		it.valid = true
		return nil
	}
}

func (it *Iterator) Next() {
	it.bbr.Next()
	it.valid = false
}

func (it *Iterator) fillBuf() {
	if !it.bbr.End() || it.err != nil {
		return
	}

	it.bbw.Reset(it.bbw.Buf(), false)
	if it.Fwd {
		_, it.err = it.JReader.ReadForward(&it.bbw)
	} else {
		_, it.err = it.JReader.ReadBack(&it.bbw)
	}
	it.bbw.Close()

	if it.err != nil {
		it.bbr.Reset(nil)
		return
	}
	it.err = it.bbr.Reset(it.bbw.Buf())
}
