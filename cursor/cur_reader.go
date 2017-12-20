package cursor

import (
	"context"
	"errors"
	"io"

	"github.com/kplr-io/kplr/model"
)

type (
	// recs_reader used by the cur_reader for reading records from cursor
	recs_reader interface {
		nextRecord() (string, error)
		waitRecords(ctx context.Context)
		onReaderClosed()
	}

	// cur_reader implements NOT thread-safe implementation of io.ReaderCloser
	// on top of a cursor. Function Close() is tread-safe and may be used from
	// any go-routine to signal that the reader is closed, but the Read() function
	// can be used by one goroutine at a time.
	cur_reader struct {
		ctx    context.Context
		cancel context.CancelFunc
		limit  int64
		exact  bool
		rr     recs_reader
		buf    []byte
	}
)

var (
	errAlreadyClosed = errors.New("use when already closed")
)

func new_cur_reader(rr recs_reader, limit int64, exact bool) *cur_reader {
	r := new(cur_reader)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.limit = limit
	r.exact = exact
	r.rr = rr
	return r
}

func (r *cur_reader) Read(p []byte) (n int, err error) {
	n = 0
	for len(p) > 0 {
		err = r.fillBuf(r.exact && n == 0)
		if err != nil {
			if n != 0 {
				return n, nil
			}
			r.Close()
			return n, err
		}
		c := copy(p, r.buf)
		p = p[c:]
		r.buf = r.buf[c:]
		n += c
	}
	return n, nil
}

func (r *cur_reader) Close() error {
	r.rr.onReaderClosed()
	r.cancel()
	return nil
}

func (r *cur_reader) fillBuf(waitIfEof bool) error {
	if len(r.buf) > 0 {
		return nil
	}

	if r.limit == 0 {
		return io.EOF
	}

	for {
		if r.ctx.Err() != nil {
			return errAlreadyClosed
		}

		s, err := r.rr.nextRecord()
		if err == io.EOF && waitIfEof {
			r.rr.waitRecords(r.ctx)
			continue
		}

		if err != nil {
			return err
		}

		r.buf = model.StringToByteArray(s)
		if r.limit > 0 {
			r.limit--
		}
		return nil
	}
}
