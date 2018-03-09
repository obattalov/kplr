package cursor

import (
	"context"
	"errors"
	"io"

	"github.com/kplr-io/kplr/model"
)

type (
	// recsReader is used by the curReader for reading records from a cursor
	recsReader interface {
		nextRecord() (string, error)
		waitRecords(ctx context.Context)
		onReaderClosed()
	}

	// curReader implements NOT thread-safe implementation of io.ReaderCloser
	// on top of a cursor. Function Close() is tread-safe and may be used from
	// any go-routine to signal that the reader is closed, but the Read() function
	// can be used by one goroutine at a time.
	curReader struct {
		ctx    context.Context
		cancel context.CancelFunc
		limit  int64
		exact  bool
		rr     recsReader
		buf    []byte
	}
)

var (
	errAlreadyClosed = errors.New("use when already closed")
)

func newCurReader(rr recsReader, limit int64, exact bool) *curReader {
	r := new(curReader)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.limit = limit
	r.exact = exact
	r.rr = rr
	return r
}

func (r *curReader) Read(p []byte) (n int, err error) {
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

func (r *curReader) Close() error {
	r.rr.onReaderClosed()
	r.cancel()
	return nil
}

func (r *curReader) fillBuf(waitIfEof bool) error {
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
