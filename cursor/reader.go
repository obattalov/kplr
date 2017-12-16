package cursor

import (
	"io"

	"github.com/kplr-io/kplr/model"
)

type (

	// formatter gets a formatted string that should be returned to user. If lines
	// are supposed to be separated, the separator can be included into the string
	// by the formatter
	// the formatter returns io.EOF in case of no strings can be read
	formatter func() (string, error)

	reader struct {
		f   formatter
		buf []byte
	}
)

func (r *reader) Read(p []byte) (n int, err error) {
	for len(p) > 0 {
		err = r.fillBuf()
		if err != nil {
			if err == io.EOF && n != 0 {
				return n, nil
			}
			return n, err
		}
		c := copy(p, r.buf)
		p = p[c:]
		r.buf = r.buf[c:]
		n += c
	}
	return n, nil
}

func (r *reader) fillBuf() error {
	if len(r.buf) > 0 {
		return nil
	}
	s, err := r.f()
	if err != nil {
		return err
	}
	r.buf = model.StringToByteArray(s)
	return nil
}
