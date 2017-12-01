package evstrm

import (
	"io"

	"github.com/kplr-io/kplr/model"
)

type (
	Iterator interface {
		// End() Returns whether the end is reached
		End() bool

		// Get() reads event or return error if the operation failed. It returns
		// io.EOF if the method is called after End() which returns positive result
		Get(le *model.LogEvent) error

		// Next switches to the next record
		Next()
	}

	test_it []string
)

func (ti *test_it) End() bool {
	return len(*ti) == 0
}

func (ti *test_it) Get(le *model.LogEvent) error {
	if len(*ti) > 0 {
		le.Reset(0, (*ti)[0])
		return nil
	}
	return io.EOF
}

func (ti *test_it) Next() {
	if len(*ti) > 0 {
		*ti = (*ti)[1:]
	}
}
