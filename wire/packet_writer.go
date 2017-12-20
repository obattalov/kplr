package wire

import (
	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/kplr/model"
)

type (
	Writer struct {
		encoder model.MessageEncoder
		bbw     btsbuf.Writer
	}
)

func NewWriter(encoder model.MessageEncoder) *Writer {
	w := new(Writer)
	w.encoder = encoder
	w.bbw.Reset(make([]byte, 4096), true)
	return w
}

func (w *Writer) MakeBtsBuf(header model.SSlice, lines []string) ([]byte, error) {
	w.bbw.Reset(w.bbw.Buf(), true)
	bf, err := w.bbw.Allocate(header.Size())
	if err != nil {
		return nil, err
	}

	_, err = model.MarshalSSlice(header, bf)
	if err != nil {
		return nil, err
	}

	for _, ln := range lines {
		err = w.encoder.Encode(ln, &w.bbw)
		if err != nil {
			return nil, err
		}
	}

	return w.bbw.Close()
}
