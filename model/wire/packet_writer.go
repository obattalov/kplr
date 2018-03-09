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

// MakeBtsBuf writes a log context. Encoder should control status of the packet
func (w *Writer) MakeBtsBuf(source string, lines []string) ([]byte, error) {
	w.bbw.Reset(w.bbw.Buf(), true)
	bf, err := w.bbw.Allocate(len(source), true)
	if err != nil {
		return nil, err
	}

	copy(bf, model.StringToByteArray(source))
	for _, ln := range lines {
		err = w.encoder.Encode(ln, &w.bbw)
		if err != nil {
			return nil, err
		}
	}

	return w.bbw.Close()
}
