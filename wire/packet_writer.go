package wire

import (
	"github.com/kplr-io/container"
	"github.com/kplr-io/kplr/model"
)

type (
	Writer struct {
		encoder model.MessageEncoder
		hdrMeta model.Meta
		bbw     container.BtsBufWriter
	}
)

func NewWriter(encoder model.MessageEncoder, hdrMeta model.Meta) *Writer {
	w := new(Writer)
	w.encoder = encoder
	w.hdrMeta = hdrMeta
	w.bbw.Reset(make([]byte, 4096), true)
	return w
}

func (w *Writer) MakeBtsBuf(header model.Event, lines []string) ([]byte, error) {
	w.bbw.Reset(w.bbw.Buf(), true)
	bf, err := w.bbw.Allocate(header.Size(w.hdrMeta))
	if err != nil {
		return nil, err
	}

	_, err = model.MarshalEvent(w.hdrMeta, header, bf)
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
