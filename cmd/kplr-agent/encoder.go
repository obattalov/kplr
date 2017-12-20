package main

import (
	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/geyser"
	"github.com/kplr-io/kplr/model"
)

type encoder struct {
	bbw btsbuf.Writer
}

func newEncoder() *encoder {
	e := new(encoder)
	e.bbw.Reset(make([]byte, 4096), true)
	return e
}

func (e *encoder) encode(header model.SSlice, ev *geyser.Event) ([]byte, error) {
	e.bbw.Reset(e.bbw.Buf(), true)
	bf, err := e.bbw.Allocate(header.Size(), true)
	if err != nil {
		return nil, err
	}

	_, err = model.MarshalSSlice(header, bf)
	if err != nil {
		return nil, err
	}

	var le model.LogEvent
	for _, r := range ev.Records {
		le.Reset(uint64(r.GetTs().UnixNano()), model.WeakString(r.Data), model.Tags(""))
		rb, err := e.bbw.Allocate(le.BufSize(), true)
		if err != nil {
			return nil, err
		}
		_, err = le.Marshal(rb)
		if err != nil {
			return nil, err
		}
	}

	return e.bbw.Close()
}
