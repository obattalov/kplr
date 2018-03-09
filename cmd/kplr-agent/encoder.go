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

func (e *encoder) encode(hdr *hdrsCacheRec, ev *geyser.Event) ([]byte, error) {
	e.bbw.Reset(e.bbw.Buf(), true)
	bf, err := e.bbw.Allocate(len(hdr.srcId), true)
	if err != nil {
		return nil, err
	}

	_, err = model.MarshalStringBuf(hdr.srcId, bf)
	if err != nil {
		return nil, err
	}

	first := true
	var le model.LogEvent
	for _, r := range ev.Records {
		if first {
			le.InitWithTagLine(int64(r.GetTs().UnixNano()), model.WeakString(r.Data), hdr.tags)
		} else {
			le.Init(int64(r.GetTs().UnixNano()), model.WeakString(r.Data))
		}
		first = false

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
