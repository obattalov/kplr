package model

import (
	"time"

	"github.com/kplr-io/container/btsbuf"
)

type (
	// MessageEncoder interface provides functionality for encoding log messages
	// to a slice of bytes
	MessageEncoder interface {
		// Encode takes a text message msg and writes it to the bytes buffer
		Encode(msg string, bbw *btsbuf.Writer) error
	}

	// The SimpleMessageEncoder simply transforms a string message to slice of bytes
	SimpleMessageEncoder struct{}

	// SimpleLogEventEncoder encode lines to LogEvents, it sets encoding timestamp
	SimpleLogEventEncoder struct {
		tags Tags
	}
)

func (sme *SimpleMessageEncoder) Encode(msg string, bbw *btsbuf.Writer) error {
	bf := []byte(msg)
	res, err := bbw.Allocate(len(bf), true)
	if err != nil {
		return err
	}
	copy(res, bf)
	return nil
}

// SetTags specifies tags that will be attached to each log event
func (sle *SimpleLogEventEncoder) SetTags(tags Tags) {
	sle.tags = tags
}

func (sle *SimpleLogEventEncoder) Encode(msg string, bbw *btsbuf.Writer) error {
	var le LogEvent
	ts := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	le.Reset(ts, WeakString(msg), sle.tags)
	rb, err := bbw.Allocate(le.BufSize(), true)
	if err != nil {
		return err
	}
	_, err = le.Marshal(rb)
	return err
}
