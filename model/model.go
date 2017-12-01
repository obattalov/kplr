package model

import (
	"time"

	"github.com/kplr-io/container/btsbuf"
)

type (
	// The Descriptor interface provides an abstraction for a kplr environment.
	// It provides helper methods for describing the environment specifics,
	// for instance, it returns Meta object for event group headers, which
	// could contain different fields in different environments. The Descriptor
	// implementation can return journal id calculated by the meta header etc.
	Descriptor interface {
		// EventGroupMeta returns Meta object for a group of objects
		EventGroupMeta() Meta

		// GetJournalId returns journal id by event group meta info
		GetJournalId(evGrpMeta Event) string
	}

	// MessageEncoder interface provides functionality for encoding log messages
	// to a slice of bytes
	MessageEncoder interface {
		// Encode takes a text message msg and writes it to the bytes buffer
		Encode(msg string, bbw *btsbuf.Writer) error
	}

	// The SimpleMessageEncoder simply transforms a string message to slice of bytes
	SimpleMessageEncoder struct{}

	// SimpleLogEventEncoder encode lines to LogEvents, it sets encoding timestamp
	SimpleLogEventEncoder struct{}
)

func (sme *SimpleMessageEncoder) Encode(msg string, bbw *btsbuf.Writer) error {
	bf := []byte(msg)
	res, err := bbw.Allocate(len(bf))
	if err != nil {
		return err
	}
	copy(res, bf)
	return nil
}

func (sle *SimpleLogEventEncoder) Encode(msg string, bbw *btsbuf.Writer) error {
	var le LogEvent
	ts := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	le.Reset(ts, msg)
	rb, err := bbw.Allocate(le.BufSize())
	if err != nil {
		return err
	}
	_, err = le.Marshal(rb)
	return err
}
