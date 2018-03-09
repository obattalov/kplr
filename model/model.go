// model package contains data types are used for data representations. The
// package defines also some common types which are used for various data
// processing procedures
package model

import (
	"time"
)

import (
	"github.com/kplr-io/container/btsbuf"

	"github.com/kplr-io/journal"
)

type (
	JournalRecord struct {
		Journal  string
		RecordId journal.RecordId
	}

	// FilterF returns true, if the event must be filtered (disregarded), or false
	// if it is not.
	FilterF func(ev *LogEvent) bool

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
		tags   TagLine
		tgsSet bool
	}
)

func (sme *SimpleMessageEncoder) Encode(msg string, bbw *btsbuf.Writer) error {
	bf := StringToByteArray(msg)
	res, err := bbw.Allocate(len(bf), true)
	if err != nil {
		return err
	}
	copy(res, bf)
	return nil
}

func (sle *SimpleLogEventEncoder) SetTags(tags TagLine) {
	if sle.tgsSet {
		panic("Wrong usage, cannot set tags after encoding first record")
	}
	sle.tags = tags
}

func (sle *SimpleLogEventEncoder) Encode(msg string, bbw *btsbuf.Writer) error {
	var le LogEvent
	ts := int64(time.Now().UnixNano() / int64(time.Millisecond))
	if !sle.tgsSet {
		le.InitWithTagLine(ts, WeakString(msg), sle.tags)
	} else {
		le.Init(ts, WeakString(msg))
	}
	sle.tgsSet = true
	rb, err := bbw.Allocate(le.BufSize(), true)
	if err != nil {
		return err
	}
	_, err = le.Marshal(rb)
	return err
}
