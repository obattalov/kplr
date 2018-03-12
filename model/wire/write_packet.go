package wire

import (
	"fmt"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/kplr/model"
)

type (
	// WritePacket defines an interface for a group of records that should be
	// stored into a journal.
	//
	// The packet supposes to have the following format (records in btsbuf):
	// 1st Record: SourcId encoded as a string value.
	// 2nd Record: LogEvent with tags to be written
	// 3rd..Nth Records: LogEvent(s) with tags == ""
	WritePacket interface {
		// GetSourceId returns Source Id from known tag Maps
		GetSourceId() string

		// GetTags returns Tags string, safe to be stored
		GetTags() model.TagLine

		// Will update tagGroupId for all records in the buffer. The operation
		// will affect initial buffer value
		ApplyTagGroupId(tgid int64) error

		// GetDataReader returns reader which points to the data
		GetDataReader() btsbuf.Iterator
	}

	// The BtBufWritePacket wraps a byte buffer and provides WritePacket interface
	// around it
	BtBufWritePacket struct {
		buf []byte

		// Bytes buffer reader
		bbi btsbuf.Reader

		tagLine model.TagLine
		source  string

		// tm is tags map, it contains key:value pairs
		tm map[model.WeakString]model.WeakString
	}
)

func (bbwp *BtBufWritePacket) Init(buf []byte) error {
	err := bbwp.bbi.Reset(buf)
	if err != nil {
		return err
	}

	if bbwp.bbi.End() {
		return fmt.Errorf("Empty packet")
	}

	// source
	ws := model.UnmarshalStringBuf(bbwp.bbi.Get())
	bbwp.source = ws.String()
	if len(bbwp.source) == 0 {
		return fmt.Errorf("Source Id could not be empty")
	}
	bbwp.bbi.Next()

	// read tagLine from the first record
	var le model.LogEvent
	_, err = le.Unmarshal(bbwp.bbi.Get())
	if err != nil {
		return err
	}
	bbwp.tagLine = le.GetTagLine()
	if len(bbwp.tagLine) == 0 {
		return fmt.Errorf("Tags should be provided in first record, but it is empty")
	}

	return nil
}

func (bbwp *BtBufWritePacket) GetSourceId() string {
	return bbwp.source
}

func (bbwp *BtBufWritePacket) GetTags() model.TagLine {
	return bbwp.tagLine
}

func (bbwp *BtBufWritePacket) GetDataReader() btsbuf.Iterator {
	return &bbwp.bbi
}

func (bbwp *BtBufWritePacket) ApplyTagGroupId(tgid int64) error {
	var bi btsbuf.Reader
	err := bi.Reset(bbwp.buf)
	if err != nil {
		return err
	}
	bi.Next()

	var le model.LogEvent
	le.SetTGroupId(tgid)
	for !bi.End() {
		le.MarshalTagGroupIdOnly(bi.Get())
		bi.Next()
	}
	return nil
}
