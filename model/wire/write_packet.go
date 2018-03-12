package wire

import (
	"errors"
	"sort"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/kplr/model"
)

type (
	// WritePacket defines an interface for a group of records that should be
	// stored into the journal
	WritePacket interface {
		// GetSourceId returns Source Id from known tag Maps
		GetSourceId() string

		// GetTagNames returns list of tag names in ascending order. This is a
		// WeakString which must not be stored somewhere in direct form, but it
		// can be used for transformatios. For example it can be cast to []byte
		// then stored to disk.
		GetTagNames() model.SSlice

		// GetTagsMap returns map of tagName:tagValue pairs as map of WeakStrings
		// must not be stored somewhere
		GetTagsMap() map[model.WeakString]model.WeakString

		// GetTags returns Tags string, safe to be stored
		GetTags() string

		// GetDataReader returns reader which points to the data
		GetDataReader() btsbuf.Iterator
	}

	// The BtBufWritePacket wraps a byte buffer and provides WritePacket interface
	// around it
	BtBufWritePacket struct {
		// Bytes buffer reader
		bbi btsbuf.Reader

		tagNames model.SSlice
		tags     string

		// tm is tags map, it contains key:value pairs
		tm map[model.WeakString]model.WeakString

		// a helper array, which is used to keep tagNames
		sArr [20]string
		// default buffer for iterator
		defBuf [256]byte
		// iterator buf
		buf []byte
		// bufOk shows the buf has relevant value
		bufOk bool
	}
)

func (bbwp *BtBufWritePacket) Init(buf []byte) error {
	err := bbwp.bbi.Reset(buf)
	if err != nil {
		return err
	}

	if bbwp.bbi.End() {
		return errors.New("empty list")
	}

	// get a list of tags encoded as a slice of strings. Odd records are keys, even
	// ones are values
	var sArr [20]model.WeakString
	ss := model.SSlice(sArr[:])
	ss, _, err = model.UnmarshalSSlice(ss, bbwp.bbi.Get())
	if err != nil {
		return err
	}

	err = bbwp.parseHeader(ss)
	if err != nil {
		return err
	}

	bbwp.bbi.Next()
	return nil
}

func (bbwp *BtBufWritePacket) GetSourceId() string {
	return string(bbwp.tm[model.TAG_SRC_ID])
}

func (bbwp *BtBufWritePacket) GetTagNames() model.SSlice {
	return bbwp.tagNames
}

func (bbwp *BtBufWritePacket) GetTagsMap() map[model.WeakString]model.WeakString {
	return bbwp.tm
}

func (bbwp *BtBufWritePacket) GetTags() string {
	if bbwp.tags == "" {
		bbwp.tags = string(model.WeakString(model.MapToTags(bbwp.tagNames, bbwp.tm)))
	}

	return bbwp.tags
}

func (bbwp *BtBufWritePacket) GetDataReader() btsbuf.Iterator {
	return bbwp
}

func (bbwp *BtBufWritePacket) parseHeader(ss model.SSlice) error {
	if len(ss) < 2 {
		return errors.New("Expecting at least one pair - source id in tags of the header.")
	}
	if len(ss)&1 == 1 {
		return errors.New("header must contain even number of strings(key:value pairs)")
	}

	// Sorting keys, before inserting them into the table
	srtKeys := bbwp.sArr[:0]
	bbwp.tm = make(map[model.WeakString]model.WeakString, len(ss))
	for i := 0; i < len(ss); i += 2 {
		tag := ss[i]
		bbwp.tm[tag] = ss[i+1]
		key := string(tag)
		idx := sort.SearchStrings(srtKeys, key)
		srtKeys = append(srtKeys, key)
		if idx < len(srtKeys)-1 {
			copy(srtKeys[idx+1:], srtKeys[idx:])
		}
		srtKeys[idx] = key
	}

	srcId, ok := bbwp.tm[model.TAG_SRC_ID]
	if !ok {
		return errors.New("No expected tag " + model.TAG_SRC_ID + " in the header.")
	}
	// Turns model.TAG_SRC_ID to real string
	bbwp.tm[model.TAG_SRC_ID] = model.WeakString(srcId.String())

	bbwp.tags = ""
	bbwp.tagNames = model.StrSliceToSSlice(bbwp.sArr[:len(srtKeys)])
	return nil
}

// ========================== btsbuf.Iterator ================================
func (bbwp *BtBufWritePacket) End() bool {
	return bbwp.bbi.End()
}

func (bbwp *BtBufWritePacket) Get() []byte {
	if bbwp.bufOk {
		return bbwp.buf
	}

	itBuf := bbwp.bbi.Get()
	var le model.LogEvent
	_, err := le.Unmarshal(itBuf)
	if err != nil {
		return nil
	}
	le.Reset(le.Timestamp(), le.Source(), model.Tags(bbwp.GetTags()))
	sz := le.BufSize()

	bbwp.buf = bbwp.buf[:cap(bbwp.buf)]
	if sz > len(bbwp.buf) {
		bbwp.buf = bbwp.defBuf[:]
		if sz > len(bbwp.buf) {
			if sz < 2048 {
				sz = 2048
			}
			bbwp.buf = make([]byte, sz)
		}
	}
	n, _ := le.Marshal(bbwp.buf)
	bbwp.bufOk = true
	bbwp.buf = bbwp.buf[:n]
	return bbwp.buf
}

func (bbwp *BtBufWritePacket) Next() {
	bbwp.bufOk = false
	bbwp.bbi.Next()
}
