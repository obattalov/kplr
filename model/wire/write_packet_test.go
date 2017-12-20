package wire

import (
	"reflect"
	"testing"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/kplr/model"
)

func TestBtBufWritePacket(t *testing.T) {
	var tags model.Tags
	tags = tags.Add(model.TAG_SRC_ID, "bbb")

	bbw := makeBtsBuf(t, model.SSlice{model.TAG_SRC_ID, "bbb"}, []string{"1", "2"})
	var wp BtBufWritePacket
	err := wp.Init(bbw.Buf())
	if err != nil {
		t.Fatal("could not init BtBufWritePacket, err=", err)
	}

	if !reflect.DeepEqual(wp.GetTagNames(), model.SSlice{model.TAG_SRC_ID}) {
		t.Fatal("GetTagNames is not expected", wp.GetTagNames())
	}

	if wp.GetTags() != string(tags) {
		t.Fatal("wp.GetTags() is not expected ", wp.GetTags(), ", but expected ones ", tags)
	}

	tm := wp.GetTagsMap()
	if len(tm) != 1 || tm[model.TAG_SRC_ID] != "bbb" {
		t.Fatal("Unexpected tags list", tm)
	}

	if string(wp.GetSourceId()) != "bbb" {
		t.Fatal("Should be bbb, but ", string(wp.GetSourceId()))
	}

	if wp.GetDataReader().End() || !checkLogEvent(wp.GetDataReader().Get(), "1", string(tags)) {
		t.Fatal("Expected 1, but got ", string(wp.GetDataReader().Get()))
	}
	wp.GetDataReader().Next()
	if wp.GetDataReader().End() || !checkLogEvent(wp.GetDataReader().Get(), "2", string(tags)) {
		t.Fatal("Expected 2, but got ", string(wp.GetDataReader().Get()))
	}

	wp.GetDataReader().Next()
	if !wp.GetDataReader().End() {
		t.Fatal("End must be reached")
	}
}

func TestBtBufWritePacketWrongInit(t *testing.T) {
	var wp BtBufWritePacket
	err := wp.Init(nil)
	if err == nil {
		t.Fatal("could init BtBufWritePacket, but should be an error")
	}

	bbw := makeBtsBuf(t, model.SSlice{}, []string{})
	err = wp.Init(bbw.Buf())
	if err == nil {
		t.Fatal("could init BtBufWritePacket, but should be an error, the wrong header")
	}

	bbw = makeBtsBuf(t, model.SSlice{model.TAG_SRC_ID, "bbb", "ccc"}, []string{})
	err = wp.Init(bbw.Buf())
	if err == nil {
		t.Fatal("could init BtBufWritePacket, but should be an error, the wrong header")
	}

	bbw = makeBtsBuf(t, model.SSlice{"bbb", model.TAG_SRC_ID}, []string{})
	err = wp.Init(bbw.Buf())
	if err == nil {
		t.Fatal("could init BtBufWritePacket, but should be an error, no ", model.TAG_SRC_ID, " as a key provided")
	}
}

func TestBtBufWritePacketIterator(t *testing.T) {
	var tags model.Tags
	tags = tags.Add(model.TAG_SRC_ID, "bbb")

	lngStr := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	var wp BtBufWritePacket
	if len(lngStr) <= len(wp.defBuf) {
		t.Fatal("Wrong the test condition")
	}

	bbw := makeBtsBuf(t, model.SSlice{model.TAG_SRC_ID, "bbb"}, []string{"1", lngStr})
	err := wp.Init(bbw.Buf())
	if err != nil {
		t.Fatal("could not init BtBufWritePacket, err=", err)
	}

	if wp.GetDataReader().End() || !checkLogEvent(wp.GetDataReader().Get(), "1", string(tags)) {
		t.Fatal("Expected 1, but got ", string(wp.GetDataReader().Get()))
	}
	wp.GetDataReader().Next()
	if wp.GetDataReader().End() || !checkLogEvent(wp.GetDataReader().Get(), lngStr, string(tags)) {
		t.Fatal("Expected ", lngStr, ", but got ", string(wp.GetDataReader().Get()))
	}

	wp.GetDataReader().Next()
	if !wp.GetDataReader().End() {
		t.Fatal("End must be reached")
	}
}

func checkLogEvent(buf []byte, src, tags string) bool {
	var le model.LogEvent
	le.Unmarshal(buf)
	return string(le.Source()) == src && string(le.Tags()) == tags
}

func makeBtsBuf(t *testing.T, header model.SSlice, lines []string) *btsbuf.Writer {
	wr := NewWriter(&model.SimpleLogEventEncoder{})
	bf, err := wr.MakeBtsBuf(header, lines)
	if err != nil {
		t.Fatal("oops, could not initialize buffer writer")
	}

	var bbw btsbuf.Writer
	bbw.Reset(bf, true)
	return &bbw
}
