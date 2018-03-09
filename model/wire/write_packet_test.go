package wire

import (
	"reflect"
	"testing"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/kplr/model"
)

func TestBtBufWritePacket(t *testing.T) {
	tags := model.TagLine("a=a|b=b")

	bbw := makeBtsBuf(t, "j1", tags, []string{"1", "2"})
	var wp BtBufWritePacket
	err := wp.Init(bbw.Buf())
	if err != nil {
		t.Fatal("could not init BtBufWritePacket, err=", err)
	}

	if !reflect.DeepEqual(wp.GetTags(), tags) {
		t.Fatal("GetTagNames is not expected", wp.GetTags())
	}

	if wp.GetSourceId() != "j1" {
		t.Fatal("Expecting source=j1, but ", wp.GetSourceId())
	}

	if wp.GetDataReader().End() || !checkLogEvent(wp.GetDataReader().Get(), "1", tags) {
		t.Fatal("Expected 1, but got ", string(wp.GetDataReader().Get()))
	}
	wp.GetDataReader().Next()
	if wp.GetDataReader().End() || !checkLogEvent(wp.GetDataReader().Get(), "2", model.TagLine("")) {
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

	bbw := makeBtsBuf(t, "j1", model.TagLine(""), []string{"a", "b"})
	err = wp.Init(bbw.Buf())
	if err == nil {
		t.Fatal("could init BtBufWritePacket, but should be an error, the wrong header")
	}

	bbw = makeBtsBuf(t, "j1", model.TagLine("a=aaa"), []string{})
	err = wp.Init(bbw.Buf())
	if err == nil {
		t.Fatal("could init BtBufWritePacket, but should be an error, the empty data")
	}

	bbw = makeBtsBuf(t, "", model.TagLine("asdfasd"), []string{"a", "b"})
	err = wp.Init(bbw.Buf())
	if err == nil {
		t.Fatal("could init BtBufWritePacket, but should be an error, no journal is here as a key provided")
	}
}

func checkLogEvent(buf []byte, src string, tags model.TagLine) bool {
	var le model.LogEvent
	le.Unmarshal(buf)
	return string(le.GetMessage()) == src && le.GetTagLine() == tags
}

func makeBtsBuf(t *testing.T, src string, tl model.TagLine, lines []string) *btsbuf.Writer {
	enc := &model.SimpleLogEventEncoder{}
	enc.SetTags(tl)
	wr := NewWriter(enc)
	bf, err := wr.MakeBtsBuf(src, lines)
	if err != nil {
		t.Fatal("oops, could not initialize buffer writer")
	}

	var bbw btsbuf.Writer
	bbw.Reset(bf, true)
	return &bbw
}
