package wire

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/container/btsbuf"
	kjrnl "github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/mpool"
	"github.com/kplr-io/kplr/storage"
	"github.com/kplr-io/zebra"
)

type (
	TransportConfig struct {
		ListenAddress  string
		SessTimeoutSec int
	}

	Transport struct {
		MemPool   mpool.Pool         `inject:"mPool"`
		Table     storage.Table      `inject:"table"`
		JrnlCtrlr journal.Controller `inject:""`

		logger  log4g.Logger
		zserver io.Closer
		tcfg    TransportConfig
	}
)

func NewTransport(tcfg *TransportConfig) *Transport {
	t := new(Transport)
	t.logger = log4g.GetLogger("wire.Transport")
	t.tcfg = *tcfg
	return t
}

func (t *Transport) DiPhase() int {
	return 100
}

func (t *Transport) DiInit() error {
	var scfg zebra.ServerConfig
	scfg.ListenAddress = t.tcfg.ListenAddress
	scfg.SessTimeoutMs = int(time.Duration(t.tcfg.SessTimeoutSec) * time.Second / time.Millisecond)
	scfg.Auth = noAuthFunc
	scfg.ConnListener = t

	var err error
	t.zserver, err = zebra.NewTcpServer(&scfg)
	if err != nil {
		return err
	}
	return nil
}

func (t *Transport) DiShutdown() {
	t.logger.Info("Shutting down")
	if t.zserver != nil {
		t.zserver.Close()
	}
}

func (t *Transport) String() string {
	return fmt.Sprint("Transport:{ListenOn=", t.tcfg.ListenAddress, ", sessTOSec=", t.tcfg.SessTimeoutSec, "}")
}

// ------------------------- zebra.ServerListener ----------------------------
func (t *Transport) OnRead(r zebra.Reader, n int) error {
	buf := t.MemPool.GetBtsBuf(n)
	defer t.MemPool.ReleaseBtsBuf(buf)

	rd, err := r.Read(buf)
	if rd != n || err != nil {
		t.logger.Error("Could not read ", n, " bytes for ", r, " to the buffer, err=", err)
		return err
	}

	var bbi btsbuf.Reader
	err = bbi.Reset(buf)
	if err != nil {
		t.logger.Error("Unexpected data: err=", err)
		return err
	}

	if bbi.End() {
		t.logger.Error("At least header is expected, but got empty bytes buffer")
		return errors.New("empty list")
	}

	var sArr [20]string
	ss := model.SSlice(sArr[:])
	ss, _, err = model.UnmarshalSSlice(ss, bbi.Get())
	if err != nil {
		t.logger.Error("Could not unmarshal header err=", err)
		return err
	}

	//	var header [10]interface{}
	//	meta := model.Event(header[:])
	//	_, err = model.UnmarshalEvent(t.ModelDesc.EventGroupMeta(), bbi.Get(), meta)
	//	if err != nil {
	//		t.logger.Error("Could not unmarshal header err=", err)
	//		return err
	//	}

	//	jid := t.ModelDesc.GetJournalId(meta)
	var jrnl kjrnl.Writer
	jrnl, err = t.JrnlCtrlr.GetWriter(jid)
	if err != nil {
		t.logger.Error("Could not get journal by jid=", jid, ", err=", err)
		return err
	}

	bbi.Next()
	_, err = jrnl.Write(&bbi)
	if err != nil {
		t.logger.Error("Could not store data to journal ", jid, ", err=", err)
		return err
	}

	return r.ReadResponse(nil)
}

// parseHeader - parses header, updates tags index in the table and returns
// journal Id, or returns an error if any
func (t *Transport) parseHeader(ss model.SSlice) (string, error) {
	if len(ss) < 2 {
		return "", errors.New("Expecting at least one pair - source id in tags of the header.")
	}
	if len(ss)&1 == 1 {
		return "", errors.New("header must contain even number of strings(key:value pairs)")
	}

	// Sorting keys, before inserting them into the table
	var sArr [20]string
	srtKeys = sArr[:0]
	m := make(map[string]string, len(ss))
	for i := 0; i < len(ss); i += 2 {
		tag := ss[i]
		m[tag] = ss[i+1]
		idx := sort.SearchStrings(srtKeys, tag)
		srtKeys = append(srtKeys, key)
		if idx < len(srtKeys)-1 {
			copy(srtKeys[idx+1:], srtKeys[idx:])
		}
		srtKeys[idx] = key
	}

	srcId, ok := model.CopyString(m[model.TAG_SRC_ID])
	if !ok {
		return "", errors.New("No expected tag " + model.TAG_SRC_ID + " in the header.")
	}
	m[model.TAG_SRC_ID] = srcId
	return srcId, t.Table.Upsert(srtKeys, m)
}

func noAuthFunc(aKey, sKey string) bool {
	return true
}
