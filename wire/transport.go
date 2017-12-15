package wire

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/container/btsbuf"
	kjrnl "github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/mpool"
	"github.com/kplr-io/zebra"
)

type (
	TransportConfig struct {
		ListenAddress  string
		SessTimeoutSec int
	}

	Transport struct {
		MemPool   mpool.Pool         `inject:"mPool"`
		ModelDesc model.Descriptor   `inject:"mdlDesc"`
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

	var header [10]interface{}
	meta := model.Event(header[:])
	_, err = model.UnmarshalEvent(t.ModelDesc.EventGroupMeta(), bbi.Get(), meta)
	if err != nil {
		t.logger.Error("Could not unmarshal header err=", err)
		return err
	}

	jid := t.ModelDesc.GetJournalId(meta)
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

func noAuthFunc(aKey, sKey string) bool {
	return true
}
