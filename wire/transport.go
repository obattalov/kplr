package wire

import (
	"io"
	"time"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/container"
	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/mpool"
	"github.com/kplr-io/zebra"
)

type (
	TransportConfig struct {
		ListenAddress  string
		SessTimeoutSec int
		MemPool        mpool.Pool
		ModelDesc      model.Descriptor
		JrnlController journal.Controller
	}

	Transport struct {
		logger    log4g.Logger
		zserver   io.Closer
		memPool   mpool.Pool
		mdesc     model.Descriptor
		jrnlCtrlr journal.Controller
	}
)

func NewTransport(tcfg *TransportConfig) (*Transport, error) {
	var err error
	t := new(Transport)
	t.memPool = tcfg.MemPool
	t.logger = log4g.GetLogger("wire.Transport")
	t.mdesc = tcfg.ModelDesc
	t.jrnlCtrlr = tcfg.JrnlController

	var scfg zebra.ServerConfig
	scfg.ListenAddress = tcfg.ListenAddress
	scfg.SessTimeoutMs = int(time.Duration(tcfg.SessTimeoutSec) * time.Second / time.Millisecond)
	scfg.Auth = noAuthFunc
	scfg.ConnListener = t
	t.zserver, err = zebra.NewTcpServer(&scfg)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Transport) Shutdown() {
	t.logger.Info("Shutting down")
	t.zserver.Close()
}

// ------------------------- zebra.ServerListener ----------------------------
func (t *Transport) OnRead(r zebra.Reader, n int) error {
	buf := t.memPool.GetBtsBuf(n)
	defer t.memPool.ReleaseBtsBuf(buf)

	rd, err := r.Read(buf)
	if rd != n || err != nil {
		t.logger.Error("Could not read ", n, " bytes for ", r, " to the buffer, err=", err)
		return err
	}

	var header [10]interface{}
	var offs int
	meta := model.Event(header[:])
	offs, err = model.UnmarshalEvent(t.mdesc.EventGroupMeta(), buf, meta)
	if err != nil {
		t.logger.Error("Could not unmarshal header err=", err)
		return err
	}

	jid := t.mdesc.GetJournalId(meta)
	var jrnl *journal.Journal
	jrnl, err = t.jrnlCtrlr.GetJournal(jid)
	if err != nil {
		t.logger.Error("Could not get journal by jid=", jid, ", err=", err)
		return err
	}

	var bbi container.BtsBufIterator
	err = bbi.Reset(buf[offs:])
	if err != nil {
		t.logger.Error("Unexpected data: err=", err)
		return err
	}

	err = jrnl.Write(&bbi)
	if err != nil {
		t.logger.Error("Could not store data to journal ", jid, ", err=", err)
		return err
	}

	return r.ReadResponse(nil)
}

func noAuthFunc(aKey, sKey string) bool {
	return true
}
