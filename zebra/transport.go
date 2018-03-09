package zebra

import (
	"fmt"
	"io"
	"time"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model/index"
	"github.com/kplr-io/kplr/model/wire"
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
		Table     index.TagsIndexer  `inject:"tIndexer"`
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
	t.zserver, err = zebra.NewServer(&scfg)
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

	var bbwp wire.BtBufWritePacket
	err = bbwp.Init(buf)
	if err != nil {
		t.logger.Error("Could not initialize WritePacket err=", err)
		return err
	}

	err = t.JrnlCtrlr.Write(&bbwp)
	if err != nil {
		t.logger.Error("Could not store data, err=", err)
		return err
	}

	return r.ReadResponse(nil)
}

func noAuthFunc(aKey, sKey string) bool {
	return true
}
