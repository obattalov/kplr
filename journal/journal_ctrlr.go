package journal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/jrivets/log4g"
	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr"
	"github.com/kplr-io/kplr/model/index"
	"github.com/kplr-io/kplr/model/wire"
	"github.com/kplr-io/kplr/mpool"
)

type (
	JournalConfig interface {
		GetJournalDir() string
		GetJournalChunkSize() int64
		GetJournalMaxSize() int64
		// GetJournalRecoveryOnIfError - Enables recovery if journal error happens
		GetJournalRecoveryOnIfError() bool
	}

	Controller interface {
		GetJournals() []string
		GetJournalInfo(jid string) (*JournalInfo, error)

		GetReader(jid string) (journal.Reader, error)

		// Write data to a journal, the WritePacket contains all needed information
		// for the operaion
		Write(wp wire.WritePacket) error

		// Truncate the journal to the desired size. The algorithm has the following
		// logic - oldest chunks of the journal will be deleted until the size
		// of the journal will reach size the maxSize or less.
		Truncate(jid string, maxSize int64) (*TruncateResult, error)
	}

	// Journal
	JournalInfo struct {
		Created  time.Time
		Modified time.Time
		Size     int64
		Path     string
		Chunks   int
		Tags     []string
	}

	TruncateResult struct {
		SizeBefore  int64
		SizeAfter   int64
		ChksRemoved int
	}

	controller struct {
		MPool   mpool.Pool        `inject:"mPool"`
		TIdxr   index.TagsIndexer `inject:"tIndexer"`
		JCfg    JournalConfig     `inject:"journalConfig"`
		MainCtx context.Context   `inject:"mainCtx"`

		lock sync.Mutex

		journals *treemap.Map
		logger   log4g.Logger
		shtdwn   bool
	}

	jrnl_wrap struct {
		createdAt time.Time
		jctrlr    *controller
		lock      sync.Mutex
		ready     chan bool
		jid       string
		dir       string
		jrnl      *journal.Journal
		logger    log4g.Logger
	}
)

const (
	cJrnlStatusFileName = "journal.meta"
	cSizeCheckTOSec     = 300
)

func NewJournalCtrlr() Controller {
	jc := new(controller)
	jc.journals = treemap.NewWithStringComparator()
	jc.logger = log4g.GetLogger("kplr.journal.Controller")
	return jc
}

func (jc *controller) DiPhase() int {
	return 0
}

func (jc *controller) DiInit() error {
	jc.logger.Info("Initializing, journal dir=", jc.JCfg.GetJournalDir(),
		", MaxChunkSize=", kplr.FormatSize(jc.JCfg.GetJournalChunkSize()),
		", MaxJournalSize=", kplr.FormatSize(jc.JCfg.GetJournalMaxSize()))
	dir := jc.JCfg.GetJournalDir()
	jids, err := scanForJournals(dir)
	if err != nil {
		jc.logger.Error("Could not scan the folder dir=", dir, ", err=", err)
		return err
	}

	jc.lock.Lock()
	defer jc.lock.Unlock()

	jc.logger.Info(len(jids), " journals found by the scan procedure.")
	wrps := make([]*jrnl_wrap, 0, len(jids))
	for _, jid := range jids {
		jw, err := jc.newJournal(jid)
		if err != nil {
			jc.logger.Warn("Could not open journal ", jid, ", err=", err)
		} else {
			wrps = append(wrps, jw)
		}
	}

	go func() {
		cs := &chkSynchronizer{jc, log4g.GetLogger("kplr.journal.Controller.chkSynchronizer")}
		cs.sync(wrps)
	}()

	go jc.sizeChecker()

	return nil
}

func (jc *controller) DiShutdown() {
	jc.lock.Lock()
	defer jc.lock.Unlock()

	if jc.shtdwn {
		jc.logger.Warn("Already was shut down")
		return
	}

	jc.shtdwn = true
	it := jc.journals.Iterator()
	for it.Begin(); it.Next(); {
		j := it.Value().(*jrnl_wrap)
		j.shutdown()
	}
	jc.journals.Clear()
}

func (jc *controller) String() string {
	return fmt.Sprint("journal.Controller{dir=", jc.JCfg.GetJournalDir(), "}")
}

func (jc *controller) GetJournals() []string {
	jc.lock.Lock()
	defer jc.lock.Unlock()

	res := make([]string, 0, jc.journals.Size())
	it := jc.journals.Iterator()
	for it.Begin(); it.Next(); {
		jid := it.Key().(string)
		res = append(res, jid)
	}

	return res
}

func (jc *controller) GetJournalInfo(jid string) (*JournalInfo, error) {
	jc.lock.Lock()
	defer jc.lock.Unlock()

	jwi, ok := jc.journals.Get(jid)
	if !ok {
		return nil, kplr.ErrNotFound
	}
	jw := jwi.(*jrnl_wrap)
	jrnl, err := jw.getJournal()
	if err != nil {
		return nil, err
	}

	ji := &JournalInfo{}
	ji.Created = time.Unix(jrnl.GetFCCT(), 0)
	ji.Modified = time.Unix(jrnl.GetFCMT(), 0)
	ji.Chunks = len(jrnl.GetChunks())
	ji.Size = jrnl.Size()
	ji.Path = jw.dir
	return ji, nil
}

func (jc *controller) GetReader(jid string) (journal.Reader, error) {
	_, jrnl, err := jc.getJournal(jid)
	if err != nil {
		return nil, err
	}

	var jr journal.JReader
	jrnl.InitReader(&jr)
	return &jr, nil
}

func (jc *controller) Write(wp wire.WritePacket) error {
	_, jrnl, err := jc.getJournal(wp.GetSourceId())
	if err != nil {
		return err
	}

	tgs, err := jc.TIdxr.UpsertTags(wp.GetTags())
	if err != nil {
		return err
	}
	wp.ApplyTagGroupId(tgs.GetId())

	_, recId, err := jrnl.WriteToChunk(wp.GetDataReader())
	if err != nil {
		return err
	}

	var rb index.RecordsBatch
	rb.TGroupId = tgs.GetId()
	rb.LastRecord.Journal = wp.GetSourceId()
	rb.LastRecord.RecordId = recId

	jc.TIdxr.OnRecords(&rb)
	return nil
}

func (jc *controller) Truncate(jid string, maxSize int64) (*TruncateResult, error) {
	if maxSize <= 0 {
		return nil, errors.New(fmt.Sprint("Expecting positive maxSize, but got maxSize=", maxSize))
	}

	_, j, err := jc.getJournal(jid)
	if err != nil {
		return nil, err
	}

	sz := j.Size()
	tr := &TruncateResult{SizeBefore: sz, SizeAfter: sz}
	for sz > maxSize {
		if chkId := j.Truncate(); chkId > 0 {
			tr.ChksRemoved++
		}
		sz = j.Size()
	}

	if tr.ChksRemoved > 0 {
		jc.logger.Info("Journal ", jid, " truncation: ", tr)
	} else {
		jc.logger.Debug("Journal ", jid, " truncation: ", tr)
	}

	return tr, nil
}

func (jc *controller) getJournal(jid string) (*jrnl_wrap, *journal.Journal, error) {
	// will do 2 checks just because we could come here for already broken journal
	// what could happen due to unsuccessful write
	for i := 0; i < 2; i++ {
		jw, err := jc.getJournalWrapper(jid)
		if err != nil {
			return jw, nil, err
		}

		jrnl, err := jw.getJournal()
		if err != nil {
			jc.onInitError(jw)
			continue
		}

		return jw, jrnl, err
	}
	return nil, nil, fmt.Errorf("Could not initialize wrapper and the journal for ", jid, " in 2 attempts. Giving up for now.")
}

func (jc *controller) getJournalWrapper(jid string) (*jrnl_wrap, error) {
	jc.lock.Lock()
	if jc.shtdwn {
		jc.lock.Unlock()
		return nil, errors.New("JournalCtrlr already stopped")
	}

	var err error
	var jw *jrnl_wrap
	jiw, ok := jc.journals.Get(jid)
	if !ok {
		jw, err = jc.newJournal(jid)
		if err == nil {
			go func() {
				cs := &chkSynchronizer{jc, log4g.GetLogger("kplr.journal.Controller.chkSynchronizer").
					WithId("{" + jid + "}").(log4g.Logger)}
				cs.syncJrnlWrapper0(jw)
			}()
		}
	} else {
		jw = jiw.(*jrnl_wrap)
	}
	jc.lock.Unlock()
	return jw, err
}

func (jc *controller) newJournal(jid string) (*jrnl_wrap, error) {
	jdir, err := journalPath(jc.JCfg.GetJournalDir(), jid)
	if err != nil {
		jc.logger.Error("Could not create dir err=", err)
		return nil, err
	}
	jrnl := newJournalWrapper(jc, jdir, jid)
	jc.journals.Put(jid, jrnl)
	return jrnl, nil
}

func (jc *controller) onInitError(jrnl *jrnl_wrap) {
	jc.lock.Lock()
	defer jc.lock.Unlock()

	jc.journals.Remove(jrnl.jid)
}

func (jc *controller) sizeChecker() {
	jc.logger.Info("Running sizeChecker")
	defer jc.logger.Info("Done with sizeChecker")
	for {
		select {
		case <-jc.MainCtx.Done():
			return
		case <-time.After(cSizeCheckTOSec * time.Second):
			jids := jc.GetJournals()
			szBefore := int64(0)
			szAfter := int64(0)
			chnks := 0
			jrnls := 0
			for _, jid := range jids {
				tr, err := jc.Truncate(jid, jc.JCfg.GetJournalMaxSize())
				if err != nil {
					jc.logger.Warn("Could not truncate journal, err=", err)
					continue
				}
				szBefore += tr.SizeBefore
				szAfter += tr.SizeAfter
				chnks += tr.ChksRemoved
				if tr.ChksRemoved > 0 {
					jrnls++
				}
			}
			if chnks == 0 {
				jc.logger.Info("No data rotation. ", len(jids), " journals known, total size is ", kplr.FormatSize(szBefore))
			} else {
				jc.logger.Info("Data rotation:\n\t", len(jids), " journals known,\n\t", jrnls, " journals were affected,\n\t",
					chnks, " chunks were removed,\n\t", kplr.FormatSize(szBefore-szAfter), " bytes were removed\n\t",
					kplr.FormatSize(szBefore), " bytes were BEFORE, and\n\t", kplr.FormatSize(szAfter), " bytes AFTER")
			}
		}
	}
}

// ============================== jrnl_wrap ==================================
func newJournalWrapper(jc *controller, dir, jid string) *jrnl_wrap {
	j := new(jrnl_wrap)
	j.ready = make(chan bool)
	j.dir = dir
	j.jid = jid
	j.jctrlr = jc
	j.logger = log4g.GetLogger("kplr.journal.Journal").WithId("{" + jid + "}").(log4g.Logger)
	j.logger.Debug("Just created")
	go func() {
		defer close(j.ready)

		jcfg := journal.NewDefaultJournalConfig(j.dir)
		jcfg.Id = j.jid
		jcfg.MaxChunkSize = jc.JCfg.GetJournalChunkSize()
		jcfg.RecoveryOnIfError = jc.JCfg.GetJournalRecoveryOnIfError()
		jrnl, err := journal.NewJournal(jcfg)
		if err != nil {
			j.logger.Error("newJournal(): Could not open journal, err=", err)
			jc.onInitError(j)
			return
		}

		j.lock.Lock()
		defer j.lock.Unlock()

		j.createdAt = time.Unix(jrnl.GetFCCT(), 0)
		j.jrnl = jrnl
		j.logger.Info("Initialized successfully.")
	}()
	return j
}

func (j *jrnl_wrap) shutdown() {
	j.lock.Lock()
	defer j.lock.Unlock()

	if j.jrnl == nil {
		j.logger.Warn("Could not shutdown, journal was not initialized properly")
		return
	}
	j.jrnl.Close()
}

// getJournal() returns kplr journal from the journal wrapper
func (j *jrnl_wrap) getJournal() (*journal.Journal, error) {
	<-j.ready
	j.lock.Lock()
	jrnl := j.jrnl
	j.lock.Unlock()

	if jrnl == nil {
		j.logger.Warn("getJournal(): found the journal could not be properly initialized.")
		return nil, errors.New("Could not open the journal")
	}
	return jrnl, jrnl.GetError()
}

// ================================= misc ====================================
func (tr *TruncateResult) String() string {
	return fmt.Sprint("{BeforeSize: ", kplr.FormatSize(tr.SizeBefore), ", AfterSize: ",
		kplr.FormatSize(tr.SizeAfter), ", ChunksRemoved: ", tr.ChksRemoved, "}")
}
