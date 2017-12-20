package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/jrivets/log4g"
	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr"
	"github.com/kplr-io/kplr/index"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/wire"
	"github.com/kplr-io/kplr/mpool"
)

type (
	JournalConfig interface {
		GetJournalDir() string
		GetJournalChunkSize() int64
		GetJournalMaxSize() int64
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
		MPool   mpool.Pool      `inject:"mPool"`
		TTable  *index.TTable   `inject:"tTable"`
		JCfg    JournalConfig   `inject:"journalConfig"`
		MainCtx context.Context `inject:"mainCtx"`

		lock sync.Mutex

		//journals map[string]*jrnl_wrap
		journals *treemap.Map
		logger   log4g.Logger
		shtdwn   bool
	}

	jrnl_wrap struct {
		wLock sync.Mutex

		createdAt time.Time
		jctrlr    *controller
		lock      sync.Mutex
		ready     chan bool
		jid       string
		dir       string
		jrnl      *journal.Journal
		logger    log4g.Logger
		ctags     *chnk_tags
	}

	// jw_wrap_desc is used for persisiting jrnl_wrap state
	jw_wrap_desc struct {
		ChnksTags map[uint32]*chunk_desc
		CreatedAt kplr.ISO8601Time
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
		jc.logger.Debug("Awaiting journal ctags being initialized...")
		tags := make(map[string]string)
		for _, jw := range wrps {
			jw.add_ctags(tags)
		}
		jc.TTable.Append(tags)
		jc.logger.Debug("Done with ", len(wrps), " journal wrappers, ", len(tags), " tags were read")
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
	jrnl, err := jw.get_journal()
	if err != nil {
		return nil, err
	}

	ji := &JournalInfo{}
	ji.Created = time.Unix(jrnl.GetFCCT(), 0)
	ji.Modified = time.Unix(jrnl.GetFCMT(), 0)
	ji.Chunks = len(jrnl.GetChunks())
	ji.Size = jrnl.Size()
	ji.Path = jw.dir
	ji.Tags = jw.ctags.get_tags_as_slice()
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
	jw, jrnl, err := jc.getJournal(wp.GetSourceId())
	if err != nil {
		return err
	}

	jw.wLock.Lock()
	var chks [20]journal.RecordId
	_, cCnt, err := jrnl.Write(wp.GetDataReader(), chks[:])
	if err != nil {
		jw.wLock.Unlock()
		return err
	}

	jw.on_write(wp.GetTags(), chks[:cCnt])
	jw.wLock.Unlock()

	jc.TTable.Upsert(wp)
	return nil
}

func (jc *controller) Truncate(jid string, maxSize int64) (*TruncateResult, error) {
	if maxSize <= 0 {
		return nil, errors.New(fmt.Sprint("Expecting positive maxSize, but got maxSize=", maxSize))
	}

	jw, j, err := jc.getJournal(jid)
	if err != nil {
		return nil, err
	}

	sz := j.Size()
	tr := &TruncateResult{SizeBefore: sz, SizeAfter: sz}
	for sz > maxSize {
		if chkId := j.Truncate(); chkId > 0 {
			tr.ChksRemoved++
			jw.ctags.on_chnk_delete(chkId)
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
	jw, err := jc.getJournalWrapper(jid)
	if err != nil {
		return jw, nil, err
	}
	jrnl, err := jw.get_journal()
	return jw, jrnl, err
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
		if err == nil {
			jw, err = jc.newJournal(jid)
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
	jrnl := newJournal(jc, jdir, jid)
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
func newJournal(jc *controller, dir, jid string) *jrnl_wrap {
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
		jrnl, err := journal.NewJournal(jcfg)
		if err != nil {
			j.logger.Error("newJournal(): Could not open journal, err=", err)
			jc.onInitError(j)
			return
		}

		ctags := new_chnk_tags()
		var ctime time.Time
		dc, err := j.load_status()
		nonConsistent := false
		if err == nil {
			ctags.on_chnks_load(dc.ChnksTags)
			if !ctags.is_consistent(jrnl.GetChunks()) {
				nonConsistent = true
				j.logger.Warn("newJournal(): ctags is inconsisent, will re-build it.")
			}
			ctime = time.Time(dc.CreatedAt)
		} else {
			ctime = time.Unix(jrnl.GetFCCT(), 0)
		}

		if err != nil || nonConsistent {
			err = j.build_tags(jrnl, ctags)
			if err != nil {
				j.logger.Error("newJournal(): Could not build ctags list, err=", err)
				jc.onInitError(j)
				return
			}
			nonConsistent = true
		}

		j.lock.Lock()
		defer j.lock.Unlock()

		j.createdAt = ctime
		j.jrnl = jrnl
		j.ctags = ctags
		if nonConsistent {
			j.save_status()
		}
		j.logger.Info("Initialized successfully.")
	}()
	return j
}

// build_tags walking over the journal to update the chnk_tags collection
func (j *jrnl_wrap) build_tags(jrnl *journal.Journal, ctags *chnk_tags) error {
	j.logger.Debug("build_tags(): building tags")
	var buf [1024]byte
	jr := journal.JReader{}
	jrnl.InitReader(&jr)
	it := NewIterator(&jr, buf[:])
	var le model.LogEvent
	var curTags string
	var curRecId journal.RecordId
	for !it.End() {
		err := it.Get(&le)
		if err != nil {
			return err
		}

		recId := jr.GetCurrentRecordId()

		tags := model.WeakString(le.Tags())
		if string(tags) != curTags || recId.ChunkId != curRecId.ChunkId || recId.Offset > curRecId.Offset {
			curRecId = recId
			curTags = tags.String()
			if ctags.on_chnk_tags(curTags, curRecId) {
				j.logger.Debug("Adding new tags ", curTags, " for ", curRecId)
			}
		}
		it.Next()
	}
	return nil
}

// on_write updates tags for the list of chunks
func (j *jrnl_wrap) on_write(tags string, chks []journal.RecordId) {
	for _, chk := range chks {
		j.ctags.on_chnk_tags(tags, chk)
	}
}

func (j *jrnl_wrap) shutdown() {
	j.lock.Lock()
	defer j.lock.Unlock()

	j.save_status()

	if j.jrnl == nil {
		j.logger.Warn("Could not shutdown, journal was not initialized properly")
		return
	}
	j.jrnl.Close()
}

func (j *jrnl_wrap) get_journal() (*journal.Journal, error) {
	<-j.ready
	j.lock.Lock()
	jrnl := j.jrnl
	j.lock.Unlock()

	if jrnl == nil {
		j.logger.Warn("Write(): found the journal could not be properly initialized.")
		return nil, errors.New("Could not open the journal")
	}

	return jrnl, nil
}

func (j *jrnl_wrap) add_ctags(m map[string]string) {
	<-j.ready

	j.lock.Lock()
	if j.ctags == nil {
		j.logger.Error("Could not initialiaze journal wrapper for ", j.dir, " ignoring to add tags jid=", j.jid)
		j.lock.Unlock()
		return
	}
	jt := j.ctags.get_tags(j.jid)
	j.lock.Unlock()

	for tags, srcId := range jt {
		m[tags] = srcId
	}
}

// save_status - saves status of the journal to the disk, see jw_wrap_desc struct
func (j *jrnl_wrap) save_status() error {
	var dc jw_wrap_desc
	dc.ChnksTags = j.ctags.chnks
	dc.CreatedAt = kplr.ISO8601Time(j.createdAt)

	b, err := json.Marshal(&dc)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(j.get_state_filename(), b, 0644)
}

// load_status - reads status of the journal from the disk and update the journal
func (j *jrnl_wrap) load_status() (*jw_wrap_desc, error) {
	b, err := ioutil.ReadFile(j.get_state_filename())
	if err != nil {
		return nil, err
	}

	var dc jw_wrap_desc
	err = json.Unmarshal(b, &dc)
	if err != nil {
		return nil, err
	}

	return &dc, nil
}

func (j *jrnl_wrap) get_state_filename() string {
	return path.Join(j.dir, cJrnlStatusFileName)
}

// ================================= misc ====================================
func (tr *TruncateResult) String() string {
	return fmt.Sprint("{BeforeSize: ", kplr.FormatSize(tr.SizeBefore), ", AfterSize: ",
		kplr.FormatSize(tr.SizeAfter), ", ChunksRemoved: ", tr.ChksRemoved, "}")
}
