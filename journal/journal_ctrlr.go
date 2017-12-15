package journal

import (
	"errors"
	"fmt"
	"sync"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/journal"
)

type (
	JournalConfig struct {
		Dir string
	}

	Controller interface {
		GetReader(jid string) (journal.Reader, error)
		GetWriter(jid string) (journal.Writer, error)
	}

	controller struct {
		dir      string
		lock     sync.Mutex
		journals map[string]*jrnl_wrap
		logger   log4g.Logger
		shtdwn   bool
	}

	jrnl_wrap struct {
		jctrlr *controller
		lock   sync.Mutex
		ready  chan bool
		jid    string
		dir    string
		jrnl   *journal.Journal
		logger log4g.Logger
	}
)

func NewJournalCtrlr(jcfg *JournalConfig) Controller {
	jc := new(controller)
	jc.journals = make(map[string]*jrnl_wrap)
	jc.dir = jcfg.Dir
	jc.logger = log4g.GetLogger("journal.Controller")
	jc.logger.Info("Just created dir=", jc.dir)
	return jc
}

func (jc *controller) DiPhase() int {
	return 0
}

func (jc *controller) DiInit() error {
	jc.logger.Info("Initializing. Will scan ", jc.dir)
	jids, err := scanForJournals(jc.dir)
	if err != nil {
		jc.logger.Error("Could not scan the folder dir=", jc.dir, ", err=", err)
		return err
	}

	jc.lock.Lock()
	defer jc.lock.Unlock()

	jc.logger.Info(len(jids), " journals found by the scan procedure.")
	for _, jid := range jids {
		_, err := jc.newJournal(jid)
		if err != nil {
			jc.logger.Warn("Could not open journal ", jid, ", err=", err)
		}
	}
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
	for _, j := range jc.journals {
		j.shutdown()
	}
	jc.journals = make(map[string]*jrnl_wrap)
}

func (jc *controller) String() string {
	return fmt.Sprint("journal.Controller{dir=", jc.dir, "}")
}

func (jc *controller) GetReader(jid string) (journal.Reader, error) {
	jrnl, err := jc.getJournal(jid)
	if err != nil {
		return nil, err
	}

	var jr journal.JReader
	jrnl.InitReader(&jr)
	return &jr, nil
}

func (jc *controller) GetWriter(jid string) (journal.Writer, error) {
	jrnl, err := jc.getJournal(jid)
	if err != nil {
		return nil, err
	}
	return jrnl, nil
}

func (jc *controller) getJournal(jid string) (*journal.Journal, error) {
	jc.lock.Lock()
	if jc.shtdwn {
		jc.lock.Unlock()
		return nil, errors.New("JournalCtrlr already stopped")
	}

	var err error
	jw, ok := jc.journals[jid]
	if !ok {
		if err == nil {
			jw, err = jc.newJournal(jid)
		}
	}
	jc.lock.Unlock()
	return jw.get_journal()
}

func (jc *controller) newJournal(jid string) (*jrnl_wrap, error) {
	jdir, err := journalPath(jc.dir, jid)
	if err != nil {
		jc.logger.Error("Could not create dir err=", err)
		return nil, err
	}
	jrnl := newJournal(jc, jdir, jid)
	jc.journals[jid] = jrnl
	return jrnl, nil
}

func (jc *controller) onInitError(jrnl *jrnl_wrap) {
	jc.lock.Lock()
	defer jc.lock.Unlock()

	delete(jc.journals, jrnl.jid)
}

func newJournal(jc *controller, dir, jid string) *jrnl_wrap {
	j := new(jrnl_wrap)
	j.ready = make(chan bool)
	j.dir = dir
	j.jid = jid
	j.jctrlr = jc
	j.logger = log4g.GetLogger("journal.Journal").WithId("{" + jid + "}").(log4g.Logger)
	j.logger.Info("Just created")
	go func() {
		j.lock.Lock()
		defer j.lock.Unlock()
		defer close(j.ready)

		var err error
		jcfg := journal.NewDefaultJournalConfig(j.dir)
		jcfg.Id = j.jid
		j.jrnl, err = journal.NewJournal(jcfg)
		if err != nil {
			j.logger.Error("newJournal(): Could not open journal, err=", err)
			jc.onInitError(j)
		}
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
