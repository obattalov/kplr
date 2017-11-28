package journal

import (
	"errors"
	"fmt"
	"sync"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/container"
	"github.com/kplr-io/journal"
)

type (
	JournalConfig struct {
		Dir string
	}

	Controller struct {
		dir      string
		lock     sync.Mutex
		journals map[string]*Journal
		logger   log4g.Logger
		shtdwn   bool
	}

	Journal struct {
		jctrlr *Controller
		lock   sync.Mutex
		ready  chan bool
		jid    string
		dir    string
		jrnl   *journal.Journal
		logger log4g.Logger
	}
)

func NewJournalCtrlr(jcfg *JournalConfig) *Controller {
	jc := new(Controller)
	jc.journals = make(map[string]*Journal)
	jc.dir = jcfg.Dir
	jc.logger = log4g.GetLogger("journal.Controller")
	jc.logger.Info("Just created dir=", jc.dir)
	return jc
}

func (jc *Controller) DiPhase() int {
	return 0
}

func (jc *Controller) DiInit() error {
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

func (jc *Controller) DiShutdown() {
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
	jc.journals = make(map[string]*Journal)
}

func (jc *Controller) String() string {
	return fmt.Sprint("journal.Controller{dir=", jc.dir, "}")
}

func (jc *Controller) GetJournal(jid string) (*Journal, error) {
	jc.lock.Lock()
	if jc.shtdwn {
		jc.lock.Unlock()
		return nil, errors.New("JournalCtrlr already stopped")
	}

	var err error
	jrnl, ok := jc.journals[jid]
	if !ok {
		if err == nil {
			jrnl, err = jc.newJournal(jid)
		}
	}
	jc.lock.Unlock()
	return jrnl, err
}

func (jc *Controller) newJournal(jid string) (*Journal, error) {
	jdir, err := journalPath(jc.dir, jid)
	if err != nil {
		jc.logger.Error("Could not create dir err=", err)
		return nil, err
	}
	jrnl := newJournal(jc, jdir, jid)
	jc.journals[jid] = jrnl
	return jrnl, nil
}

func (jc *Controller) onInitError(jrnl *Journal) {
	jc.lock.Lock()
	defer jc.lock.Unlock()

	delete(jc.journals, jrnl.jid)
}

func newJournal(jc *Controller, dir, jid string) *Journal {
	j := new(Journal)
	j.ready = make(chan bool)
	j.dir = dir
	j.jid = jid
	j.jctrlr = jc
	j.logger = log4g.GetLogger("ppln.Journal").WithId("{" + jid + "}").(log4g.Logger)
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

func (j *Journal) shutdown() {
	j.lock.Lock()
	defer j.lock.Unlock()

	if j.jrnl == nil {
		j.logger.Warn("Could not shutdown, journal was not initialized properly")
		return
	}
	j.jrnl.Close()
}

func (j *Journal) Write(bbi *container.BtsBufIterator) error {
	<-j.ready
	j.lock.Lock()
	jrnl := j.jrnl
	j.lock.Unlock()

	if jrnl == nil {
		j.logger.Warn("Write(): found the journal could not be properly initialized.")
		return errors.New("Could not open the journal")
	}

	_, err := jrnl.Write(bbi, nil)

	return err
}
