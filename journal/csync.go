package journal

import (
	"sync"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/index"
)

type (
	chkSynchronizer struct {
		jc     *controller
		logger log4g.Logger
	}

	idxJrnlDesc map[string]index.ChunkDescs
)

func (cs *chkSynchronizer) sync(wrprs []*jrnl_wrap) {
	cs.logger.Info("Checking tags for ", len(wrprs), " journals")
	defer cs.logger.Info("Done checking tags for journals")

	var wg sync.WaitGroup
	ijd := cs.buildStatus()
	for _, w := range wrprs {
		cds, ok := ijd[w.jid]
		if !ok {
			cds = make(index.ChunkDescs)
			ijd[w.jid] = cds
		}

		wg.Add(1)
		go func(w *jrnl_wrap, cds index.ChunkDescs) {
			defer wg.Done()
			cs.syncJrnlWrapper(w, cds)
		}(w, cds)
	}

	wg.Wait()
}

func (cs *chkSynchronizer) syncJrnlWrapper0(w *jrnl_wrap) {
	ijd := cs.buildJrnlStatus(w.jid)
	cds, ok := ijd[w.jid]
	if !ok {
		cds = make(index.ChunkDescs)
	}
	cs.syncJrnlWrapper(w, cds)
}

// syncJrnlWrapper checks journal wrapper, and update the index if needed.
// cds contains index information regarding the journal.
func (cs *chkSynchronizer) syncJrnlWrapper(w *jrnl_wrap, cds index.ChunkDescs) {
	j, err := w.getJournal()
	if err != nil {
		cs.logger.Warn("syncJrnlWrapper(): Could not run, err=", err)
		return
	}

	cks := j.GetChunks()
	for _, c := range cks {
		cid := c.GetLastRecordId().ChunkId
		lr := c.GetLastRecordId()
		c0, ok := cds[cid]
		if !ok {
			c0 = new(index.ChunkDesc)
			cds[cid] = c0
		}

		if c0.LastRecord == lr {
			delete(cds, lr.ChunkId)
		}
	}

	cs.syncChunks(w.jid, j, cds)
}

func (cs *chkSynchronizer) syncChunks(jid string, jrnl *journal.Journal, cds index.ChunkDescs) error {
	var buf [1024]byte
	jr := journal.JReader{}
	jrnl.InitReader(&jr)
	it := NewIterator(&jr, buf[:])
	var le model.LogEvent
	m := make(map[model.TagLine]journal.RecordId)
	var recs int64
	for cid := range cds {
		it.SetCurrentPos(journal.RecordId{cid, 0})
		var tl model.TagLine
		var lr journal.RecordId
		for !it.End() {
			recs++
			err := it.Get(&le)
			if err != nil {
				return err
			}

			tl1 := le.GetTagLine()
			if len(tl1) != 0 {
				if len(tl) != 0 {
					m[tl] = lr
				}
				tl = tl1
			}

			lr0 := it.GetCurrentPos()
			if lr0.ChunkId != cid {
				break
			}

			lr = lr0
			it.Next()
		}

		if len(tl) != 0 {
			m[tl] = lr
		}
	}

	// update idx
	for tl, lr := range m {
		tags, err := cs.jc.TIdxr.UpsertTags(tl)
		if err != nil {
			return err
		}

		var rb index.RecordsBatch
		rb.TGroupId = tags.GetId()
		rb.LastRecord.Journal = jid
		rb.LastRecord.RecordId = lr

		err = cs.jc.TIdxr.OnRecords(&rb)
		if err != nil {
			return err
		}
	}

	cs.logger.Info("syncChunks(): jrnl=", jid, ", ", len(m), " tags found and updated, ", recs, " records checked.")
	return nil
}

// buildStatus walks over tag indexes and provides a map of journals
// with latest record with every chunk. This is what the index knows.
func (cs *chkSynchronizer) buildStatus() idxJrnlDesc {
	res := make(idxJrnlDesc)
	cs.jc.TIdxr.Visit(func(td *index.TagsDesc) bool {
		res.updateAll(td.Journals)
		return true
	})
	return res
}

// buildJrnlStatus walks over tag indexs and provides a map of tags for the
// requested journal.
func (cs *chkSynchronizer) buildJrnlStatus(jid string) idxJrnlDesc {
	res := make(idxJrnlDesc)
	cs.jc.TIdxr.Visit(func(td *index.TagsDesc) bool {
		cds, ok := td.Journals[jid]
		if !ok {
			return true
		}
		res.updateJournalOnly(jid, cds)
		return true
	})
	return res
}

// updateAll adds status of journals from small to ijd
func (ijd *idxJrnlDesc) updateAll(small idxJrnlDesc) {
	for j, cds := range small {
		ijd.updateJournalOnly(j, cds)
	}
}

func (ijd *idxJrnlDesc) updateJournalOnly(j string, cds index.ChunkDescs) {
	ds, ok := (*ijd)[j]
	if !ok {
		ds = make(index.ChunkDescs)
		(*ijd)[j] = ds
	}

	for c, d := range cds {
		d0, ok := ds[c]
		if !ok {
			d0 = new(index.ChunkDesc)
			ds[c] = d0
		}
		if d0.LastRecord.CmpLE(&d.LastRecord) {
			*d0 = *d
		}
	}

}
