// tidx package contains TagsIndexer implementation
package tidx

import (
	"fmt"
	"sync"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/kplr"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/index"
)

type (

	// inMemTagIndex struct contains in-memory TagIndexer implementation
	inMemTagIndex struct {
		lock   sync.Mutex
		logger log4g.Logger
		tlines map[model.TagLine]int64
		ttable map[int64]*index.TagsDesc
	}
)

func NewInMemTagIndex() index.TagsIndexer {
	mti := new(inMemTagIndex)
	mti.logger = log4g.GetLogger("kplr.index.inMemTagIndex")
	mti.tlines = make(map[model.TagLine]int64)
	mti.ttable = make(map[int64]*index.TagsDesc)
	return mti
}

func (mti *inMemTagIndex) UpsertTags(tl model.TagLine) (*model.Tags, error) {
	mti.lock.Lock()
	defer mti.lock.Unlock()

	tid, ok := mti.tlines[tl]
	if !ok {
		tid = kplr.NextId64()
		tags, err := tl.NewTags(tid)
		if err != nil {
			return nil, err
		}

		mti.logger.Debug("Creating new tags record tid=", tid, " for tags=", tl)
		td := &index.TagsDesc{Tags: &tags, Journals: make(map[string]index.ChunkDescs, 5)}
		mti.tlines[tl] = tid
		mti.ttable[tid] = td
		return &tags, nil
	}

	return mti.ttable[tid].Tags, nil
}

func (mti *inMemTagIndex) OnRecords(rb *index.RecordsBatch) error {
	mti.lock.Lock()
	defer mti.lock.Unlock()

	td, ok := mti.ttable[rb.TGroupId]
	if !ok {
		return fmt.Errorf("OnRecords: Unexpected tid=%d", rb.TGroupId)
	}

	jrnl, ok := td.Journals[rb.LastRecord.Journal]
	if !ok {
		mti.logger.Debug("Creating new journal record for journal=", rb.LastRecord.Journal, ", tags=", td.Tags.GetTagLine())
		jrnl = make(index.ChunkDescs, 3)
		td.Journals[rb.LastRecord.Journal] = jrnl
	}

	cd, ok := jrnl[rb.LastRecord.RecordId.ChunkId]
	if !ok {
		mti.logger.Debug("Creating new chunk record for journal=", rb.LastRecord.Journal, ", chunk=", rb.LastRecord.RecordId.ChunkId, ", tags=", td.Tags.GetTagLine())
		cd = new(index.ChunkDesc)
		jrnl[rb.LastRecord.RecordId.ChunkId] = cd
	}
	cd.LastRecord = rb.LastRecord.RecordId
	return nil
}

func (mti *inMemTagIndex) OnDelete(jName string, cid uint32) error {
	mti.lock.Lock()
	defer mti.lock.Unlock()

	// not optimized, but it is ok so far because it is very rear
	for tid, td := range mti.ttable {
		if jrnl, ok := td.Journals[jName]; ok {
			if _, ok := jrnl[cid]; ok {
				delete(jrnl, cid)
				if len(jrnl) == 0 {
					mti.logger.Debug("Deleting tags=", &td.Tags, " completely, because no other chunks for it. Deleted journal=", jName, ", chunk=", cid)
					delete(mti.ttable, tid)
					delete(mti.tlines, td.Tags.GetTagLine())
				} else {
					mti.logger.Debug("Deleting tags=", &td.Tags, " for journal=", jName, ", chunk=", cid, ". Still have ", len(jrnl), " chunks with the tags")
				}
			}
		}
	}

	return nil
}

// TODO: implement me right way
func (mti *inMemTagIndex) GetAllJournals() []string {
	mti.lock.Lock()
	defer mti.lock.Unlock()

	m := make(map[string]bool, len(mti.ttable))
	for _, tgs := range mti.ttable {
		for jrnl := range tgs.Journals {
			m[jrnl] = true
		}
	}

	res := make([]string, 0, len(m))
	for j := range m {
		res = append(res, j)
	}
	return res
}

func (mti *inMemTagIndex) GetTagsDesc(tgid int64) *index.TagsDesc {
	mti.lock.Lock()
	defer mti.lock.Unlock()

	return mti.ttable[tgid]
}

func (mti *inMemTagIndex) Visit(visitor index.TagsIndexerVisitor) {
	mti.lock.Lock()
	defer mti.lock.Unlock()

	for _, td := range mti.ttable {
		if !visitor(td) {
			return
		}
	}
}
