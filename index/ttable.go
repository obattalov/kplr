package index

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/wire"
)

type (
	// TTable is a structure which describes Tags table which is used for selecting
	// journal by tags (sorted by key string)
	TTable struct {
		// The flag indicates Append was called or not.
		init bool
		lock sync.Mutex

		// tags contain mapping of knwon tags to source Id
		tags map[string]string
		// set of known tags
		knwnTags atomic.Value

		logger log4g.Logger
	}
)

var (
	ErrNotInitialized = errors.New("cannot perform request, the index table is not initialied yet.")
	ErrMaxExcceded    = errors.New("The number of sources exceeds maximum value provided ")
)

func NewTTable() *TTable {
	tt := new(TTable)
	tt.tags = make(map[string]string)
	kt := make(map[string]bool)
	tt.knwnTags.Store(kt)
	tt.logger = log4g.GetLogger("index.TTable")
	return tt
}

// Append adds new tags to TTable. The behaviour allows to execute
// upserts before the initialization happens
func (tt *TTable) Append(m map[string]string) {
	tt.lock.Lock()
	defer tt.lock.Unlock()

	om := tt.GetKnownTags()
	nm := make(map[string]bool)

	for k := range om {
		nm[k] = true
	}

	tt.init = true
	for tags, srcId := range m {
		t := model.Tags(tags)
		ta := t.GetTagNames()
		tt.addTagsToMap(ta, nm)
		tt.tags[tags] = srcId
		tt.logger.Debug("Adding ", t, " tags for ", srcId, " journal")
	}

	tt.logger.Info("New known keys after Append: ", nm)
	tt.knwnTags.Store(nm)
	tt.logger.Info(len(m), " different tags have been just appended")
}

// Add new record to a journal tags list. The calls will be successfull whether
// Init was called or not.
func (tt *TTable) Upsert(wp wire.WritePacket) {
	tt.lock.Lock()

	tags := wp.GetTags()
	if _, ok := tt.tags[tags]; ok {
		tt.lock.Unlock()
		return
	}

	kt := tt.GetKnownTags()
	for k, _ := range wp.GetTagsMap() {
		if _, ok := kt[string(k)]; !ok {
			tt.updateKnwnTags(wp.GetTagsMap())
			break
		}
	}

	// Save copies of the weak string here
	tt.tags[tags] = wp.GetSourceId()
	tt.logger.Debug("New tags: ", tags)
	tt.lock.Unlock()
}

// GetSrcId returns source Ids based on the Expression provided. It will stop
// search for the sources if the number of matched exceeds the maxAllowed.
func (tt *TTable) GetSrcId(ed *model.ExprDesc, maxAllowed int) ([]string, error) {
	tt.lock.Lock()
	defer tt.lock.Unlock()

	if !tt.init {
		return nil, ErrNotInitialized
	}

	var le model.LogEvent
	m := make(map[string]string)
	cnt := 0
	for tags, srcId := range tt.tags {
		if _, ok := m[srcId]; ok {
			continue
		}

		le.Reset(0, "", model.Tags(tags))
		if ed.Expr(&le) {
			m[srcId] = srcId
			cnt++
			if cnt > maxAllowed {
				return nil, ErrMaxExcceded
			}
		}
	}

	res := make([]string, 0, len(m))
	for _, sid := range m {
		res = append(res, sid)
	}

	return res, nil
}

// GetKnownTags - returns set of
func (tt *TTable) GetKnownTags() map[string]bool {
	return tt.knwnTags.Load().(map[string]bool)
}

func (tt *TTable) addTagsToMap(tags []model.WeakString, nm map[string]bool) {
	for _, t := range tags {
		if _, ok := nm[string(t)]; !ok {
			nm[t.String()] = true
		}
	}
}

func (tt *TTable) updateKnwnTags(m map[model.WeakString]model.WeakString) {
	om := tt.GetKnownTags()
	nm := make(map[string]bool)

	for k := range om {
		nm[k] = true
	}

	for k := range m {
		nm[k.String()] = true
	}

	tt.logger.Info("New known keys: ", nm)
	tt.knwnTags.Store(nm)
}
