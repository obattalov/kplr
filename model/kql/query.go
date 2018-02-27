package kql

import (
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/index"
)

type (
	// tagGroupDesc struct is a helper, which is used for caching tags check results
	tagGroupDesc struct {
		// chkRes contains result of tgChkFunc to the tags group
		chkRes bool
		// tags contains the tags for the group
		tags *model.Tags
	}

	Query struct {
		idxer   index.TagsIndexer
		tgCache map[int64]*tagGroupDesc

		// tags check pair - valuator and function
		tgChkValuator queryExpValuator
		tgChkFunc     *expFuncDesc

		// records valuation
		recChkValuator queryExpValuator
		recsChkFunc    *expFuncDesc

		// source journals
		jrnls []string
	}

	queryExpValuator struct {
		le   *model.LogEvent
		tags *model.Tags
	}
)

func Compile(kQuery string, idxer index.TagsIndexer) (*Query, error) {
	s, err := Parse(kQuery)
	if err != nil {
		return nil, err
	}

	qry := new(Query)
	qry.idxer = idxer
	qry.tgCache = make(map[int64]*tagGroupDesc)
	qry.tgChkFunc, err = evaluate(s.Where, &qry.tgChkValuator, cExpValMsgIgnore|cExpValTsIgnore)
	if err != nil {
		return nil, err
	}

	qry.recsChkFunc, err = evaluate(s.Where, &qry.recChkValuator, 0)
	if err != nil {
		return nil, err
	}

	if qry.tgChkFunc.typ == cFTIgnore {
		qry.jrnls = idxer.GetAllJournals()
	} else {
		m := make(map[string]bool)
		idxer.Visit(func(td *index.TagsDesc) bool {
			qry.tgChkValuator.tags = td.Tags
			tgd := &tagGroupDesc{qry.tgChkFunc.fn(), td.Tags}
			qry.tgCache[td.Tags.GetId()] = tgd
			if tgd.chkRes {
				for j := range td.Journals {
					m[j] = true
				}
			}
			return true
		})

		qry.jrnls = make([]string, 0, len(m))
		for j := range m {
			qry.jrnls = append(qry.jrnls, j)
		}
	}

	return qry, nil
}

// Filter returns true if the event le must be filtered
func (q *Query) Filter(le *model.LogEvent) bool {
	gid := le.GetTGroupId()
	tgd, ok := q.tgCache[gid]
	if !ok {
		td := q.idxer.GetTagsDesc(gid)
		if td != nil {
			q.tgChkValuator.tags = td.Tags
			tgd = &tagGroupDesc{q.tgChkFunc.fn(), td.Tags}
			q.tgCache[gid] = tgd
		}
	}

	if tgd != nil {
		if q.tgChkFunc.typ != cFTIgnore && !tgd.chkRes {
			// must be filtered
			return true
		}
		q.recChkValuator.tags = tgd.tags
	} else {
		q.recChkValuator.tags = nil
	}

	q.recChkValuator.le = le
	return !q.recsChkFunc.fn()
}

// tagGroupDesc provide expValuator to check tags
func (qv *queryExpValuator) timestamp() uint64 {
	return uint64(qv.le.GetTimestamp())
}

func (qv *queryExpValuator) msg() model.WeakString {
	return qv.le.GetMessage()
}

func (qv *queryExpValuator) tagVal(tag string) string {
	if qv.tags == nil {
		return ""
	}
	return qv.tags.GetValue(tag)
}
