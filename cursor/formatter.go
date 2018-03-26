package cursor

import (
	"strconv"
	"time"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/kql"
)

type (
	// curFormatter struct is a helper for formatting a cursor output
	curFormatter struct {
		c     *cur
		cnctr btsbuf.Concatenator
		fmtr  *kql.Formatter
	}

	// curFmtF cursor formatter function
	curFmtF func() []byte
)

const (
	cFmtStringToken = iota
	cFmtVarToken
)

func newCurFromatter(c *cur, fmtr *kql.Formatter) *curFormatter {
	cf := new(curFormatter)
	cf.c = c
	cf.fmtr = fmtr.WithFunc(cf.getValue)
	return cf
}

func (cf *curFormatter) getCurFmtF() curFmtF {
	return cf.fmtLine
}

func (cf *curFormatter) checkBuf() {
	b := cf.cnctr.Buf()
	if cap(b) > 1024 && len(b) < 512 {
		cf.cnctr.Reset(nil)
	} else {
		cf.cnctr.Reset(cf.cnctr.Buf())
	}
}

func (cf *curFormatter) fmtSimple() []byte {
	return model.StringToByteArray(string(cf.c.le.GetMessage()))
}

func (cf *curFormatter) fmtLine() []byte {
	cf.checkBuf()
	cf.fmtr.Format(&cf.cnctr)
	return cf.cnctr.Buf()
}

// getValue return value by field name
func (cf *curFormatter) getValue(key string, qtn bool) string {
	res := ""
	switch key {
	case model.TAG_MESSAGE:
		res = string(cf.c.le.GetMessage())
	case model.TAG_TIMESTAMP:
		res = time.Unix(0, cf.c.le.GetTimestamp()).Format(time.RFC1123Z)
	case model.TAG_JOURNAL:
		ip := cf.c.it.GetIteratorPos().(journal.IteratorPosition)
		res = ip.Id
	default:
		tags := cf.getTags()
		if tags != nil {
			res = tags.GetValue(key)
		}
	}

	if qtn {
		return strconv.Quote(res)
	}

	return res
}

// getTags returns Tags for current log event
func (cf *curFormatter) getTags() *model.Tags {
	gid := cf.c.le.GetTGroupId()
	tds := cf.c.cp.TIndexer.GetTagsDesc(gid)
	if tds == nil {
		return nil
	}
	return tds.Tags
}
