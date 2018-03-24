package cursor

import (
	"strconv"
	"time"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model"
)

type (
	// curFormatter struct is a helper for formatting a cursor output
	curFormatter struct {
		c         *cur
		cnctr     btsbuf.Concatenator
		fmtTokens []*fmtToken
	}

	fmtToken struct {
		tt        int
		bv        []byte
		varName   string
		quotation bool
	}

	// curFmtF cursor formatter function
	curFmtF func() []byte
)

const (
	cFmtStringToken = iota
	cFmtVarToken
)

func newCurFromatter(c *cur, json bool, fields []string, qtn bool) *curFormatter {
	cf := new(curFormatter)
	cf.c = c

	flds := fields
	if len(flds) == 0 {
		flds = []string{model.TAG_MESSAGE}
	}

	if len(flds) != 1 || flds[0] == model.TAG_MESSAGE || json {
		if json {
			cf.fmtTokens = compileJsonTokens(flds)
		} else {
			cf.fmtTokens = compileTextTokens(flds, " ", qtn)
		}
	}

	return cf
}

func (cf *curFormatter) getCurFmtF() curFmtF {
	if len(cf.fmtTokens) == 0 {
		return cf.fmtSimple
	}
	return cf.fmtByTokens
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

func (cf *curFormatter) fmtByTokens() []byte {
	cf.checkBuf()
	for _, t := range cf.fmtTokens {
		if t.tt == cFmtStringToken {
			cf.cnctr.Write(t.bv)
		} else {
			cf.cnctr.Write(model.StringToByteArray(cf.getValue(t.varName, t.quotation)))
		}
	}
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

func compileJsonTokens(fields []string) []*fmtToken {
	res := make([]*fmtToken, 0, len(fields)*2)
	notFirst := false
	for _, fld := range fields {
		var bb []byte
		if notFirst {
			bb = []byte(", \"" + fld + "\": ")
		} else {
			bb = []byte("{\"" + fld + "\": ")
		}
		notFirst = true
		res = append(res, &fmtToken{tt: cFmtStringToken, bv: bb})
		res = append(res, &fmtToken{tt: cFmtVarToken, varName: fld, quotation: true})
	}
	res = append(res, &fmtToken{tt: cFmtStringToken, bv: []byte("}\n")})
	return res
}

func compileTextTokens(fields []string, sep string, qtn bool) []*fmtToken {
	res := make([]*fmtToken, 0, len(fields)*2)
	sepBuf := []byte(sep)
	notFirst := false
	for _, fld := range fields {
		if notFirst {
			res = append(res, &fmtToken{tt: cFmtStringToken, bv: sepBuf, quotation: qtn})
		}
		notFirst = true
		res = append(res, &fmtToken{tt: cFmtVarToken, varName: fld, quotation: qtn})
	}

	if fields[len(fields)-1] != model.TAG_MESSAGE {
		res = append(res, &fmtToken{tt: cFmtStringToken, bv: []byte("\n"), quotation: qtn})
	}

	return res
}
