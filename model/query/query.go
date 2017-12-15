package query

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/query/kql"
)

type (
	Query struct {
		qSel   *kql.Select
		fltF   journal.FilterF
		srcIds []string
	}

	// ChkF defines a function which evaluates a condition over provided
	// LogEvent value and it returns the evanluation result
	ChkF func(le *model.LogEvent) bool

	// trvrsl_ctx a structure which allows to traverse WHERE condition in kql.Select
	// to return journal.FilterF function
	trvrsl_ctx struct {
		// tagsOnly whether ts and src tags should be taken into account
		tagsOnly bool
	}
)

func NewQuery(query string) (*Query, error) {
	s, err := kql.Parse(query)
	if err != nil {
		return nil, err
	}

	var tc trvrsl_ctx
	f, err := tc.buildFilterF(s)
	if err != nil {
		return nil, err
	}

	r := new(Query)
	r.qSel = s
	r.fltF = f
	r.srcIds = make([]string, len(s.From.SrcIds))
	if len(s.From.SrcIds) > 0 {
		for i, id := range s.From.SrcIds {
			r.srcIds[i] = id.Id
		}
	}

	return r, nil
}

func (r *Query) GetFilterF() journal.FilterF {
	return r.fltF
}

func (r *Query) GetSources() []string {
	return r.srcIds
}

// Limit returns number of records that should be read, if 0 is returned
// there is no limit specified in the query
func (r *Query) Limit() int64 {
	return int64(r.qSel.Limit)
}

func (r *Query) Reverse() bool {
	return r.qSel.Tail
}

// ============================== trvrsl_ctx ===================================
func negative(le *model.LogEvent) bool {
	return false
}

func positive(le *model.LogEvent) bool {
	return true
}

func neglect(f ChkF) ChkF {
	return func(le *model.LogEvent) bool {
		return !f(le)
	}
}

// buildFilterF takes kql's AST and builds the filtering function (which returns true
// if LogEvent does NOT match the condition)
func (tc *trvrsl_ctx) buildFilterF(query *kql.Select) (journal.FilterF, error) {
	if query.Where == nil || len(query.Where.Or) == 0 {
		return nil, nil
	}

	f, err := tc.getOrConds(query.Where.Or)
	if err != nil {
		return nil, err
	}

	// Filter function must return true if the record should be filtered, so
	// the condition value is false
	return journal.FilterF(neglect(f)), nil
}

func (tc *trvrsl_ctx) getOrConds(ocn []*kql.OrCondition) (ChkF, error) {
	if len(ocn) == 0 {
		return positive, nil
	}

	if len(ocn) == 1 {
		return tc.getXConds(ocn[0].And)
	}

	f2, err := tc.getOrConds(ocn[1:])
	if err != nil {
		return f2, err
	}

	f, err := tc.getXConds(ocn[0].And)
	if err != nil {
		return f, err
	}

	return func(le *model.LogEvent) bool {
		return f(le) || f2(le)
	}, nil
}

func (tc *trvrsl_ctx) getXConds(cn []*kql.XCondition) (ChkF, error) {
	if len(cn) == 0 {
		return positive, nil
	}

	if len(cn) == 1 {
		return tc.getXCond(cn[0])
	}

	f2, err := tc.getXConds(cn[1:])
	if err != nil {
		return f2, err
	}

	f, err := tc.getXCond(cn[0])
	if err != nil {
		return f, err
	}

	return func(le *model.LogEvent) bool {
		return f(le) && f2(le)
	}, nil
}

func (tc *trvrsl_ctx) getXCond(xc *kql.XCondition) (f ChkF, err error) {
	if xc.Expr != nil {
		f, err = tc.getOrConds(xc.Expr.Or)
	} else {
		f, err = tc.getCond(xc.Cond)
	}

	if err != nil {
		return f, err
	}

	if xc.Not {
		return neglect(f), nil
	}

	return f, nil
}

func (tc *trvrsl_ctx) getCond(cn *kql.Condition) (ChkF, error) {
	op := strings.ToLower(cn.Operand)
	if op == model.TAG_TS {
		return tc.getTsCond(cn)
	}

	if op == model.TAG_SRC {
		return tc.getSrcCond(cn)
	}

	return tc.getTagCond(cn)
}

func (tc *trvrsl_ctx) getTsCond(cn *kql.Condition) (f ChkF, err error) {
	if tc.tagsOnly {
		return positive, nil
	}

	tm, err := parseTime(cn.Value)
	if err != nil {
		return nil, err
	}

	switch cn.Op {
	case "<":
		f = func(le *model.LogEvent) bool {
			return le.Timestamp() < tm
		}
	case ">":
		f = func(le *model.LogEvent) bool {
			return le.Timestamp() > tm
		}
	case "<=":
		f = func(le *model.LogEvent) bool {
			return le.Timestamp() <= tm
		}
	case ">=":
		f = func(le *model.LogEvent) bool {
			return le.Timestamp() >= tm
		}
	case "!=":
		f = func(le *model.LogEvent) bool {
			return le.Timestamp() != tm
		}
	case "=":
		f = func(le *model.LogEvent) bool {
			return le.Timestamp() == tm
		}
	default:
		err = errors.New("Unsupported operation " + cn.Op + " for timetstamp comparison")
	}
	return f, err
}

func (tc *trvrsl_ctx) getSrcCond(cn *kql.Condition) (f ChkF, err error) {
	if tc.tagsOnly {
		return positive, nil
	}

	switch strings.ToUpper(cn.Op) {
	case kql.CMP_CONTAINS:
		f = func(le *model.LogEvent) bool {
			return strings.Contains(le.Source(), cn.Value)
		}
	case kql.CMP_HAS_PREFIX:
		f = func(le *model.LogEvent) bool {
			return strings.HasPrefix(le.Source(), cn.Value)
		}
	case kql.CMP_HAS_SUFFIX:
		f = func(le *model.LogEvent) bool {
			return strings.HasSuffix(le.Source(), cn.Value)
		}
	default:
		err = errors.New("Unsupported operation " + cn.Op + " for source comparison")
	}
	return f, err
}

func (tc *trvrsl_ctx) getTagCond(cn *kql.Condition) (f ChkF, err error) {
	switch strings.ToUpper(cn.Op) {
	case "=":
		cmpStr := model.TagSubst(cn.Operand, cn.Value)
		f = func(le *model.LogEvent) bool {
			return strings.Contains(le.Tags(), cmpStr)
		}
	case kql.CMP_LIKE:
		// test it
		_, err := path.Match(cn.Value, "abc")
		if err != nil {
			err = errors.New(fmt.Sprint("Wrong like expression: ", cn.Value, " err=", err))
		} else {
			f = func(le *model.LogEvent) bool {
				b, _ := path.Match(cn.Value, model.GetTag(le.Tags(), cn.Operand))
				return b
			}
		}
	default:
		err = errors.New("Unsupported operation " + cn.Op + " for tags filtering")
	}
	return f, err
}

const unix_no_zone = "2006-01-02T15:04:05"

func parseTime(val string) (uint64, error) {
	v, err := strconv.ParseInt(val, 10, 64)
	if err == nil {
		return uint64(v), nil
	}

	tm, err := time.Parse(time.RFC3339, val)
	if err != nil {
		tm, err = time.Parse(unix_no_zone, val)
		if err != nil {
			return 0, errors.New(fmt.Sprint("Could not parse timestamp ", val, " doesn't look like unix time or RFC3339 format or short form."))
		}
	}

	return uint64(tm.Unix()), nil
}
