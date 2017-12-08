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

	//	Write QUery here
	//	We need to know OR conditions to find subsets of journals?

	Request struct {
		qSel *kql.Select
		fltF journal.FilterF
	}

	chk_func func(le *model.LogEvent) bool
)

func NewRequest(query string) (*Request, error) {
	s, err := kql.Parse(query)
	if err != nil {
		return nil, err
	}

	f, err := buildFilterF(s)
	if err != nil {
		return nil, err
	}

	r := new(Request)
	r.qSel = s
	r.fltF = f
	return r, nil
}

func (r *Request) GetFilterF() journal.FilterF {
	return r.fltF
}

// BuildFilterF takes kql AST and builds the filtering function (which returns true
// if LogEvent does NOT match the condition)
func buildFilterF(query *kql.Select) (journal.FilterF, error) {
	if query.Where == nil || len(query.Where.Or) == 0 {
		return negative, nil
	}

	return func() (f journal.FilterF, err error) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			e, ok := r.(error)
			if !ok {
				err = errors.New(fmt.Sprint("Unexpected panic ", r))
				return
			}
			err = e
		}()

		f1 := getOrConds(query.Where.Or)
		f = func(el *model.LogEvent) bool {
			return !f1(el)
		}
		return f, err
	}()
}

func negative(le *model.LogEvent) bool {
	return false
}

func positive(le *model.LogEvent) bool {
	return true
}

func getOrConds(ocn []*kql.OrCondition) chk_func {
	if len(ocn) == 0 {
		return positive
	}

	if len(ocn) == 1 {
		return getConds(ocn[0].And)
	}

	f2 := getOrConds(ocn[1:])
	f := getConds(ocn[0].And)
	return func(le *model.LogEvent) bool {
		return f(le) || f2(le)
	}
}

func getConds(cn []*kql.Condition) chk_func {
	if len(cn) == 0 {
		return positive
	}

	if len(cn) == 1 {
		return getCond(cn[0])
	}

	f2 := getConds(cn[1:])
	f := getCond(cn[0])

	return func(le *model.LogEvent) bool {
		return f(le) && f2(le)
	}
}

func getCond(cn *kql.Condition) chk_func {
	op := strings.ToLower(cn.Operand)
	if op == model.TAG_TS {
		return getTsCond(cn)
	}

	if op == model.TAG_SRC {
		return getSrcCond(cn)
	}

	return getTagCond(cn)
}

func getTsCond(cn *kql.Condition) chk_func {
	tm := parseTime(cn.Value)
	switch cn.Op {
	case "<":
		return func(le *model.LogEvent) bool {
			return le.Timestamp() < tm
		}
	case ">":
		return func(le *model.LogEvent) bool {
			return le.Timestamp() > tm
		}
	case "<=":
		return func(le *model.LogEvent) bool {
			return le.Timestamp() <= tm
		}
	case ">=":
		return func(le *model.LogEvent) bool {
			return le.Timestamp() >= tm
		}
	case "!=":
		return func(le *model.LogEvent) bool {
			return le.Timestamp() != tm
		}
	case "=":
		return func(le *model.LogEvent) bool {
			return le.Timestamp() == tm
		}
	}
	panic("Unsupported operation " + cn.Op + " for timetstamp comparison")
}

func getSrcCond(cn *kql.Condition) chk_func {
	switch strings.ToUpper(cn.Op) {
	case kql.CMP_CONTAINS:
		return func(le *model.LogEvent) bool {
			return strings.Contains(le.Source(), cn.Value)
		}
	case kql.CMP_HAS_PREFIX:
		return func(le *model.LogEvent) bool {
			return strings.HasPrefix(le.Source(), cn.Value)
		}
	case kql.CMP_HAS_SUFFIX:
		return func(le *model.LogEvent) bool {
			return strings.HasSuffix(le.Source(), cn.Value)
		}
	}
	panic("Unsupported operation " + cn.Op + " for source comparison")
}

func getTagCond(cn *kql.Condition) chk_func {
	switch strings.ToUpper(cn.Op) {
	case "=":
		cmpStr := model.TagSubst(cn.Operand, cn.Value)
		return func(le *model.LogEvent) bool {
			return strings.Contains(le.Tags(), cmpStr)
		}
	case kql.CMP_LIKE:
		// test it
		_, err := path.Match(cn.Value, "abc")
		if err != nil {
			panic(fmt.Sprint("Wrong like expression: ", cn.Value, " err=", err))
		}
		return func(le *model.LogEvent) bool {
			b, _ := path.Match(cn.Value, model.GetTag(le.Tags(), cn.Operand))
			return b
		}
	}
	panic("Unsupported operation " + cn.Op + " for tags filtering")
}

const unix_no_zone = "2006-01-02T15:04:05"

func parseTime(val string) uint64 {
	v, err := strconv.ParseInt(val, 10, 64)
	if err == nil {
		return uint64(v)
	}

	tm, err := time.Parse(time.RFC3339, val)
	if err != nil {
		tm, err = time.Parse(unix_no_zone, val)
		if err != nil {
			panic(fmt.Sprint("Could not parse timestamp ", val, " doesn't look like unix time or RFC3339 format or short form."))
		}
	}

	return uint64(tm.Unix())
}
