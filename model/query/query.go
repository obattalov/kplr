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
		srcExp ExprDesc
		srcIds []string
	}

	// ChkF defines a function which evaluates a condition over provided
	// LogEvent value and it returns the evanluation result
	ChkF func(le *model.LogEvent) bool

	IndexCond struct {
		Tag   string
		Op    string
		Value string
	}

	// ExprDesc - an expressing descriptor, which contains the expression
	// function valueation (ChkF) and the index descriptor, if any. The index
	// descriptor allows to use the index for selecting records for the evaluation
	ExprDesc struct {
		// The index can be used to select records by the condition. THe field
		// does make sense only for Priorities 1 and 2
		Index IndexCond

		// Function to calcualte the expression
		Expr ChkF

		// Priority:
		// 0 - disregard, always true or not relevant at all
		// 1 - indexed by exact value
		// 2 - indexed by greater/less condition or interval
		// 3 - cannot be indexed, all records must be considered
		Priority int
	}

	// trvrsl_ctx a structure which allows to traverse WHERE condition in kql.Select
	// to return journal.FilterF function
	trvrsl_ctx struct {
		// tagsOnly whether ts and src tags should not be taken into account
		tagsOnly bool
		exp      ExprDesc
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

	tc.tagsOnly = true
	err = tc.getOrConds(s.Where.Or)
	if err != nil {
		return nil, err
	}

	r := new(Query)
	r.qSel = s
	r.srcExp = tc.exp
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

func (r *Query) GetSrcExpr() ExprDesc {
	return r.srcExp
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
		return nil, errors.New("Expecting WHERE condition for selecting sources")
	}

	err := tc.getOrConds(query.Where.Or)
	if err != nil {
		return nil, err
	}

	// Filter function must return true if the record should be filtered, so
	// the condition value is false
	return journal.FilterF(neglect(tc.exp.Expr)), nil
}

func (tc *trvrsl_ctx) getOrConds(ocn []*kql.OrCondition) (err error) {
	if len(ocn) == 0 {
		tc.exp.Priority = 0
		tc.exp.Expr = positive
		return nil
	}

	if len(ocn) == 1 {
		return tc.getXConds(ocn[0].And)
	}

	err = tc.getOrConds(ocn[1:])
	if err != nil {
		return err
	}
	exp2 := tc.exp

	err = tc.getXConds(ocn[0].And)
	if err != nil {
		return err
	}

	if exp2.Priority == 0 {
		return nil
	}

	if tc.exp.Priority == 0 {
		tc.exp = exp2
		return nil
	}

	tc.exp.chooseWorse(&exp2)
	f1 := tc.exp.Expr
	f2 := exp2.Expr
	tc.exp.Expr = func(le *model.LogEvent) bool {
		return f1(le) || f2(le)
	}
	return nil
}

func (tc *trvrsl_ctx) getXConds(cn []*kql.XCondition) (err error) {
	if len(cn) == 0 {
		tc.exp.Priority = 0
		tc.exp.Expr = positive
		return nil
	}

	if len(cn) == 1 {
		return tc.getXCond(cn[0])
	}

	err = tc.getXConds(cn[1:])
	if err != nil {
		return err
	}
	exp2 := tc.exp

	err = tc.getXCond(cn[0])
	if err != nil {
		return err
	}

	if exp2.Priority == 0 {
		return nil
	}

	if tc.exp.Priority == 0 {
		tc.exp = exp2
		return nil
	}

	tc.exp.chooseBetter(&exp2)
	f1 := tc.exp.Expr
	f2 := exp2.Expr
	tc.exp.Expr = func(le *model.LogEvent) bool {
		return f1(le) && f2(le)
	}
	return nil
}

func (exp *ExprDesc) chooseBetter(exp2 *ExprDesc) {
	if exp.Priority > exp2.Priority {
		exp.Index = exp2.Index
		exp.Priority = exp2.Priority
	}
}

func (exp *ExprDesc) chooseWorse(exp2 *ExprDesc) {
	if exp.Priority < exp2.Priority {
		exp.Index = exp2.Index
		exp.Priority = exp2.Priority
	}
}

func (tc *trvrsl_ctx) getXCond(xc *kql.XCondition) (err error) {
	if xc.Expr != nil {
		err = tc.getOrConds(xc.Expr.Or)
	} else {
		err = tc.getCond(xc.Cond)
	}

	if err != nil {
		return err
	}

	if xc.Not {
		if tc.exp.Priority != 0 {
			tc.exp.Priority = 3
		}
		tc.exp.Expr = neglect(tc.exp.Expr)
		return nil
	}

	return nil
}

func (tc *trvrsl_ctx) getCond(cn *kql.Condition) (err error) {
	op := strings.ToLower(cn.Operand)
	if op == model.TAG_TS {
		return tc.getTsCond(cn)
	}

	if op == model.TAG_SRC {
		return tc.getSrcCond(cn)
	}

	return tc.getTagCond(cn)
}

func (tc *trvrsl_ctx) getTsCond(cn *kql.Condition) (err error) {
	if tc.tagsOnly {
		tc.exp.Priority = 0
		tc.exp.Expr = positive
		return nil
	}

	tm, err := parseTime(cn.Value)
	if err != nil {
		return err
	}

	tc.exp.Priority = 2
	tc.exp.Index = IndexCond{cn.Operand, cn.Op, cn.Value}
	switch cn.Op {
	case "<":
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return le.Timestamp() < tm
		}
	case ">":
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return le.Timestamp() > tm
		}
	case "<=":
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return le.Timestamp() <= tm
		}
	case ">=":
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return le.Timestamp() >= tm
		}
	case "!=":
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return le.Timestamp() != tm
		}
	case "=":
		tc.exp.Priority = 1
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return le.Timestamp() == tm
		}
	default:
		err = errors.New("Unsupported operation " + cn.Op + " for timetstamp comparison")
	}
	return err
}

func (tc *trvrsl_ctx) getSrcCond(cn *kql.Condition) (err error) {
	if tc.tagsOnly {
		tc.exp.Priority = 0
		tc.exp.Expr = positive
		return nil
	}

	op := strings.ToUpper(cn.Op)
	tc.exp.Priority = 3
	switch op {
	case kql.CMP_CONTAINS:
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return strings.Contains(le.Source(), cn.Value)
		}
	case kql.CMP_HAS_PREFIX:
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return strings.HasPrefix(le.Source(), cn.Value)
		}
	case kql.CMP_HAS_SUFFIX:
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return strings.HasSuffix(le.Source(), cn.Value)
		}
	default:
		err = errors.New("Unsupported operation " + cn.Op + " for source comparison")
	}
	return err
}

func (tc *trvrsl_ctx) getTagCond(cn *kql.Condition) (err error) {
	op := strings.ToUpper(cn.Op)
	tc.exp.Index = IndexCond{cn.Operand, op, cn.Value}
	switch op {
	case "=":
		cmpTag := model.TagSubst(cn.Operand, cn.Value)
		tc.exp.Priority = 1
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return le.Tags().ContainsAll(cmpTag)
		}
	case kql.CMP_LIKE:
		// test it
		tc.exp.Priority = 3
		_, err := path.Match(cn.Value, "abc")
		if err != nil {
			err = errors.New(fmt.Sprint("Wrong like expression: ", cn.Value, " err=", err))
		} else {
			tc.exp.Expr = func(le *model.LogEvent) bool {
				b, _ := path.Match(cn.Value, le.Tags().GetTag(cn.Operand))
				return b
			}
		}
	default:
		err = errors.New("Unsupported operation " + cn.Op + " for tags filtering")
	}
	return err
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
