package query

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/kplr-io/kplr"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/query/kql"
)

type (
	// Query is a structure which contains details of parsed KQL. It contains
	// filter function, which returns result whether a record should be skipped
	// or not and source expression descriptor - a structure which contains
	// information for selecting sources from index table.
	//
	// The srcExp contains filter function which is sub-expreession of initial
	// query condition refined by known tags. For instance, when known tags are
	// [val1, val2]:
	// val1 == 'src1' and time > 10  ---> val1 == 'src1'
	// val1 == 'src1" or val2 == '3' and someval contains 'x' ---> val1 == 'src1" or val2 == '3'
	// so only known tags are left in the conditions. For expression with
	// no condition any tag must be considered (always true)
	Query struct {
		QSel   *kql.Select
		fltF   model.FilterF
		srcExp model.ExprDesc
	}

	// trvrsl_ctx a structure which allows to traverse WHERE condition in kql.Select
	// to return journal.FilterF function
	trvrsl_ctx struct {
		// knwnTags specifies a list of tags that must be included in the expression
		// if the map is empty or nil, the field will be ignored
		knwnTags map[string]bool
		exp      model.ExprDesc
	}
)

// NewQuery - creates new query. It receives KQL text and list of known tags to
// be sure which tags can be used in the query body
func NewQuery(query string, knwnTags map[string]bool) (*Query, error) {
	s, err := kql.Parse(query)
	if err != nil {
		return nil, err
	}

	var tc trvrsl_ctx
	f, err := tc.buildFilterF(s)
	if err != nil {
		return nil, err
	}

	tc.knwnTags = knwnTags
	var ocn []*kql.OrCondition
	if s.Where != nil {
		ocn = s.Where.Or
	}
	err = tc.getOrConds(ocn)
	if err != nil {
		return nil, err
	}

	r := new(Query)
	r.QSel = s
	r.srcExp = tc.exp
	r.fltF = f
	return r, nil
}

func (r *Query) GetFilterF() model.FilterF {
	return r.fltF
}

func (r *Query) GetSrcExpr() *model.ExprDesc {
	return &r.srcExp
}

// Position returns position provided, or head, if the position was skipped in
// the original query
func (r *Query) Position() string {
	if r.QSel.Position == nil {
		return "head"
	}
	return r.QSel.Position.PosId
}

// Limit returns number of records that should be read, if 0 is returned
// there is no limit specified in the query
func (r *Query) Limit() int64 {
	return int64(r.QSel.Limit)
}

func (r *Query) Offset() int64 {
	return kplr.GetInt64Val(r.QSel.Offset, 0)
}

func (r *Query) Reverse() bool {
	return r.QSel.Tail
}

// ============================== trvrsl_ctx ===================================
func negative(le *model.LogEvent) bool {
	return false
}

func positive(le *model.LogEvent) bool {
	return true
}

func neglect(f model.ChkF) model.ChkF {
	return func(le *model.LogEvent) bool {
		return !f(le)
	}
}

// buildFilterF takes kql's AST and builds the filtering function (which returns true
// if LogEvent does NOT match the condition)
func (tc *trvrsl_ctx) buildFilterF(query *kql.Select) (model.FilterF, error) {
	if query.Where == nil || len(query.Where.Or) == 0 {
		// no filters then
		return nil, nil
	}

	err := tc.getOrConds(query.Where.Or)
	if err != nil {
		return nil, err
	}

	// Filter function must return true if the record should be filtered, so
	// the condition value is false
	return model.FilterF(neglect(tc.exp.Expr)), nil
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

	tc.exp.ChooseWorse(&exp2)
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

	tc.exp.ChooseBetter(&exp2)
	f1 := tc.exp.Expr
	f2 := exp2.Expr
	tc.exp.Expr = func(le *model.LogEvent) bool {
		return f1(le) && f2(le)
	}
	return nil
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
	if !tc.isKnown(op) {
		tc.exp.Priority = 0
		tc.exp.Expr = positive
		return nil
	}

	if op == model.TAG_TS {
		return tc.getTsCond(cn)
	}

	if op == model.TAG_SRC {
		return tc.getSrcCond(cn)
	}

	return tc.getTagCond(cn)
}

func (tc *trvrsl_ctx) isKnown(tag string) bool {
	if len(tc.knwnTags) == 0 {
		return true
	}
	_, ok := tc.knwnTags[tag]
	return ok
}

func (tc *trvrsl_ctx) getTsCond(cn *kql.Condition) (err error) {
	tm, err := parseTime(cn.Value)
	if err != nil {
		return err
	}

	tc.exp.Priority = 2
	tc.exp.Index = model.IndexCond{cn.Operand, cn.Op, cn.Value}
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
	op := strings.ToUpper(cn.Op)
	tc.exp.Priority = 3
	switch op {
	case kql.CMP_CONTAINS:
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return strings.Contains(string(le.Source()), cn.Value)
		}
	case kql.CMP_HAS_PREFIX:
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return strings.HasPrefix(string(le.Source()), cn.Value)
		}
	case kql.CMP_HAS_SUFFIX:
		tc.exp.Expr = func(le *model.LogEvent) bool {
			return strings.HasSuffix(string(le.Source()), cn.Value)
		}
	default:
		err = errors.New("Unsupported operation " + cn.Op + " for source comparison")
	}
	return err
}

func (tc *trvrsl_ctx) getTagCond(cn *kql.Condition) (err error) {
	op := strings.ToUpper(cn.Op)
	tc.exp.Index = model.IndexCond{cn.Operand, op, cn.Value}
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
				//fmt.Print("pattern=", cn.Value, ", tagValue=", le.Tags().GetTag(cn.Operand), ", tag=", cn.Operand, "result=", b, "\n")
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
