package kql

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/kplr-io/kplr/model"
)

type (
	// expFunc is a helper function which is used in evaluation expressions process
	// and which is returned as an expression evaluation result. The evaluate()
	// function will traverse the expression AST and connect expValidator to
	// the expression. As a result the function is returned in expFuncDesc object
	expFunc func() bool

	// expFuncDesc a wrapper, which helps to keep the function compilation while
	// traveling over expression AST
	expFuncDesc struct {
		fn expFunc
		// typ defines the expFunc type see cFTXXX consts below
		typ int
	}

	// expValuator is an object, which is used for evaluating an expression and
	// associate an expFunc with it.
	expValuator interface {
		// timestamp should return 'ts' value when the expression func is called.
		// the method can be called only when cExpValTsIgnore flag is not set
		timestamp() uint64

		// msg should return 'msg' value when the expression func is called.
		// the method can be called only when cExpValMsgIgnore flag is not set
		msg() model.WeakString

		// tagVal should return a tag value when the expression func is called.
		// the method can be called only when cExpValTagsIgnore flag is not set
		tagVal(tag string) string
	}

	eval struct {
		efd   *expFuncDesc
		ev    expValuator
		flags int
	}
)

// Expression valuation flags
const (
	cExpValTsIgnore   = 1 << 0
	cExpValMsgIgnore  = 1 << 1
	cExpValTagsIgnore = 1 << 2
)

// the function type see expFuncDesc
const (
	cFTUser   = 1
	cFTIgnore = 0
)

var (
	expFuncDescIgnore = &expFuncDesc{func() bool { return true }, cFTIgnore}
)

// or applys efd1 value as OR condition to the function and returns pointer to
// the descriptor to be used efd or efd1
func (efd *expFuncDesc) or(efd1 *expFuncDesc) *expFuncDesc {
	if efd.typ == cFTIgnore || efd1.typ == cFTIgnore {
		return expFuncDescIgnore
	}

	return &expFuncDesc{func() bool {
		return efd.fn() || efd1.fn()
	}, cFTUser}
}

// and applys efd1 value as AND condition to the function and returns pointer to
// the descriptor to be used efd or efd1
func (efd *expFuncDesc) and(efd1 *expFuncDesc) *expFuncDesc {
	if efd.typ == cFTIgnore {
		return efd1
	}

	if efd1.typ == cFTIgnore {
		return efd
	}

	return &expFuncDesc{func() bool {
		return efd.fn() && efd1.fn()
	}, cFTUser}
}

func (efd *expFuncDesc) not() *expFuncDesc {
	if efd.typ == cFTIgnore {
		return efd
	}

	return &expFuncDesc{func() bool {
		return !efd.fn()
	}, efd.typ}
}

// evaluate builds the expression evaluator using the provided validator expV.
// The function will traverse over the expression exp AST and construct expFunc,
// which will return whether the expression is true of false for the expression
// validator vn. So, the resulted expFunc tightly connected with expValuator expV
// and it returns the expression value for the expValuator settings.
//
// flags defines settings how the expression can be modified, if needed. The operands
// are split onto 3 categories: timestamp, message and tags. A category can
// be ignored, which means that evaluation of the expression is not needed. For
// example, if an expression is 'ts < 10 and msg contains "abc"' and timestamp
// should be ignored, it means that the evaluator will build the function in
// supposition that ts can have any value and will not be included into result
// expression. For the case the expression above will be translated to simple
// ' msg contains "abc"', cause it will makes sense only when the expression is
// true and will always be false if the msg check is false.
//
// Ignoring of some part of the expression will help to simplify expression and
// make an optimization regarding the initial expression value. For example, if
// after ignoring of params the result value does not have cFTIgnore type, then
// it means if the function returns false, the initial expression will be false.
// we stil can say nothing about initial expression result if the optimized one
// returns true, so the initial expression must be checked.
//
// When an operand is ignored (marked as I) the following rules of expression
// transformation are used:
// I OR e  => I
// I AND e => e
// NOT I   => I
// NOT e   => e
func evaluate(exp *Expression, expV expValuator, flags int) (*expFuncDesc, error) {
	if exp == nil {
		// Trust everything
		return expFuncDescIgnore, nil
	}
	ev := eval{nil, expV, flags}
	err := ev.buildOrConds(exp.Or)
	if err != nil {
		return nil, err
	}

	return ev.efd, nil
}

func (ev *eval) buildOrConds(ocn []*OrCondition) error {
	if len(ocn) == 0 {
		ev.efd = expFuncDescIgnore
		return nil
	}

	err := ev.buildXConds(ocn[0].And)
	if err != nil {
		return err
	}

	if len(ocn) == 1 {
		// no need to go ahead anymore
		return nil
	}

	efd0 := ev.efd
	err = ev.buildOrConds(ocn[1:])
	if err != nil {
		return err
	}

	ev.efd = efd0.or(ev.efd)
	return nil
}

func (ev *eval) buildXConds(cn []*XCondition) (err error) {
	if len(cn) == 0 {
		ev.efd = expFuncDescIgnore
		return nil
	}

	if len(cn) == 1 {
		return ev.buildXCond(cn[0])
	}

	err = ev.buildXCond(cn[0])
	if err != nil {
		return err
	}

	efd0 := ev.efd
	err = ev.buildXConds(cn[1:])
	if err != nil {
		return err
	}

	ev.efd = efd0.and(ev.efd)
	return nil

}

func (ev *eval) buildXCond(xc *XCondition) (err error) {
	if xc.Expr != nil {
		err = ev.buildOrConds(xc.Expr.Or)
	} else {
		err = ev.buildCond(xc.Cond)
	}

	if err != nil {
		return err
	}

	if xc.Not {
		ev.efd = ev.efd.not()
		return nil
	}

	return nil
}

func (ev *eval) buildCond(cn *Condition) (err error) {
	op := strings.ToLower(cn.Operand)
	if op == model.TAG_TIMESTAMP {
		return ev.buildTsCond(cn)
	}

	if op == model.TAG_MESSAGE {
		return ev.buildMsgCond(cn)
	}

	return ev.buildTagCond(cn)
}

func (ev *eval) buildTsCond(cn *Condition) (err error) {
	if ev.flags&cExpValTsIgnore != 0 {
		ev.efd = expFuncDescIgnore
		return nil
	}

	tm, err := parseTime(cn.Value)
	if err != nil {
		return err
	}

	switch cn.Op {
	case "<":
		ev.efd = &expFuncDesc{func() bool {
			return ev.ev.timestamp() < tm
		}, cFTUser}
	case ">":
		ev.efd = &expFuncDesc{func() bool {
			return ev.ev.timestamp() > tm
		}, cFTUser}
	case "<=":
		ev.efd = &expFuncDesc{func() bool {
			return ev.ev.timestamp() <= tm
		}, cFTUser}
	case ">=":
		ev.efd = &expFuncDesc{func() bool {
			return ev.ev.timestamp() >= tm
		}, cFTUser}
	case "!=":
		ev.efd = &expFuncDesc{func() bool {
			return ev.ev.timestamp() != tm
		}, cFTUser}
	case "=":
		ev.efd = &expFuncDesc{func() bool {
			return ev.ev.timestamp() == tm
		}, cFTUser}
	default:
		err = fmt.Errorf("Unsupported operation %s for timetstamp comparison", cn.Op)
	}
	return err

}

func (ev *eval) buildMsgCond(cn *Condition) (err error) {
	if ev.flags&cExpValMsgIgnore != 0 {
		ev.efd = expFuncDescIgnore
		return nil
	}

	op := strings.ToUpper(cn.Op)
	switch op {
	case CMP_CONTAINS:
		ev.efd = &expFuncDesc{func() bool {
			return strings.Contains(string(ev.ev.msg()), cn.Value)
		}, cFTUser}
	case CMP_HAS_PREFIX:
		ev.efd = &expFuncDesc{func() bool {
			return strings.HasPrefix(string(ev.ev.msg()), cn.Value)
		}, cFTUser}
	case CMP_HAS_SUFFIX:
		ev.efd = &expFuncDesc{func() bool {
			return strings.HasSuffix(string(ev.ev.msg()), cn.Value)
		}, cFTUser}
	default:
		err = fmt.Errorf("Unsupported operation %s for tag %s", cn.Op, cn.Operand)
	}
	return err
}

func (ev *eval) buildTagCond(cn *Condition) (err error) {
	if ev.flags&cExpValTagsIgnore != 0 {
		ev.efd = expFuncDescIgnore
		return nil
	}

	op := strings.ToUpper(cn.Op)
	switch op {
	case "=":
		ev.efd = &expFuncDesc{func() bool {
			return ev.ev.tagVal(cn.Operand) == cn.Value
		}, cFTUser}
	case CMP_LIKE:
		// test it first
		_, err := path.Match(cn.Value, "abc")
		if err != nil {
			err = fmt.Errorf("Wrong 'like' expression for %s, err=%s", cn.Value, err.Error())
		} else {
			ev.efd = &expFuncDesc{func() bool {
				b, _ := path.Match(cn.Value, ev.ev.tagVal(cn.Operand))
				return b
			}, cFTUser}
		}
	default:
		err = fmt.Errorf("Unsupported operation %s for '%s' tag ", cn.Op, cn.Operand)
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
			return 0, fmt.Errorf("Could not parse timestamp %s doesn't look like unix time or RFC3339 format or short form.", val)
		}
	}

	return uint64(tm.Unix()), nil
}
