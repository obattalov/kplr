package kql

import (
	"fmt"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/kplr/model"
)

type (
	// Formatter struct contains compiled KQL result format expression. The
	// result format expression is a string, which is formed by the following
	// rules:
	// - variables and tags defined in curl braces like {var1}
	// - variables and tags which text will be escaped are defined in double
	// curl braces like {{var1}}
	// - special symbols are escaped using back slash: \{, \} etc.
	//
	// Examples:
	// "Result" - simple text
	// "Result where ts={ts} log message={msg}" - some text with variables
	// "{\"ts\": \"{ts}\", \"msg\": \"{{msg}}\"}" - a JSON result
	Formatter struct {
		fvf   FmtVarValueF
		ftkns []*fmtToken
	}

	// FmtVarValueF defines type of a function which returns variable value
	// by its name. The escape flag defines whether special symbols in the result
	// will be escaped. For instance, new line will be replaced by the sequence
	// \n etc.
	FmtVarValueF func(varName string, escape bool) string

	fmtToken struct {
		tt       int
		bv       []byte
		varName  string
		escaping bool
	}
)

const (
	cFmtTknConst = iota
	cFmtTknVar
)

// NewFormatter compiles the KQL result format string (fmtStr) to Formatter.
// The formatter will use fmtVarVal function to identify variables values when
// Format() method is invoked on it. Returns formatter or an error if any.
func NewFormatter(fmtStr string, fmtVarVal FmtVarValueF) (*Formatter, error) {
	ftkns := make([]*fmtToken, 0, 1)
	val := ""
	crlBr := 0
	var prevRn rune
	for idx, rv := range fmtStr {
		if prevRn == '\\' {
			val += string(rv)
			prevRn = 0
			continue
		}

		if rv == '{' {
			if crlBr == 0 {
				if len(val) > 0 {
					ftkns = append(ftkns, &fmtToken{tt: cFmtTknConst, bv: []byte(val)})
				}
				val = ""
			}
			if crlBr > 0 && prevRn != rv {
				return nil, fmt.Errorf("Unexpected '{': ...%s", fmtStr[idx-1:])
			}
			crlBr++
			prevRn = rv
			continue
		}

		if rv == '}' {
			if len(val) > 0 {
				ftkns = append(ftkns, &fmtToken{tt: cFmtTknVar, varName: val, escaping: crlBr > 1})
				val = ""
			}

			if crlBr == 0 {
				return nil, fmt.Errorf("Unexpected '}': ...%s", fmtStr[idx:])
			}
			crlBr--
			prevRn = 0

			if crlBr > 0 {
				prevRn = rv
			}
			continue
		}

		if prevRn == '}' {
			return nil, fmt.Errorf("Unexpected '}': ...%s", fmtStr[idx:])
		}

		prevRn = rv
		if rv == '\\' {
			continue
		}

		val += string(rv)
	}

	if crlBr > 0 {
		return nil, fmt.Errorf("Unexpected end of line, expecting '}'")
	}

	if len(val) > 0 {
		ftkns = append(ftkns, &fmtToken{tt: cFmtTknConst, bv: []byte(val)})
	}
	return &Formatter{fvf: fmtVarVal, ftkns: ftkns}, nil
}

// Format method will write result to provided Concatenator c.
func (f *Formatter) Format(c *btsbuf.Concatenator) {
	for _, t := range f.ftkns {
		if t.tt == cFmtTknConst {
			c.Write(t.bv)
		} else {
			c.Write(model.StringToByteArray(f.fvf(t.varName, t.escaping)))
		}
	}
}
