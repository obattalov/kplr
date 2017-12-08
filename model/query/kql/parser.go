package kql

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
)

var (
	kqlLexer = lexer.Unquote(lexer.Upper(lexer.Must(lexer.Regexp(`(\s+)`+
		`|(?P<Keyword>(?i)SELECT|FROM|TAIL|WHERE|LIMIT|OFFSET|AND|OR|LIKE|CONTAINS|PREFIX|SUFFIX)`+
		`|(?P<Ident>[a-zA-Z0-9-_@#$%^&*{}]+)`+
		`|(?P<String>'[^']*'|"[^"]*")`+
		`|(?P<Operator><>|!=|<=|>=|[-+*/%,.()=<>])`,
	)), "Keyword"), "String")
	parser = participle.MustBuild(&Select{}, kqlLexer)
)

const (
	CMP_CONTAINS   = "CONTAINS"
	CMP_HAS_PREFIX = "PREFIX"
	CMP_HAS_SUFFIX = "SUFFIX"
	CMP_LIKE       = "LIKE"
)

type (
	Int int64

	Select struct {
		Tail   bool        `"SELECT" [ @"TAIL" ]`
		Format string      `[@String]`
		From   From        `["FROM" @@]`
		Where  *Expression `["WHERE" @@]`
		Limit  Int         `"LIMIT" @Ident`
		Offset *int64      `["OFFSET" @Ident]`
	}

	From struct {
		SrcIds []Ident `@@ { "," @@ }`
	}

	Ident struct {
		Id string `(@Ident | @String)`
	}

	Expression struct {
		Or []*OrCondition `@@ { "OR" @@ }`
	}

	OrCondition struct {
		And []*Condition `@@ { "AND" @@ }`
	}

	Condition struct {
		Operand string `  @Ident`
		Op      string ` (@("<"|">"|">="|"<="|"!="|"="|"CONTAINS"|"PREFIX"|"SUFFIX"|"LIKE"))`
		Value   string `(@String|@Ident)`
	}
)

func (i *Int) Capture(values []string) error {
	v, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return err
	}
	if v < 0 {
		return errors.New(fmt.Sprint("Expecting positive integer, but ", v))
	}
	*i = Int(v)
	return nil
}

func Parse(kql string) (*Select, error) {
	sel := &Select{}
	err := parser.ParseString(kql, sel)
	if err != nil {
		return nil, err
	}
	return sel, err
}
