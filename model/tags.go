package model

import (
	"bytes"
	"strings"
)

const (
	TAG_TS  = "ts"
	TAG_SRC = "src"
)

func IsTsTag(tg string) bool {
	return TAG_TS == tg
}

func TagSubst(tag, val string) string {
	return tag + "=" + val
}

func TagsToStr(m map[string]string) string {
	var b bytes.Buffer
	first := true
	for k, v := range m {
		if !first {
			b.WriteString(",")
		}
		first = false
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(v)
	}
	return b.String()
}

func GetTag(tag, key string) string {
	kv := key + "="
	i := strings.Index(tag, kv)
	if i == -1 {
		return ""
	}
	st := i + len(kv)
	for i2 := st; i2 < len(tag); i2++ {
		if tag[i2] == ',' {
			return tag[st:i2]
		}
	}
	return tag[st:]
}
