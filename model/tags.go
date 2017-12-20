package model

import (
	"bytes"
	"strings"
)

const (
	TAG_TS     = "ts"
	TAG_SRC    = "src"
	TAG_SRC_ID = "__source_id__"
)

type (
	// Tags is a string in which contains values in format
	// <tagName>=<tagValue>|[<tagName>=<tagValue>|]*
	// tag names must be lower case.
	Tags WeakString
)

const (
	cTagValueSeparator = '='
	cTagSeparator      = '|'
)

func (t Tags) ContainsAll(tgs Tags) bool {
	i := 0
	var str WeakString
	for i > -1 {
		str, i = tgs.nextTag(i)
		idx := strings.Index(string(t), string(str))
		if idx == -1 {
			return false
		}

		if !strings.Contains(string(t), string(str)) {
			return false
		}
	}
	return true
}

func (t Tags) GetTagNames() []WeakString {
	ws := make([]WeakString, 0, 10)
	s := string(t)
	for {
		idx := 0
		for idx < len(s) && s[idx] != cTagSeparator {
			idx++
		}

		idx++
		if idx >= len(s) {
			break
		}

		s = s[idx:]
		idx = 0
		for idx < len(s) && s[idx] != cTagValueSeparator {
			idx++
		}
		if idx >= len(s) {
			break
		}
		ws = append(ws, WeakString(s[:idx]))
	}
	return ws
}

func (t Tags) nextTag(i int) (WeakString, int) {
	tag := string(t)
	if i >= len(tag) {
		return "", -1
	}

	if tag[i] != cTagSeparator {
		return "", -1
	}

	for i2 := i + 1; i2 < len(tag); i2++ {
		if tag[i2] == cTagSeparator {
			if i2+1 == len(tag) {
				return WeakString(tag[i : i2+1]), -1
			}
			return WeakString(tag[i : i2+1]), i2
		}
	}
	return WeakString(tag[i:]), -1
}

func (t Tags) GetTag(key string) string {
	var b bytes.Buffer
	b.WriteByte(cTagSeparator)
	b.WriteString(key)
	b.WriteByte(cTagValueSeparator)
	kv := ByteArrayToString(b.Bytes())

	tags := string(t)
	i := strings.Index(tags, string(kv))
	if i == -1 {
		return ""
	}

	st := i + len(kv)
	for i2 := st; i2 < len(tags); i2++ {
		if tags[i2] == cTagSeparator {
			return tags[st:i2]
		}
	}
	return tags[st:]
}

// Adds t2 to t and returns concatenation
func (t Tags) Add(k, v string) Tags {
	var b bytes.Buffer
	b.WriteString(string(t))
	if b.Len() == 0 {
		b.WriteByte(cTagSeparator)
	}
	b.WriteString(strings.ToLower(k))
	b.WriteByte(cTagValueSeparator)
	b.WriteString(v)
	b.WriteByte(cTagSeparator)
	return Tags(b.String())
}

func IsTsTag(tg string) bool {
	return TAG_TS == tg
}

func TagSubst(tag, val string) Tags {
	var b bytes.Buffer
	b.WriteByte(cTagSeparator)
	b.WriteString(strings.ToLower(tag))
	b.WriteByte(cTagValueSeparator)
	b.WriteString(val)
	b.WriteByte(cTagSeparator)
	return Tags(b.String())
}

// MapToTags turns the map m to Tags string using key order kOrder
func MapToTags(kOrder SSlice, m map[WeakString]WeakString) Tags {
	if len(kOrder) == 0 {
		return Tags("")
	}

	var b bytes.Buffer
	for _, key := range kOrder {
		v, ok := m[key]
		if !ok {
			continue
		}

		if b.Len() == 0 {
			b.WriteByte(cTagSeparator)
		}
		b.WriteString(string(key))
		b.WriteByte(cTagValueSeparator)
		b.WriteString(string(v))
		b.WriteByte(cTagSeparator)
	}

	return Tags(b.String())
}
