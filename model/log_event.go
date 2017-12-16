package model

import (
	"fmt"
)

type (
	LogEvent struct {
		ts   int64
		src  string
		tags string
	}
)

func (le *LogEvent) Reset(ts uint64, src, tags string) {
	le.ts = int64(ts)
	le.src = src
	le.tags = tags
}

func (le *LogEvent) Source() string {
	return le.src
}

func (le *LogEvent) Timestamp() uint64 {
	return uint64(le.ts)
}

func (le *LogEvent) Tags() string {
	return le.tags
}

func (le *LogEvent) Tag(tag string) interface{} {
	switch tag {
	case TAG_SRC:
		return le.src
	case TAG_TS:
		return le.ts
	default:
		return GetTag(le.tags, tag)
	}
}

// BufSize returns size of marshalled data
func (le *LogEvent) BufSize() int {
	return 16 + len(le.src) + len(le.tags)
}

func (le *LogEvent) Marshal(buf []byte) (int, error) {
	n, err := MarshalInt64(le.ts, buf)
	if err != nil {
		return 0, err
	}

	n1, err := MarshalString(le.src, buf[n:])
	if err != nil {
		return 0, err
	}

	n += n1
	n1, err = MarshalString(le.tags, buf[n:])

	return n + n1, err
}

func (le *LogEvent) Unmarshal(buf []byte) (n int, err error) {
	n, le.ts, err = UnmarshalInt64(buf)
	if err != nil {
		return 0, err
	}

	var n1 int
	n1, le.src, err = UnmarshalString(buf[n:])
	if err != nil {
		return
	}

	n += n1
	n1, le.tags, err = UnmarshalString(buf[n:])

	return n + n1, err
}

func (le *LogEvent) UnmarshalCopy(buf []byte) (n int, err error) {
	n, le.ts, err = UnmarshalInt64(buf)
	if err != nil {
		return 0, err
	}
	var n1 int
	n1, le.src, err = UnmarshalStringCopy(buf[n:])
	if err != nil {
		return
	}

	n += n1
	n1, le.tags, err = UnmarshalString(buf[n:])
	return n + n1, err
}

func (le *LogEvent) String() string {
	return fmt.Sprint("{ts:", uint64(le.ts), ", src:", le.src, "}")
}
