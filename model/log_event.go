package model

import (
	"fmt"
)

type (
	LogEvent struct {
		ts  int64
		src string
	}
)

func (le *LogEvent) Reset(ts uint64, src string) {
	le.ts = int64(ts)
	le.src = src
}

func (le *LogEvent) Source() string {
	return le.src
}

func (le *LogEvent) Timestamp() uint64 {
	return uint64(le.ts)
}

// BufSize returns size of marshalled data
func (le *LogEvent) BufSize() int {
	return 12 + len(le.src)
}

func (le *LogEvent) Marshal(buf []byte) (int, error) {
	n, err := MarshalInt64(le.ts, buf)
	if err != nil {
		return 0, err
	}
	n1, err := MarshalString(le.src, buf[n:])
	return n + n1, err
}

func (le *LogEvent) Unmarshal(buf []byte) (n int, err error) {
	n, le.ts, err = UnmarshalInt64(buf)
	if err != nil {
		return 0, err
	}
	var n1 int
	n1, le.src, err = UnmarshalString(buf[n:])
	return n + n1, err
}

func (le *LogEvent) UnmarshalCopy(buf []byte) (n int, err error) {
	n, le.ts, err = UnmarshalInt64(buf)
	if err != nil {
		return 0, err
	}
	var n1 int
	n1, le.src, err = UnmarshalStringCopy(buf[n:])
	return n + n1, err
}

func (le *LogEvent) String() string {
	return fmt.Sprint("{ts:", uint64(le.ts), ", src:", le.src, "}")
}
