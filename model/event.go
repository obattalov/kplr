package model

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type (
	Event     []interface{}
	FieldType int
	Meta      []FieldType
)

const (
	FTUint16 = 1
	FTInt64  = 2
	FTString = 3
)

var (
	errUnkonwnFiledType = errors.New("unknown field data type")
)

// Size returns size of a byte buffer which is required to serialize the object
// using the m meta information
func (ev *Event) Size(m Meta) int {
	sz := 2
	for i, ft := range m {
		if (*ev)[i] != nil {
			switch ft {
			case FTUint16:
				sz += 2
			case FTInt64:
				sz += 8
			case FTString:
				sz += 4 + len((*ev)[i].(string))
			default:
				return -1
			}

		}
	}
	return sz
}

func MarshalEvent(meta Meta, ev Event, buf []byte) (int, error) {
	idx := 2
	fldHeader := uint16(0)
	for i, ft := range meta {
		if ev[i] == nil {
			continue
		}
		fldHeader = fldHeader | (1 << uint(i))

		switch ft {
		case FTUint16:
			n, err := MarshalUint16(ev[i].(uint16), buf[idx:])
			if err != nil {
				return 0, err
			}
			idx += n
		case FTInt64:
			n, err := MarshalInt64(ev[i].(int64), buf[idx:])
			if err != nil {
				return 0, err
			}
			idx += n
		case FTString:
			n, err := MarshalString(ev[i].(string), buf[idx:])
			if err != nil {
				return 0, err
			}
			idx += n

		default:
			return 0, errUnkonwnFiledType
		}
	}
	MarshalUint16(fldHeader, buf)
	return idx, nil
}

func UnmarshalEvent(meta Meta, buf []byte, ev Event) (int, error) {
	idx, fldHeader, err := UnmarshalUint16(buf)
	if err != nil {
		return 0, err
	}
	for i, ft := range meta {
		if fldHeader&(1<<uint(i)) == 0 {
			ev[i] = nil
			continue
		}
		switch ft {
		case FTUint16:
			n, v, err := UnmarshalUint16(buf[idx:])
			if err != nil {
				return 0, err
			}
			idx += n
			ev[i] = v
		case FTInt64:
			n, v, err := UnmarshalInt64(buf[idx:])
			if err != nil {
				return 0, err
			}
			idx += n
			ev[i] = v
		case FTString:
			n, v, err := UnmarshalString(buf[idx:])
			if err != nil {
				return 0, err
			}
			idx += n
			ev[i] = v
		default:
			return 0, errUnkonwnFiledType
		}
	}
	return idx, nil
}

func MarshalUint16(v uint16, buf []byte) (int, error) {
	if len(buf) < 2 {
		return 0, noBufErr("MarshalUint16", len(buf), 2)
	}
	binary.BigEndian.PutUint16(buf, uint16(v))
	return 2, nil
}

func UnmarshalUint16(buf []byte) (int, uint16, error) {
	if len(buf) < 2 {
		return 0, 0, noBufErr("UnmarshalUint16", len(buf), 2)
	}
	return 2, binary.BigEndian.Uint16(buf), nil
}

func MarshalInt64(v int64, buf []byte) (int, error) {
	if len(buf) < 8 {
		return 0, noBufErr("MarshalInt64", len(buf), 8)
	}
	binary.BigEndian.PutUint64(buf, uint64(v))
	return 8, nil
}

func UnmarshalInt64(buf []byte) (int, int64, error) {
	if len(buf) < 8 {
		return 0, 0, noBufErr("UnmarshalInt64", len(buf), 8)
	}
	return 8, int64(binary.BigEndian.Uint64(buf)), nil
}

func MarshalString(v string, buf []byte) (int, error) {
	bl := len(buf)
	ln := len(v)
	if ln+4 > bl {
		return 0, noBufErr("MarshalString-size-body", bl, ln+4)
	}
	binary.BigEndian.PutUint32(buf, uint32(ln))
	copy(buf[4:ln+4], []byte(v))
	return ln + 4, nil
}

func UnmarshalString(buf []byte) (int, string, error) {
	if len(buf) < 4 {
		return 0, "", noBufErr("UnmarshalString-size", len(buf), 4)
	}
	ln := int(binary.BigEndian.Uint32(buf))
	if ln+4 > len(buf) {
		return 0, "", noBufErr("UnmarshalString-body", len(buf), ln+4)
	}
	return ln + 4, string(buf[4 : ln+4]), nil
}

func noBufErr(src string, ln, req int) error {
	return errors.New(fmt.Sprintf("Not enough space in the buf: %s requres %d bytes, but actual buf size is %d", src, req, ln))
}
