package model

import (
	"errors"
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

const FLD_PER_EVENT = 16

var (
	errUnkonwnFiledType = errors.New("unknown field data type")
)

func (ev *Event) Clear() {
	for i := 0; i < len(*ev); i++ {
		(*ev)[i] = nil
	}
}

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

// UnmarshalEvent unmarshal event using the buffer, no mem allocations will
// happen here. Unmarshaled strings will have references to the buf slices,
// so buf must not be modified while the event is used.
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

// UnmarshalEventCopy safe, but slow unmarshaling procedure, it will make extra
// memory allocations for string fields.
func UnmarshalEventCopy(meta Meta, buf []byte, ev Event) (int, error) {
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
			n, v, err := UnmarshalStringCopy(buf[idx:])
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
