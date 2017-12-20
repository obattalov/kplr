package model

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"
)

type (
	SSlice []string
)

// Size() returns how much memory serialization needs
func (ss SSlice) Size() int {
	sz := 2
	for _, s := range ss {
		sz += 4 + len(s)
	}
	return sz
}

func MarshalSSlice(ss SSlice, buf []byte) (int, error) {
	idx, err := MarshalUint16(uint16(len(ss)), buf)
	if err != nil {
		return 0, err
	}

	for _, s := range ss {
		n, err := MarshalString(s, buf[idx:])
		if err != nil {
			return 0, err
		}
		idx += n
	}
	return idx, nil
}

func UnmarshalSSlice(ss SSlice, buf []byte) (SSlice, int, error) {
	idx, usz, err := UnmarshalUint16(buf)
	if err != nil {
		return nil, 0, err
	}

	sz := int(usz)
	if sz > cap(ss) {
		return nil, 0, errors.New(fmt.Sprintf("Not enough space in the result slice, required capacity is %d, but actual one is %d", sz, cap(ss)))
	}

	ss = ss[:sz]
	for i := 0; i < sz; i++ {
		n, s, err := UnmarshalString(buf[idx:])
		if err != nil {
			return nil, 0, err
		}
		idx += n
		ss[i] = s
	}
	return ss, idx, nil
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
	var src = buf[4 : ln+4]
	dst := src
	src = *(*[]byte)(unsafe.Pointer(&v))
	copy(dst, src)
	return ln + 4, nil
}

func StringToByteArray(v string) []byte {
	return *(*[]byte)(unsafe.Pointer(&v))
}

func ByteArrayToString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}

func CopyString(s string) string {
	return string(StringToByteArray(s))
}

// UnmarshalStringCopy uses cast of []byte -> string, it is slow version
// becuase it requires an allocation of the new memory segment and copying it
func UnmarshalStringCopy(buf []byte) (int, string, error) {
	if len(buf) < 4 {
		return 0, "", noBufErr("UnmarshalString-size", len(buf), 4)
	}
	ln := int(binary.BigEndian.Uint32(buf))
	if ln+4 > len(buf) {
		return 0, "", noBufErr("UnmarshalString-body", len(buf), ln+4)
	}
	return ln + 4, string(buf[4 : ln+4]), nil
}

// UnmarshalString fastest, but not completely safe version of unmarshalling
// the byte buffer to string. Please use with care and keep in mind that buf must not
// be updated so as it will affect the string context then.
func UnmarshalString(buf []byte) (int, string, error) {
	if len(buf) < 4 {
		return 0, "", noBufErr("UnmarshalString-size", len(buf), 4)
	}
	ln := int(binary.BigEndian.Uint32(buf))
	if ln+4 > len(buf) {
		return 0, "", noBufErr("UnmarshalString-body", len(buf), ln+4)
	}
	bs := buf[4 : ln+4]
	res := *(*string)(unsafe.Pointer(&bs))
	return ln + 4, res, nil
}

func noBufErr(src string, ln, req int) error {
	return errors.New(fmt.Sprintf("Not enough space in the buf: %s requres %d bytes, but actual buf size is %d", src, req, ln))
}
