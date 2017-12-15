package cursor

import (
	"io"
	"testing"

	"github.com/kplr-io/kplr/model"
)

func getTestFormatter(ss ...string) formatter {
	idx := 0
	return func() (string, error) {
		if idx >= len(ss) {
			return "", io.EOF
		}
		idx++
		return ss[idx-1], nil
	}
}

func TestReader(t *testing.T) {
	r := reader{}
	r.f = getTestFormatter()
	buf := []byte{0, 0, 0}
	n, err := r.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatal("Must be empty")
	}

	r.f = getTestFormatter("a")
	n, err = r.Read(buf)
	if n != 1 || err != nil {
		t.Fatal("Must not be empty")
	}
	s := model.ByteArrayToString(buf[:n])
	n, err = r.Read(buf)
	if s != "a" || n != 0 || err != io.EOF {
		t.Fatal("Expecting 'a', but got ", s)
	}

	r.f = getTestFormatter("a", "b")
	n, err = r.Read(buf)
	s = model.ByteArrayToString(buf[:n])
	if n != 2 || err != nil || s != "ab" {
		t.Fatal("Must not be empty s=", s)
	}

	r.f = getTestFormatter("abcde")
	n, err = r.Read(buf)
	s = model.ByteArrayToString(buf[:n])
	if n != 3 || err != nil || s != "abc" {
		t.Fatal("Must not be empty s=", s)
	}
	n, err = r.Read(buf)
	s = model.ByteArrayToString(buf[:n])
	if n != 2 || err != nil || s != "de" {
		t.Fatal("Must not be empty s=", s)
	}

}
