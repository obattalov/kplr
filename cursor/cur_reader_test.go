package cursor

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/kplr-io/kplr/model"
)

type rr_test struct {
	idx    int
	src    []string
	closed bool
}

func new_rr_test(src []string) *rr_test {
	return &rr_test{src: src}
}

func (rr *rr_test) nextRecord() (string, error) {
	if rr.idx < len(rr.src) {
		res := rr.src[rr.idx]
		rr.idx++
		return res, nil
	}
	return "", io.EOF
}

func (rr *rr_test) onReaderClosed() {
	rr.closed = true
}

func (rr *rr_test) waitRecords(ctx context.Context) {
	select {
	case <-ctx.Done():
	}
}

func Test_cur_reader_general(t *testing.T) {
	r := new_cur_reader(new_rr_test([]string{}), 10, false)
	buf := []byte{0, 0, 0}
	n, err := r.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatal("Must be empty")
	}

	r = new_cur_reader(new_rr_test([]string{"a"}), 10, false)
	n, err = r.Read(buf)
	if n != 1 || err != nil {
		t.Fatal("Must not be empty")
	}

	s := model.ByteArrayToString(buf[:n])
	n, err = r.Read(buf)
	if s != "a" || n != 0 || err != io.EOF {
		t.Fatal("Expecting 'a', but got ", s)
	}

	r = new_cur_reader(new_rr_test([]string{"ab"}), 10, false)
	n, err = r.Read(buf)
	s = model.ByteArrayToString(buf[:n])
	if n != 2 || err != nil || s != "ab" {
		t.Fatal("Must not be empty s=", s)
	}

	rrt := new_rr_test([]string{"ab", "cde"})
	r = new_cur_reader(rrt, 10, false)
	n, err = r.Read(buf)
	s = model.ByteArrayToString(buf[:n])
	if n != 3 || err != nil || s != "abc" || rrt.closed {
		t.Fatal("Must not be empty s=", s, " rrt.closed must be false, but ", rrt.closed)
	}
	n, err = r.Read(buf)
	s = model.ByteArrayToString(buf[:n])
	if n != 2 || err != nil || s != "de" {
		t.Fatal("Must not be empty s=", s)
	}

	before := rrt.closed
	r.Close()
	if !rrt.closed || before {
		t.Fatal("before=", before, " rrt.closed=", rrt.closed)
	}
}

func Test_cur_reader_limit(t *testing.T) {
	// limit == 0
	r := new_cur_reader(new_rr_test([]string{"ab", "cd"}), 0, false)
	buf := make([]byte, 100)
	n, err := r.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatal("Must be io.EOF and == 0, but n=", n, ", err=", err)
	}

	// limit == 1
	rrt := new_rr_test([]string{"ab", "cd"})
	r = new_cur_reader(rrt, 1, false)
	n, err = r.Read(buf)
	s := model.ByteArrayToString(buf[:n])
	if n != 2 || err != nil || s != "ab" {
		t.Fatal("Must be ab, but got n=", n, ", s=", s, ", err=", err)
	}

	n, err = r.Read(buf)
	if n != 0 || err != io.EOF || !rrt.closed {
		t.Fatal("Must be io.EOF and == 0, but n=", n, ", err=", err, ", rrt.closed=", rrt.closed)
	}

	// limit == 2
	r = new_cur_reader(new_rr_test([]string{"ab", "cd"}), 2, false)
	n, err = r.Read(buf[:2])
	s = model.ByteArrayToString(buf[:n])
	if n != 2 || err != nil || s != "ab" {
		t.Fatal("Must be ab, but got n=", n, ", s=", s, ", err=", err)
	}

	n, err = r.Read(buf[:4])
	s = model.ByteArrayToString(buf[:n])
	if n != 2 || err != nil || s != "cd" {
		t.Fatal("Must be cd, but got n=", n, ", s=", s, ", err=", err)
	}

	n, err = r.Read(buf)
	n, err = r.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatal("Must be io.EOF, but got n=", n, ", err=", err)
	}

	// unlim
	rrt = new_rr_test([]string{"ab", "cd", "ef"})
	r = new_cur_reader(rrt, -1, false)
	n, err = r.Read(buf)
	s = model.ByteArrayToString(buf[:n])
	if n != 6 || err != nil || s != "abcdef" || rrt.closed {
		t.Fatal("Must be abcdef, but got n=", n, ", s=", s, ", err=", err, ", rrt.closed=", rrt.closed)
	}
}

func Test_cur_reader_exact(t *testing.T) {
	// limit == 2 even
	r := new_cur_reader(new_rr_test([]string{"ab", "cd"}), 2, true)
	buf := make([]byte, 100)
	n, err := r.Read(buf)
	s := model.ByteArrayToString(buf[:n])
	if n != 4 || err != nil || s != "abcd" {
		t.Fatal("Must be abcd, but got n=", n, ", s=", s, ", err=", err)
	}
	n, err = r.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatal("Must be io.EOF, but got n=", n, ", err=", err)
	}

	// blocking
	r = new_cur_reader(new_rr_test([]string{"ab", "cd", "ef"}), -1, true)
	n, err = r.Read(buf)
	s = model.ByteArrayToString(buf[:n])
	if n != 6 || err != nil || s != "abcdef" {
		t.Fatal("Must be abcdef, but got n=", n, ", s=", s, ", err=", err)
	}
	start := time.Now()
	go func() {
		time.Sleep(10 * time.Millisecond)
		r.Close()
	}()
	n, err = r.Read(buf)
	if n != 0 || err != errAlreadyClosed || time.Now().Sub(start) < 10*time.Millisecond {
		t.Fatal("Expecting err=errAlreadyClosed but err=", err, ", or timeout not 10ms")
	}

	n, err = r.Read(buf)
	if n != 0 || err != errAlreadyClosed {
		t.Fatal("Expecting err=errAlreadyClosed but err=", err)
	}
}
