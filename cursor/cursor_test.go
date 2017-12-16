package cursor

import (
	"io"
	"testing"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/query"
	"github.com/kplr-io/kplr/mpool"
	"github.com/kplr-io/kplr/storage"
)

// Mocking all needed objects here
type (
	test_jreader struct {
		idx int
		ss  []string
	}

	test_jcontroller struct {
		jrnls map[string][]string
	}
)

func (jr *test_jreader) reset(idx int, ss []string) {
	jr.idx = idx
	jr.ss = ss
}

func (jr *test_jreader) ReadForward(bbw *btsbuf.Writer) (int, error) {
	n := 0
	var le model.LogEvent
	for jr.idx < len(jr.ss) {
		le.Reset(uint64(jr.idx), jr.ss[jr.idx], "")
		bb, err := bbw.Allocate(le.BufSize())
		if err != nil {
			if n == 0 {
				return 0, err
			}
			return n, nil
		}
		le.Marshal(bb)
		jr.idx++
		n++
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (jr *test_jreader) ReadBack(bbw *btsbuf.Writer) (int, error) {
	n := 0
	var le model.LogEvent
	for jr.idx >= 0 {
		le.Reset(uint64(jr.idx), jr.ss[jr.idx], "")
		bb, err := bbw.Allocate(le.BufSize())
		if err != nil {
			if n == 0 {
				return 0, err
			}
			return n, nil
		}
		le.Marshal(bb)
		jr.idx--
		n++
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (jr *test_jreader) Close() error {
	jr.idx = len(jr.ss)
	return nil
}

func (jc *test_jcontroller) GetReader(jid string) (journal.Reader, error) {
	r := &test_jreader{}
	r.reset(0, jc.jrnls[jid])
	return r, nil
}

func (jc *test_jcontroller) GetWriter(jid string) (journal.Writer, error) {
	panic("GetWriter not supproted")
	return nil, nil
}

func new_test_cur_provider(jrnls map[string][]string) *cur_provider {
	cp := new(cur_provider)
	cp.JController = &test_jcontroller{jrnls}
	cp.MPool = mpool.NewPool()
	cp.Table = storage.NewTable()
	return cp
}

func TestNoFrom(t *testing.T) {
	cp := new_test_cur_provider(map[string][]string{})
	q, _ := query.NewQuery("select limit 120")
	_, err := cp.NewCursor(q)
	if err == nil {
		t.Fatal("expecting an error (no journal), but it is ok")
	}
}

func TestOneJournal(t *testing.T) {
	cp := new_test_cur_provider(map[string][]string{"j1": []string{"aa", "bb"}})
	q, _ := query.NewQuery("select from j1 limit 120")
	c, err := cp.NewCursor(q)
	if err != nil {
		t.Fatal("expecting no error, but err=", err)
	}

	rdr := c.GetRecords(100)
	buf := cp.MPool.GetBtsBuf(4)
	n, err := io.ReadFull(rdr, buf)
	s := model.ByteArrayToString(buf)
	if n != 4 || err != nil || s != "aabb" {
		t.Fatal("Must read 4 bytes with no error, but n=", n, ", err=", err, ", s=", s)
	}
	n, err = io.ReadFull(rdr, buf)
	if n != 0 || err != io.EOF {
		t.Fatal("Expecting eof, but n=", n, ", err=", err)
	}
}

func TestOneJournalLimit(t *testing.T) {
	cp := new_test_cur_provider(map[string][]string{"j1": []string{"aa", "bb"}})
	q, _ := query.NewQuery("select from j1 limit 120")
	c, err := cp.NewCursor(q)
	if err != nil {
		t.Fatal("expecting no error, but err=", err)
	}

	rdr := c.GetRecords(1)
	buf := cp.MPool.GetBtsBuf(4)
	n, err := io.ReadFull(rdr, buf)
	s := model.ByteArrayToString(buf[:n])
	if n != 2 || err == nil || s != "aa" {
		t.Fatal("Must read 2 bytes with io.ErrUnexpectedEOF, but n=", n, ", err=", err, ", s=", s)
	}
}

func TestManyJournals(t *testing.T) {
	cp := new_test_cur_provider(map[string][]string{"j1": []string{"aa", "bb"}, "j2": []string{"cc", "dd"}, "j3": []string{"ee"}})
	q, _ := query.NewQuery("select from j1, j2, j3 limit 120")
	c, err := cp.NewCursor(q)
	if err != nil {
		t.Fatal("expecting no error, but err=", err)
	}

	rdr := c.GetRecords(5)
	buf := cp.MPool.GetBtsBuf(20)
	n, err := io.ReadFull(rdr, buf)
	s := model.ByteArrayToString(buf[:n])
	if n != 10 || err == nil || s != "aabbccddee" {
		t.Fatal("Must read 10 bytes with io.ErrUnexpectedEOF, but n=", n, ", err=", err, ", s=", s)
	}
}

func TestManyJournals2(t *testing.T) {
	cp := new_test_cur_provider(map[string][]string{"j1": []string{"aa", "bb"}, "j2": []string{"cc", "dd"}})
	q, _ := query.NewQuery("select from j1, j2 limit 120")
	c, err := cp.NewCursor(q)
	if err != nil {
		t.Fatal("expecting no error, but err=", err)
	}

	rdr := c.GetRecords(5)
	buf := cp.MPool.GetBtsBuf(20)
	n, err := io.ReadFull(rdr, buf)
	s := model.ByteArrayToString(buf[:n])
	if n != 8 || err == nil || s != "aabbccdd" {
		t.Fatal("Must read 8 bytes with io.ErrUnexpectedEOF, but n=", n, ", err=", err, ", s=", s)
	}
}

func TestManyJournalsLimit(t *testing.T) {
	cp := new_test_cur_provider(map[string][]string{"j1": []string{"aa", "bb"}, "j2": []string{"cc", "dd"}})
	q, _ := query.NewQuery("select from j1, j2 limit 120")
	c, err := cp.NewCursor(q)
	if err != nil {
		t.Fatal("expecting no error, but err=", err)
	}

	rdr := c.GetRecords(3)
	buf := cp.MPool.GetBtsBuf(20)
	n, err := io.ReadFull(rdr, buf)
	s := model.ByteArrayToString(buf[:n])
	if n != 6 || err == nil || s != "aabbcc" {
		t.Fatal("Must read 8 bytes with io.ErrUnexpectedEOF, but n=", n, ", err=", err, ", s=", s)
	}
}
