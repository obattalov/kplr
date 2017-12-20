package cursor

import (
	"io"
	"testing"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/index"
	kj "github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/wire"
	"github.com/kplr-io/kplr/mpool"
)

type (
	test_jcontroller struct {
		jrnls map[string][]string
	}
)

func (jc *test_jcontroller) GetReader(jid string) (journal.Reader, error) {
	r := &kj.JReaderMock{}
	r.Reset(0, model.StrSliceToSSlice(jc.jrnls[jid]))
	return r, nil
}

func (jc *test_jcontroller) Write(wp wire.WritePacket) error {
	panic("Write is not supproted")
	return nil
}

func (jc *test_jcontroller) GetJournalInfo(jid string) (*kj.JournalInfo, error) {
	panic("GetJournalInfo: not implemented yet")
	return nil, nil
}

func (jc *test_jcontroller) GetJournals() []string {
	panic("GetJournals: not implemented yet")
	return nil
}

func (jc *test_jcontroller) Truncate(jid string, maxSize int64) (*kj.TruncateResult, error) {
	panic("Truncate: not implemented yet")
	return nil, nil
}

func new_test_cur_provider(jrnls map[string][]string) *cur_provider {
	cp := new(cur_provider)
	cp.JController = &test_jcontroller{jrnls}
	cp.MPool = mpool.NewPool()
	cp.TTable = index.NewTTable()
	cp.logger = log4g.GetLogger("kplr.cursor.provider")
	// add to journal source and the tag key=journal
	mp := make(map[string]string)
	for k := range jrnls {
		m := map[model.WeakString]model.WeakString{"key": model.WeakString(k), model.TAG_SRC_ID: model.WeakString(k)}
		tags := model.MapToTags([]model.WeakString{model.TAG_SRC_ID, "key"}, m)
		mp[string(tags)] = k
	}
	cp.TTable.Append(mp)

	return cp
}

func TestOneJournal(t *testing.T) {
	cp := new_test_cur_provider(map[string][]string{"j1": []string{"aa", "bb"}})
	c, err := cp.NewCursor("tst", []string{"j1"})
	if err != nil {
		t.Fatal("expecting no error, but err=", err)
	}

	rdr := c.GetReader(100, false)
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
	c, err := cp.NewCursor("tst", []string{"j1"})
	if err != nil {
		t.Fatal("expecting no error, but err=", err)
	}

	rdr := c.GetReader(1, false)
	buf := cp.MPool.GetBtsBuf(4)
	n, err := io.ReadFull(rdr, buf)
	s := model.ByteArrayToString(buf[:n])
	if n != 2 || err == nil || s != "aa" {
		t.Fatal("Must read 2 bytes with io.ErrUnexpectedEOF, but n=", n, ", err=", err, ", s=", s)
	}
}

func TestManyJournals(t *testing.T) {
	cp := new_test_cur_provider(map[string][]string{"j1": []string{"aa", "bb"}, "j2": []string{"cc", "dd"}, "j3": []string{"ee"}})
	c, err := cp.NewCursor("tst", []string{"j1", "j2", "j3"})
	if err != nil {
		t.Fatal("expecting no error, but err=", err)
	}

	rdr := c.GetReader(5, false)
	buf := cp.MPool.GetBtsBuf(20)
	n, err := io.ReadFull(rdr, buf)
	s := model.ByteArrayToString(buf[:n])
	if n != 10 || err == nil || s != "aacceebbdd" {
		t.Fatal("Must read 10 bytes with io.ErrUnexpectedEOF, but n=", n, ", err=", err, ", s=", s)
	}
}

func TestManyJournals2(t *testing.T) {
	cp := new_test_cur_provider(map[string][]string{"j1": []string{"aa", "bb"}, "j2": []string{"cc", "dd"}})
	c, err := cp.NewCursor("tst", []string{"j1", "j2"})
	if err != nil {
		t.Fatal("expecting no error, but err=", err)
	}

	rdr := c.GetReader(5, false)
	buf := cp.MPool.GetBtsBuf(20)
	n, err := io.ReadFull(rdr, buf)
	s := model.ByteArrayToString(buf[:n])
	if n != 8 || err == nil || s != "aaccbbdd" {
		t.Fatal("Must read 8 bytes with io.ErrUnexpectedEOF, but n=", n, ", err=", err, ", s=", s)
	}
}

func TestManyJournalsLimit(t *testing.T) {
	cp := new_test_cur_provider(map[string][]string{"j1": []string{"aa", "bb"}, "j2": []string{"cc", "dd"}})
	c, err := cp.NewCursor("tst", []string{"j1", "j2"})
	if err != nil {
		t.Fatal("expecting no error, but err=", err)
	}

	rdr := c.GetReader(3, false)
	buf := cp.MPool.GetBtsBuf(20)
	n, err := io.ReadFull(rdr, buf)
	s := model.ByteArrayToString(buf[:n])
	if n != 6 || err == nil || s != "aaccbb" {
		t.Fatal("Must read 8 bytes with io.ErrUnexpectedEOF, but n=", n, ", err=", err, ", s=", s)
	}
}
