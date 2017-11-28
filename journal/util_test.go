package journal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
)

func TestScanForJournals(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestScanForJournals")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	jid1 := "1a1"
	jid2 := "2a1"
	jid3 := "2BB"

	ensureDirExists(path.Join(dir, "a1", jid1))
	ensureDirExists(path.Join(dir, "a1", jid2))
	ensureDirExists(path.Join(dir, "BB", "1a1"))
	ensureDirExists(path.Join(dir, "BB", jid3))
	ensureDirExists(path.Join(dir, "B", "2BB"))
	ensureDirExists(path.Join(dir, "2BB", "2BB"))
	f, _ := os.OpenFile(path.Join(dir, "file"), os.O_CREATE|os.O_RDWR, 0640)
	f.Close()

	res, err := scanForJournals(dir)
	if err != nil {
		t.Fatal("Ooops could not scan for journal dirs err=", err)
	}

	if len(res) != 3 {
		t.Fatal("expecting 3 sutable dirs, but found ", len(res))
	}
	resStr := fmt.Sprintf("%+v", res)
	if !strings.Contains(resStr, jid1) {
		t.Fatal("Could not find ", jid1, " in ", resStr, ", but it would be nice")
	}
	if !strings.Contains(resStr, jid2) {
		t.Fatal("Could not find ", jid2, " in ", resStr, ", but it would be nice")
	}
	if !strings.Contains(resStr, jid3) {
		t.Fatal("Could not find ", jid3, " in ", resStr, ", but it would be nice")
	}
}

func TestJournalPath(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestJournalPath")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	jdir := path.Join(dir, "ab", "123ab")
	if _, err := os.Open(jdir); !os.IsNotExist(err) {
		t.Fatal("The ", jdir, " must not exist")
	}

	_, err = journalPath(dir, "1")
	if err == nil {
		t.Fatal("journalPath must return err that jid is too short")
	}

	jd, err := journalPath(dir, "123ab")
	if err != nil {
		t.Fatal("Must not create error for the journal path, but err=", err)
	}
	if jd != jdir {
		t.Fatal("Wrong dir ", jd, ", but another ", jdir, " was expected")
	}

	d, err := os.Open(jdir)
	if err != nil {
		t.Fatal("The ", jdir, " must exist, but err=", err)
	}
	d.Close()
}
