package journal

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func scanForJournals(dir string) ([]string, error) {
	res := make([]string, 0, 10)
	err := filepath.Walk(dir, func(pth string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			return nil
		}

		nm := info.Name()
		if len(nm) != 2 {
			return nil
		}

		return filepath.Walk(pth, func(pth2 string, info2 os.FileInfo, err error) error {
			if !info2.IsDir() || pth == pth2 {
				return nil
			}

			jid := info2.Name()
			if strings.HasSuffix(jid, nm) {
				res = append(res, jid)
			}
			return nil
		})
	})
	return res, err
}

func journalPath(baseDir, jid string) (string, error) {
	if len(jid) < 2 {
		return "", errors.New("Journal Id must be at least 2 chars len" + jid)
	}
	jpath := filepath.Join(baseDir, jid[len(jid)-2:], jid)
	err := ensureDirExists(jpath)
	return jpath, err
}

func ensureDirExists(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(dir, 0740)
		}
	} else {
		d.Close()
	}
	return err
}
