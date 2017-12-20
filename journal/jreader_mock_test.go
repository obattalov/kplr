package journal

import (
	"io"
	"testing"

	"github.com/kplr-io/container/btsbuf"
	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/model"
)

// Be sure that jreader meets the journal.Reader contract
func TestJReaderMock(t *testing.T) {
	ss := []model.WeakString{"aa", "bb", "cc"}
	var jr JReaderMock
	jr.Reset(0, ss)

	var buf [10000]byte
	var bbw btsbuf.Writer
	bbw.Reset(buf[:], true)

	lr := journal.RecordId{0, 0}
	res, err := jr.ReadForward(&bbw)
	if err != nil || res != lr {
		t.Fatal("Expecting lr=", lr, ", but res=", res, ", err=", err)
	}

	_, err = jr.ReadForward(&bbw)
	_, err1 := jr.ReadForward(&bbw)
	if err != err1 || err != io.EOF {
		t.Fatal("Must be io.EOF, but err=", err)
	}

	lr = journal.RecordId{0, jr.offs[len(ss)-1]}
	res, err = jr.ReadBack(&bbw)
	if err != nil || res != lr {
		t.Fatal("Expecting lr=", lr, ", but res=", res, ", err=", err)
	}

	_, err = jr.ReadBack(&bbw)
	_, err1 = jr.ReadBack(&bbw)
	if err != err1 || err != io.EOF {
		t.Fatal("Must be io.EOF, but err=", err)
	}

	lr = journal.RecordId{0, 0}
	res, err = jr.ReadForward(&bbw)
	if err != nil || res != lr {
		t.Fatal("Expecting lr=", lr, ", but res=", res, ", err=", err)
	}
}
