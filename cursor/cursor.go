package cursor

import (
	"errors"
	"io"

	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/evstrm"
	"github.com/kplr-io/kplr/model/query"
	"github.com/kplr-io/kplr/mpool"
	"github.com/kplr-io/kplr/storage"
)

type (
	Cursor interface {
		GetRecords(limit int64) io.Reader
	}

	CursorProvider interface {
		// Executes the KQL query and returns Reader which will return
		NewCursor(qry *query.Query) (Cursor, error)
	}

	cur_provider struct {
		Table       storage.Table      `inject:"storageTable"`
		JController journal.Controller `inject:""`
		MPool       mpool.Pool         `inject:"mPool"`
	}

	cur struct {
		its []*journal.Iterator
		it  evstrm.Iterator
	}
)

const (
	DEF_RD_BUF_SIZE = 16 * 1024
)

func NewCursorProvider() CursorProvider {
	cp := new(cur_provider)
	return cp
}

// ============================ cur_provider =================================
func (cp *cur_provider) NewCursor(qry *query.Query) (Cursor, error) {
	srcs := cp.Table.GetSrcId(qry)
	if len(srcs) == 0 {
		return nil, errors.New("wrong query, empty sources")
	}

	fltF := qry.GetFilterF()
	var it evstrm.Iterator
	its := make([]*journal.Iterator, len(srcs))

	if len(srcs) == 1 {
		rd, err := cp.JController.GetReader(srcs[0])
		if err != nil {
			return nil, err
		}
		jit := journal.NewIterator(rd, cp.MPool.GetBtsBuf(DEF_RD_BUF_SIZE))
		jit.FltF = fltF
		// Achtung! Be aware about this
		jit.NoCpy = true
		it = jit
		its[0] = jit
	} else {
		mxs := make([]evstrm.Iterator, len(srcs))

		// first make basic journal iterators
		for i, srcId := range srcs {
			rd, err := cp.JController.GetReader(srcId)
			if err != nil {
				return nil, err
			}
			jit := journal.NewIterator(rd, cp.MPool.GetBtsBuf(DEF_RD_BUF_SIZE))
			jit.FltF = fltF
			its[i] = jit
			mxs[i] = jit
		}

		// mixing them now
		for len(mxs) > 1 {
			for i := 0; i < len(mxs)-1; i += 2 {
				m := &evstrm.Mixer{}
				//TODO support mixing strategy
				m.Reset(evstrm.GetFirst, mxs[i], mxs[i+1])
				mxs[i/2] = m
			}
			if len(mxs)&1 == 1 {
				mxs[len(mxs)/2] = mxs[len(mxs)-1]
				mxs = mxs[:len(mxs)/2+1]
			} else {
				mxs = mxs[:len(mxs)/2]
			}
		}
		it = mxs[0]
	}

	c := new(cur)
	c.it = it
	c.its = its
	return c, nil
}

// ================================ cur ======================================
func (c *cur) GetRecords(limit int64) io.Reader {
	r := new(reader)
	r.f = c.getSimpleFormatter(limit)
	return r
}

func (c *cur) getSimpleFormatter(limit int64) formatter {
	return func() (string, error) {
		if limit <= 0 {
			return "", io.EOF
		}
		limit--
		var le model.LogEvent
		err := c.it.Get(&le)
		if err != nil {
			return "", err
		}
		c.it.Next()
		return le.Source(), nil
	}
}
