package mpool

type (
	Pool interface {
		GetBtsBuf(sz int) []byte
		ReleaseBtsBuf(buf []byte)
	}

	mpool struct {
	}
)

func NewPool() Pool {
	return new(mpool)
}

func (mp *mpool) GetBtsBuf(sz int) []byte {
	return make([]byte, sz)
}

func (mp *mpool) ReleaseBtsBuf(buf []byte) {
	// Well done ;)
}
