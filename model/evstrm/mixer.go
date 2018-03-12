package evstrm

import (
	"io"

	"github.com/kplr-io/kplr/model"
)

type (
	// SelectF decides which one should be selected, it returns true if ev1
	// must be selected instead of ev2. If ev2 should be used then it
	// returns false
	SelectF func(ev1, ev2 *model.LogEvent) bool

	Mixer struct {
		sel  SelectF
		st   int
		i1   Iterator
		i2   Iterator
		bkwd bool
		err  error
	}
)

// GetFirst returns whether the ev1 should be selected first
func GetFirst(ev1, ev2 *model.LogEvent) bool {
	return true
}

// GetEarliest returns whether ev1 has lowest timestamp rather than ev2
func GetEarliest(ev1, ev2 *model.LogEvent) bool {
	return ev1.GetTimestamp() <= ev2.GetTimestamp()
}

func (m *Mixer) Reset(sel SelectF, i1, i2 Iterator) {
	if sel == nil {
		panic("selector function cannot be nil")
	}
	m.sel = sel
	m.i1 = i1
	m.i2 = i2
	m.st = 0
}

func (m *Mixer) End() bool {
	return m.i1.End() && m.i2.End()
}

func (m *Mixer) Get(le *model.LogEvent) error {
	m.selectIt()
	switch m.st {
	case 1:
		return m.i1.Get(le)
	case 2:
		return m.i2.Get(le)
	}
	return m.err
}

func (m *Mixer) Next() {
	m.selectIt()
	switch m.st {
	case 1:
		m.i1.Next()
	case 2:
		m.i2.Next()
	}
	m.st = 0
}

func (m *Mixer) Backward(bkwrd bool) {
	if m.bkwd == bkwrd {
		return
	}

	m.i1.Backward(bkwrd)
	m.i2.Backward(bkwrd)
	m.bkwd = bkwrd
	m.st = 0
}

func (m *Mixer) selectIt() {
	if m.st != 0 {
		return
	}

	e1 := m.i1.End()
	e2 := m.i2.End()

	if e1 && e2 {
		m.st = 3
		m.err = io.EOF
		return
	}

	if e1 {
		m.st = 2
		return
	}

	if e2 {
		m.st = 1
		return
	}

	var ev1, ev2 model.LogEvent
	m.err = m.i1.Get(&ev1)
	if m.err != nil {
		m.st = 3
		return
	}

	m.err = m.i2.Get(&ev2)
	if m.err != nil {
		m.st = 3
		return
	}

	m.st = 2
	// when backward we need to revert the mixer func, which is XOR
	if m.sel(&ev1, &ev2) != m.bkwd {
		m.st = 1
	}
}

func (m *Mixer) GetIteratorPos() IteratorPos {
	m.selectIt()
	switch m.st {
	case 1:
		return m.i1.GetIteratorPos()
	case 2:
		return m.i2.GetIteratorPos()
	}
	return nil
}

func (m *Mixer) Close() error {
	err1 := m.i1.Close()
	err2 := m.i2.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
