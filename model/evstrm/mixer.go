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
		sel SelectF
		st  int
		i1  Iterator
		i2  Iterator
		err error
	}
)

func getFirst(ev1, ev2 *model.LogEvent) bool {
	return true
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
	if m.sel(&ev1, &ev2) {
		m.st = 1
	}
}
