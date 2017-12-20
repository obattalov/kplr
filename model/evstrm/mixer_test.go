package evstrm

import (
	"reflect"
	"strings"
	"testing"

	"github.com/kplr-io/kplr/model"
)

func TestMixer(t *testing.T) {
	testMixerOrder([]model.WeakString{"a", "b"}, []model.WeakString{"c", "d"}, []model.WeakString{"a", "b", "c", "d"}, GetFirst, t)
	testMixerOrder([]model.WeakString{"a"}, []model.WeakString{"c", "d"}, []model.WeakString{"a", "c", "d"}, GetFirst, t)
	testMixerOrder([]model.WeakString{}, []model.WeakString{"c", "d"}, []model.WeakString{"c", "d"}, GetFirst, t)
	testMixerOrder([]model.WeakString{"a", "b"}, []model.WeakString{}, []model.WeakString{"a", "b"}, GetFirst, t)
	testMixerOrder([]model.WeakString{}, []model.WeakString{}, []model.WeakString{}, GetFirst, t)

	i := 0
	sel := func(ev1, ev2 *model.LogEvent) bool {
		i++
		return i&1 != 0
	}
	testMixerOrder([]model.WeakString{"a", "b"}, []model.WeakString{"c", "d"}, []model.WeakString{"a", "c", "b", "d"}, sel, t)
	i = 1
	testMixerOrder([]model.WeakString{"a", "b"}, []model.WeakString{"c", "d"}, []model.WeakString{"c", "a", "d", "b"}, sel, t)
}

func TestForthBack(t *testing.T) {
	sa1i := test_it{id: 100, ss: []model.WeakString{"0", "2", "4"}}
	sa2i := test_it{id: 200, ss: []model.WeakString{"1", "3", "5", "6"}}

	var m Mixer
	m.Reset(cmpSources, &sa1i, &sa2i)
	res, poss := iterate(m)
	exp := []model.WeakString{"0", "1", "2", "3", "4", "5", "6"}
	expIdxs := []IteratorPos{100, 200, 101, 201, 102, 202, 203}
	if !reflect.DeepEqual(res, exp) {
		t.Fatal("result ", res, " is not matched to ", exp)
	}
	if !reflect.DeepEqual(poss, expIdxs) {
		t.Fatal("result ", poss, " is not matched to ", expIdxs)
	}

	m.Backward(true)
	res, poss = iterate(m)
	expIdxs = []IteratorPos{203, 202, 102, 201, 101, 200, 100}
	exp = []model.WeakString{"6", "5", "4", "3", "2", "1", "0"}
	if !reflect.DeepEqual(res, exp) {
		t.Fatal("result ", res, " is not matched to ", exp)
	}
	if !reflect.DeepEqual(poss, expIdxs) {
		t.Fatal("result ", poss, " is not matched to ", expIdxs)
	}
}

func testMixerOrder(sa1, sa2, exp []model.WeakString, sel SelectF, t *testing.T) {
	sa1i := test_it{ss: sa1}
	sa2i := test_it{ss: sa2}

	var m Mixer
	m.Reset(sel, &sa1i, &sa2i)

	res, _ := iterate(m)

	if !reflect.DeepEqual(res, exp) {
		t.Fatal("result ", res, " is not matched to ", exp)
	}
}

func iterate(m Mixer) ([]model.WeakString, []IteratorPos) {
	res := make([]model.WeakString, 0, 10)
	pos := make([]IteratorPos, 0, 10)
	var ev model.LogEvent
	for !m.End() {
		m.Get(&ev)
		res = append(res, ev.Source())
		pos = append(pos, m.GetIteratorPos())
		m.Next()
	}
	return res, pos
}

func cmpSources(ev1, ev2 *model.LogEvent) bool {
	return strings.Compare(string(ev1.Source()), string(ev2.Source())) <= 0
}
