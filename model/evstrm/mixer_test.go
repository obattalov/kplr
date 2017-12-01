package evstrm

import (
	"reflect"
	"testing"

	"github.com/kplr-io/kplr/model"
)

func TestMixer(t *testing.T) {
	testMixerOrder([]string{"a", "b"}, []string{"c", "d"}, []string{"a", "b", "c", "d"}, getFirst, t)
	testMixerOrder([]string{"a"}, []string{"c", "d"}, []string{"a", "c", "d"}, getFirst, t)
	testMixerOrder([]string{}, []string{"c", "d"}, []string{"c", "d"}, getFirst, t)
	testMixerOrder([]string{"a", "b"}, []string{}, []string{"a", "b"}, getFirst, t)
	testMixerOrder([]string{}, []string{}, []string{}, getFirst, t)

	i := 0
	sel := func(ev1, ev2 *model.LogEvent) bool {
		i++
		return i&1 != 0
	}
	testMixerOrder([]string{"a", "b"}, []string{"c", "d"}, []string{"a", "c", "b", "d"}, sel, t)
	i = 1
	testMixerOrder([]string{"a", "b"}, []string{"c", "d"}, []string{"c", "a", "d", "b"}, sel, t)
}

func testMixerOrder(sa1, sa2, exp []string, sel SelectF, t *testing.T) {
	sa1i := test_it(sa1)
	sa2i := test_it(sa2)

	var m Mixer
	m.Reset(sel, &sa1i, &sa2i)

	ln := len(sa1) + len(sa2)
	res := make([]string, 0, ln)
	var ev model.LogEvent
	for !m.End() {
		m.Get(&ev)
		res = append(res, ev.Source())
		m.Next()
	}

	if !reflect.DeepEqual(res, exp) {
		t.Fatal("result ", res, " is not matched to ", exp)
	}

}
