package journal

import (
	"reflect"
	"testing"

	"github.com/kplr-io/journal"
)

func Test_on_chnk_tags(t *testing.T) {
	ct := new_chnk_tags()
	ct.on_chnk_tags("abc", journal.RecordId{1, 0})
	ct.on_chnk_tags("abc", journal.RecordId{2, 0})
	ct.on_chnk_tags("def", journal.RecordId{1, 0})

	if len(ct.chnks) != 2 || len(ct.chnks[1].Tags) != 2 || len(ct.chnks[2].Tags) != 1 {
		t.Fatal("Incorrect chunks state ", ct.chnks)
	}

	if len(ct.tags) != 2 {
		t.Fatal("Incorrect tags state ", ct.tags)
	}
}

func Test_on_chnk_delete(t *testing.T) {
	ct := new_chnk_tags()
	ct.on_chnk_tags("abc", journal.RecordId{1, 0})
	ct.on_chnk_tags("abc", journal.RecordId{2, 0})
	ct.on_chnk_tags("def", journal.RecordId{1, 0})

	ct.on_chnk_delete(1)
	if len(ct.chnks) != 1 || ct.chnks[1] != nil || len(ct.chnks[2].Tags) != 1 {
		t.Fatal("Incorrect chunks state ", ct.chnks)
	}

	if len(ct.tags) != 1 {
		t.Fatal("Incorrect tags state ", ct.tags)
	}

	ct.on_chnk_delete(2)
	if len(ct.chnks) != 0 || ct.chnks[2] != nil || len(ct.tags) != 0 {
		t.Fatal("Incorrect chunks or tags state ", ct.chnks, ct.tags)
	}
}

func Test_get_tags(t *testing.T) {
	ct := new_chnk_tags()
	ct.on_chnk_tags("abc", journal.RecordId{1, 0})
	ct.on_chnk_tags("abc", journal.RecordId{2, 0})
	ct.on_chnk_tags("def", journal.RecordId{1, 0})

	tags := ct.get_tags("j1")
	if !reflect.DeepEqual(tags, map[string]string{"abc": "j1", "def": "j1"}) {
		t.Fatal("1) Unexpected tags=", tags)
	}

	ct.on_chnk_delete(2)
	tags = ct.get_tags("j1")
	if !reflect.DeepEqual(tags, map[string]string{"abc": "j1", "def": "j1"}) {
		t.Fatal("2) Unexpected tags=", tags)
	}

	ct.on_chnk_delete(1)
	tags = ct.get_tags("j1")
	if len(tags) != 0 {
		t.Fatal("3) Unexpected tags=", tags)
	}
}
