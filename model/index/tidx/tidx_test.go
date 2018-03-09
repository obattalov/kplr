package tidx

import (
	"reflect"
	"testing"

	"github.com/kplr-io/kplr/model"
)

func TestUpsertTags(t *testing.T) {
	tl := model.TagLine("key=value")
	mti := NewInMemTagIndex()

	tags, err := mti.UpsertTags(tl)
	if err != nil || tags.GetTagLine() != tl || tags.GetValue("key") != "value" {
		t.Fatal("Unexpected tags=", tags, ", err=", err)
	}

	tags2, err := mti.UpsertTags(tl)
	if err != nil || !reflect.DeepEqual(tags, tags2) || tags2.GetTagLine() != tl {
		t.Fatal("Unexpected tags2=", tags2, ", err=", err)
	}
}
