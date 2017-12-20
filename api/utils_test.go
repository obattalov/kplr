package api

import (
	"reflect"
	"testing"

	"github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/cursor"
)

func TestCurPosToDo(t *testing.T) {
	cp := cursor.CursorPosition{"srcId": journal.RecordId{1234, 123434}, "src123479@$%_ A234AzqId2": journal.RecordId{14, 1234}}
	curDO := curPosToCurPosDO(cp)

	cp2, err := curPosDOToCurPos(curDO)
	if err != nil || !reflect.DeepEqual(cp, cp2) {
		t.Fatal("Expecting ", cp, ", but got cp2=", cp2, " err=", err)
	}
}
