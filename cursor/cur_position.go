package cursor

import (
	"fmt"

	"github.com/kplr-io/journal"
	jnl "github.com/kplr-io/journal"
	"github.com/kplr-io/kplr/model"
)

type (
	CursorPosition map[string]journal.RecordId
)

var (
	CUR_POS_HEAD = CursorPosition{"": jnl.MinRecordId}
	CUR_POS_TAIL = CursorPosition{"": jnl.MaxRecordId}
)

func MarshalCursorPosition(cp CursorPosition, buf []byte) (int, error) {
	bp := 0
	for srcId, recId := range cp {
		n, err := model.MarshalString(srcId, buf[bp:])
		if err != nil {
			panic(fmt.Sprint("Could not marshal srcId=", srcId, ", err=", err))
		}
		bp += n

		n, err = model.MarshalUint32(recId.ChunkId, buf[bp:])
		if err != nil {
			panic(fmt.Sprint("Could not marshal recId.ChunkId=", recId.ChunkId, ", err=", err))
		}
		bp += n

		n, err = model.MarshalInt64(recId.Offset, buf[bp:])
		if err != nil {
			panic(fmt.Sprint("Could not marshal recId.Offset=", recId.Offset, ", err=", err))
		}
		bp += n
	}
	return bp, nil
}

func UnmarshalCursorPosition(buf []byte) (int, CursorPosition, error) {
	cp := make(CursorPosition)
	bp := 0
	for bp < len(buf) {
		n, ws, err := model.UnmarshalString(buf[bp:])
		if err != nil {
			return bp, cp, err
		}
		bp += n

		var recId journal.RecordId
		n, recId.ChunkId, err = model.UnmarshalUint32(buf[bp:])
		if err != nil {
			return bp, cp, err
		}
		bp += n

		n, recId.Offset, err = model.UnmarshalInt64(buf[bp:])
		if err != nil {
			return bp, cp, err
		}
		bp += n

		cp[ws.String()] = recId
	}
	return bp, cp, nil
}

func (cp *CursorPosition) BufSize() int {
	res := 0
	for srcId, _ := range *cp {
		res += 16 + len(srcId) // 4 for srcId size, 12 of journal.RecordId and srcId itself
	}
	return res
}
