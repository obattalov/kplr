package api

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/kplr-io/kplr/cursor"
)

func curPosToCurPosDO(pos cursor.CursorPosition) string {
	buf := make([]byte, pos.BufSize())
	_, err := cursor.MarshalCursorPosition(pos, buf)
	if err != nil {
		panic(fmt.Sprint("Could not marshal CursorPosition ", pos, ", err=", err))
	}
	return base64.StdEncoding.EncodeToString(buf)
}

func curPosDOToCurPos(posDO string) (cursor.CursorPosition, error) {
	if len(posDO) == 4 {
		switch strings.ToLower(posDO) {
		case "tail":
			return cursor.CUR_POS_TAIL, nil
		case "head":
			return cursor.CUR_POS_HEAD, nil
		}
	}
	buf, err := base64.StdEncoding.DecodeString(posDO)
	if err != nil {
		return nil, err
	}
	_, cp, err := cursor.UnmarshalCursorPosition(buf)
	return cp, err
}
