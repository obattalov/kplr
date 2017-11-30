package kplr

import (
	"os"

	"github.com/jrivets/log4g"
)

var DefaultLogger = log4g.GetLogger("kplr")

func Assert(cond bool, errMsg string) {
	if !cond {
		DefaultLogger.Fatal("ASSERTION ERROR: ", errMsg)
		panic(errMsg)
	}
}

func GetBoolPtr(val bool) *bool {
	return &val
}

func GetBoolVal(ptr *bool, defVal bool) bool {
	if ptr != nil {
		return *ptr
	}
	return defVal
}

func GetIntPtr(val int) *int {
	return &val
}

func GetIntVal(ptr *int, defVal int) int {
	if ptr != nil {
		return *ptr
	}
	return defVal
}

func IsFileNotExist(filename string) bool {
	_, err := os.Stat(filename)
	return os.IsNotExist(err)
}
