package model

import (
	"testing"
)

func BenchmarkParseTags(b *testing.B) {
	tags := make(map[string]string, 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		str := GetTag("ke1=vkasdlfkhjakdfjlj       aadfal1,key2=vaKFJLJKJLSJFLSl2", "key2")
		tags["key2"] = str
		//StrToTags("ke1=vkasdlfkhjakdfjlj       aadfal1,key2=vaKFJLJKJLSJFLSl2", tags)
	}
}
