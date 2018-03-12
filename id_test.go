package kplr

import (
	"testing"
	"time"
)

func TestNextId64(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Log(time.Now(), " ", NextId64())
	}
}
