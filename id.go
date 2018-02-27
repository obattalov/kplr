package kplr

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/kplr-io/hash"
	"github.com/sony/sonyflake"
)

var (
	sfGen atomic.Value
)

func initId64Gen() interface{} {
	mac, err := hash.GetMacAddress()
	var mid uint16
	if err != nil {
		mid = uint16(os.Getegid())
	} else {
		m := uint16(0)
		for _, mc := range mac {
			m <<= 8
			m |= uint16(mc)
			mid ^= m
		}
	}

	sf := sonyflake.NewSonyflake(sonyflake.Settings{
		StartTime: time.Now(),
		MachineID: func() (uint16, error) {
			return mid, nil
		},
	})

	sfGen.Store(sf)
	return sf
}

func NextId64() int64 {
	sfI := sfGen.Load()
	if sfI == nil {
		sfI = initId64Gen()
	}
	sf := sfI.(*sonyflake.Sonyflake)

	id, err := sf.NextID()
	if err != nil {
		panic(err)
	}
	return int64(id)
}
