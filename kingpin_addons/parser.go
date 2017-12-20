package kingpin_addons

import (
	"math"
	"strconv"

	"github.com/jrivets/gorivets"
	"gopkg.in/alecthomas/kingpin.v2"
)

type (
	SizeValue int64
)

func (sv *SizeValue) Set(value string) error {
	res, err := gorivets.ParseInt64(value, math.MinInt64, math.MaxInt64, 0)
	if err != nil {
		return err
	}
	*sv = SizeValue(res)
	return nil
}

func (sv *SizeValue) String() string {
	return strconv.FormatInt(int64(*sv), 10)
}

func Size(s kingpin.Settings) (target *int64) {
	target = new(int64)
	s.SetValue((*SizeValue)(target))
	return
}
