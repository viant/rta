package config

import (
	"github.com/viant/rta/shared"
	"strconv"
	"time"
)

type Suffix func() string

// TimeBasedSuffix returns a string representation for a duration in microseconds
// since 2020-01-01 00:00:00 (1577836800 s since JAN 01 1970 UTC)
func TimeBasedSuffix() string {
	return strconv.FormatInt(time.Now().Sub(time.UnixMilli(shared.StartTime)).Microseconds(), 10)
}
