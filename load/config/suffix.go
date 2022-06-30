package config

import (
	"github.com/viant/rta/shared"
	"strconv"
	"time"
)

type Suffix func() string

func TimeBasedSuffix() string {
	return strconv.FormatInt(time.Now().Sub(time.UnixMilli(shared.StartTime)).Microseconds(), 10)
}
