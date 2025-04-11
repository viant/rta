package signal

import (
	"fmt"
	"github.com/viant/rta/collector"
)

func Mapper(acc *collector.Accumulator) interface{} {
	length := acc.Len()
	var result = make([]*Record, length)
	i := 0

	if aMap := acc.FastMap; aMap != nil {
		cnt := aMap.Count()
		aMap.Iter(func(k, v interface{}) (stop bool) {
			if i < cnt {
				result[i] = v.(*Record)
			} else {
				result = append(result, v.(*Record))
				fmt.Printf("warning: mapper map size greater then accumulator length (%d > %d)", cnt, length)
			}
			i++
			return false // continue
		})

		return result
	}

	acc.RLock()
	for _, v := range acc.Map {
		result[i] = v.(*Record)
		i++
	}
	acc.RUnlock()
	return result
}
