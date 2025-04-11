package selectibility

import "github.com/viant/rta/collector"

func Mapper(acc *collector.Accumulator) interface{} {
	var result = make([]*Record, acc.Len())
	i := 0
	acc.RLock()
	for _, v := range acc.Map {
		result[i] = v.(*Record)
		i++
	}
	acc.RUnlock()
	return result
}
