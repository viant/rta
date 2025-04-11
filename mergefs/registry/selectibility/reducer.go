package selectibility

import "sync/atomic"

func Reduce(accumulator, source interface{}) {
	if accumulator == nil || source == nil {
		return
	}
	acc := accumulator.(*Record)
	src := source.(*Record)
	if acc.ID == 0 {
		acc.ID = src.ID
		acc.Key = src.Key
		acc.Bucket = src.Bucket
	}
	atomic.AddInt64(&acc.Value, src.Value)
}
