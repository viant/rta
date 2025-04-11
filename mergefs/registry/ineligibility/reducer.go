package ineligibility

import "sync/atomic"

func Reduce(accumulator, source interface{}) {
	if accumulator == nil || source == nil {
		return
	}
	acc := accumulator.(*Record)
	src := source.(*Record)

	if acc.ID == 0 {
		acc.ID = src.ID
		acc.FeatureTypeID = src.FeatureTypeID
		acc.AudienceID = src.AudienceID
	}
	atomic.AddInt64(&acc.Value, src.Value)
}
