package collector

import (
	"github.com/viant/gds/fmap"
	"sync"
)

type FMapPool struct {
	pool         sync.Pool
	expectedSize int
	fillFactor   float64
	zeroKeys     []int64 // Array of keys
	zeroData     []any   // Array of values corresponding to keys
}

func (c *FMapPool) Get() *fmap.FastMap[any] {
	v := c.pool.Get()
	return v.(*fmap.FastMap[any])
}

func (c *FMapPool) Put(m *fmap.FastMap[any]) {
	m.Clear(c.expectedSize, c.zeroKeys, c.zeroData)
	c.pool.Put(m)
}

// NewFMapPool creates a new pool
func NewFMapPool(expectedSize int, allocSize int) *FMapPool {
	ret := &FMapPool{
		expectedSize: expectedSize,
		fillFactor:   0.5,
	}

	ret.pool = sync.Pool{
		New: func() interface{} {
			return fmap.NewFastMap[any](ret.expectedSize, 0.5)
		},
	}
	var aMap *fmap.FastMap[any]
	for i := 0; i < max(allocSize, 2); i++ {
		aMap = fmap.NewFastMap[any](expectedSize, 0.5)
		ret.pool.Put(aMap)
	}
	ret.zeroKeys = make([]int64, aMap.Cap())
	ret.zeroData = make([]any, aMap.Cap())
	return ret
}
