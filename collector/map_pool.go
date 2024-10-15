package collector

import (
	"github.com/dolthub/swiss"
	"github.com/viant/rta/collector/fmap"
	"sync"
)

type FMapPool struct {
	pool         sync.Pool
	expectedSize int
	fillFactor   float64
	zeroData     []any // Array of values corresponding to keys
}

func (c *FMapPool) Get() *swiss.Map[any, any] {
	v := c.pool.Get()
	return v.(*swiss.Map[any, any])
}

func (c *FMapPool) Put(aMap *swiss.Map[any, any]) {
	fmap.ClearSwissMap(aMap, c.zeroData, c.zeroData)
	c.pool.Put(aMap)
}

// NewFMapPool creates a new pool
func NewFMapPool(expectedSize int, allocSize int) *FMapPool {
	ret := &FMapPool{
		expectedSize: expectedSize,
		fillFactor:   0.5,
	}
	ret.pool = sync.Pool{
		New: func() interface{} {
			return swiss.NewMap[any, any](uint32(expectedSize))
		},
	}
	for i := 0; i < max(allocSize, 2); i++ {
		aMap := swiss.NewMap[any, any](uint32(expectedSize))
		ret.pool.Put(aMap)
	}
	ret.zeroData = make([]any, expectedSize)
	return ret
}
