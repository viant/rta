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

// NewFMapPool creates a new fMapPool
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

// MapPool is a pool of ready-to-use maps.
// It holds up to cap(p.pool) maps; excess returned maps are dropped.
type MapPool struct {
	pool        sync.Pool
	mapInitSize int
}

// NewMapPool creates a new MapPool
func NewMapPool(mapInitSize int) *MapPool {
	ret := &MapPool{
		mapInitSize: mapInitSize,
	}

	ret.pool = sync.Pool{
		New: func() interface{} {
			m := make(map[interface{}]interface{}, mapInitSize)
			return m
		},
	}

	return ret
}

// Get returns a map from the pool or allocates a fresh one if the pool is empty.
func (p *MapPool) Get() map[interface{}]interface{} {
	v := p.pool.Get()
	aMap := v.(map[interface{}]interface{})
	return aMap
}

// Put resets the map and returns it to the pool.
func (p *MapPool) Put(aMap map[interface{}]interface{}) {
	for k, _ := range aMap {
		delete(aMap, k)
	}
	p.pool.Put(aMap)
}
