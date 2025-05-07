package collector

import (
	"fmt"
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
	pool        chan map[interface{}]interface{}
	mapInitSize int
	mapMaxSize  int
	poolMaxSize int
}

// NewMapPool creates a new MapPool
func NewMapPool(mapInitSize, mapMaxSize, poolMaxSize int) *MapPool {
	p := &MapPool{
		pool:        make(chan map[interface{}]interface{}, poolMaxSize),
		poolMaxSize: poolMaxSize,
		mapInitSize: mapInitSize,
		mapMaxSize:  mapMaxSize,
	}
	return p
}

// Get returns a map from the pool or allocates a fresh one if the pool is empty.
func (p *MapPool) Get() map[interface{}]interface{} {
	select {
	case m := <-p.pool:
		return m
	default:
		m := make(map[interface{}]interface{}, p.mapInitSize)
		return m
	}
}

// Put resets the map and returns it to the pool.
// If the pool is already full or len(m) > mapMaxSize, the map is simply discarded.
func (p *MapPool) Put(m map[interface{}]interface{}) {
	size := len(m)
	for k := range m {
		delete(m, k)
	}

	if p.mapMaxSize > 0 && size > p.mapMaxSize {
		fmt.Printf("Warning: MapPool: map size %d exceeds max size %d, discarding map (it won't be back to the pool)\n", size, p.mapMaxSize)
		return
	}

	select {
	case p.pool <- m:
		// returned to pool
	default:
		fmt.Printf("Warning: MapPool is full, discarding map (it won't be back to the pool)\n")
		// pool full, drop it
	}
}
