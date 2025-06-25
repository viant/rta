package collector

import (
	"github.com/dolthub/swiss"
	"github.com/viant/rta/collector/fmap"
	"sync"
	"sync/atomic"
)

type Accumulator struct {
	Map        map[interface{}]interface{}
	UseFastMap bool
	FastMap    *swiss.Map[any, any]
	size       uint32
	sync.RWMutex
	UseShardedAcc      bool
	ShardedAccumulator *ShardedAccumulator
}

func (a *Accumulator) GetOrCreate(key interface{}, get func() interface{}) (interface{}, bool) {
	var data interface{}
	var ok bool

	if a.UseShardedAcc {
		data, ok = a.ShardedAccumulator.GetOrCreate(key, get)
		return data, ok
	}

	if a.UseFastMap {
		data, ok = a.tryGet(key)
		if ok && data != nil {
			return data, ok
		}
		a.RWMutex.RLock()
		data, ok = a.FastMap.Get(key)
		a.RWMutex.RUnlock()
		if !ok {
			a.RWMutex.Lock()
			data, ok = a.FastMap.Get(key)
			if !ok {
				data = get()
				ok = true
				a.FastMap.Put(key, data)
				atomic.StoreUint32(&a.size, uint32(a.FastMap.Count()))
			}
			a.RWMutex.Unlock()
		}
	} else {
		a.RWMutex.RLock()
		data, ok = a.Map[key]
		a.RWMutex.RUnlock()
		if !ok {
			a.RWMutex.Lock()
			data, ok = a.Map[key]
			if !ok {
				data = get()
				a.Map[key] = data
				ok = true
				atomic.StoreUint32(&a.size, uint32(len(a.Map)))
			}
			a.RWMutex.Unlock()
		}
	}
	return data, ok
}

func (a *Accumulator) tryGet(key interface{}) (data interface{}, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
			data = nil
		}
	}()
	scn := fmap.Residents(a.FastMap)
	data, ok = a.FastMap.Get(key)
	next := fmap.Residents(a.FastMap)
	hasChanged := scn != next
	if hasChanged {
		ok = false
	}
	return
}

func (a *Accumulator) Put(key, value interface{}) interface{} {

	if a.UseShardedAcc {
		a.ShardedAccumulator.Put(key, value)
		return value
	}

	a.RWMutex.Lock()
	defer a.RWMutex.Unlock()
	if a.UseFastMap {
		k := int64(key.(int))
		if prev, _ := a.FastMap.Get(k); prev != nil {
			return prev
		}
		a.FastMap.Put(k, value)
		atomic.StoreUint32(&a.size, uint32(a.FastMap.Count()))
	} else {
		a.Map[key] = value
		atomic.StoreUint32(&a.size, uint32(len(a.Map)))
	}
	return value
}

func NewAccumulator(fastMapPool *FMapPool, mapPool *MapPool, sharedAccPool *ShardAccPool) *Accumulator {
	if sharedAccPool != nil {
		return &Accumulator{
			UseShardedAcc:      true,
			ShardedAccumulator: sharedAccPool.Get(),
		}
	}

	useFastMap := fastMapPool != nil
	if useFastMap {
		return &Accumulator{UseFastMap: useFastMap, FastMap: fastMapPool.Get()}
	}

	useMapPool := mapPool != nil
	if useMapPool {
		return &Accumulator{Map: mapPool.Get()}
	}

	return &Accumulator{Map: make(map[interface{}]interface{}, 100)}
}

func (a *Accumulator) Len() int {

	if a.UseShardedAcc {
		return a.ShardedAccumulator.Len()
	}
	return int(atomic.LoadUint32(&a.size))
}
